package proxy

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/DivyanshuShekhar55/catfish/backpressure"
	"github.com/DivyanshuShekhar55/catfish/pool"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrProxyTcpLlistener error = errors.New("catfish/proxy : error starting tcp listener ")
	ErrReadStartupMsg    error = errors.New("catfish/proxy : error reading auth stratup msg ")
	ErrReadStartupMsgAfterSSLDecline error = errors.New("catfish/proxy : error reading auth stratup msg after SSL declined ")
	ErrUnexpectedStartupMsg error = errors.New("catfish/proxy : unexpected startup message ")

)

// CATFISH SERVER STATE
// one global state

type CatfishConfig struct {
	ClientListenerAddr string // like ":6432"
	PostgresDSN        string // the real conn string like localhost:5432/products/...
	ShutdownTimeout    time.Duration

	// usernames and their priority slots.
	// Configured in pgkeeper.yml as a list of Postgres usernames.
	UserPirorityList [][]string
}

func (c *CatfishConfig) setDefaults() {
	if c.ClientListenerAddr == "" {
		c.ClientListenerAddr = ":6432"
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 10 * time.Second
	}
}

// use the close once function, so multiple goroutines do not call close at the same time.
// Calling close() from multiple goroutines at the same time causes panic
// which is against the graceful shutdown process
type CatfishServer struct {
	config    CatfishConfig
	pool      *pool.Pool
	semaphore *backpressure.Semaphore

	clientListener net.Listener
	wg             sync.WaitGroup
	closeOnce      sync.Once
	done           chan struct{}
}

func New(cfg CatfishConfig, pool *pool.Pool, semaphore *backpressure.Semaphore) *CatfishServer {
	cfg.setDefaults()

	return &CatfishServer{
		config:    cfg,
		pool:      pool,
		semaphore: semaphore,
		done:      make(chan struct{}),
	}
}

func (s *CatfishServer) Listen() error {
	ln, err := net.Listen("tcp", s.config.ClientListenerAddr)
	if err != nil {
		return fmt.Errorf(ErrProxyTcpLlistener.Error(), err)
	}

	s.clientListener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				return fmt.Errorf(ErrProxyTcpLlistener.Error(), err)

			}
		}

		// add a new goroutine to the wait-group
		s.wg.Add(1)
		go func() {
			defer s.wg.Done() // closes the goroutine after its job is done
			s.handleClient(conn)
		}()
	}
}

// Close gracefully shuts the server down.
func (s *CatfishServer) Close() {
	// step 1: call wrap the shutdown logic inside close once
	s.closeOnce.Do(func() {

		// step 2: send a close signal to the done channel
		// this will signal all the goroutines and unlock all the select cases waiting for it
		close(s.done)

		// step 3: accept no more new connections
		s.clientListener.Close()

		// step 4: wait till all the goroutines finish executing
		waitDone := make(chan struct{})
		go func() {
			// this goroutine will block till all are done
			s.wg.Wait()
			close(waitDone)
		}()

		// step 5: but don't wait forever
		// maybe some conn is stuck, rather force the shutdown after the configured ShutdownTimeout
		select {
		case <-waitDone:
		case <-time.After(s.config.ShutdownTimeout):
		}
	})
}

// CLIENT STATE
// keep one instance per connected app, track the user/app

type clientState struct {
	username        string
	database        string
	priority        backpressure.Priority
	queryInProgress bool   // is there any running query, not finished yet (used during flush logic in pooler)
	txStatus        byte   // 'I'(idle), 'E'(error), 'T'(in tx), used in pooler afterRelease logic
	backendPID      uint32 // to track and cancel
	backendSecret   uint32
}

// after it is accepted by our server
// the tcp conn now should be able to send queries and receive results
func (s *CatfishServer) handleClient(conn net.Conn) {
	defer conn.Close()

	// backend reads messages FROM the app (app is the client/frontend).
	backend := pgproto3.NewBackend(conn, conn)
	state := &clientState{txStatus: 'I'}

	// Step 1: auth — forward the full handshake to real Postgres.
	pgConn, err := s.doAuth(backend, conn, state)
	fmt.Printf(pgConn.LocalAddr().Network(), err.Error())

}

// doAuth relays the entire Postgres auth conversation between app and Postgres.
// We peek at StartupMessage for username/database and BackendKeyData for
// the cancel PID/secret. Everything else is forwarded blindly.
// NOTE: if you want a load balancer/ sharded postgres query forwarder, it would be done once you have the user-db
func (s *CatfishServer) doAuth(backend *pgproto3.Backend, appConn net.Conn, clientState *clientState) (net.Conn, error) {
	// read startup msg from app
	startupMsg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return nil, fmt.Errorf(ErrReadStartupMsg.Error(), err)
	}

	// Handle SSL req, decline TLS for now
	// TODO : TLS setup as well
	if _, ok := startupMsg.(*pgproto3.SSLRequest); ok {
		appConn.Write([]byte{'N'}) // 'N' = no SSL for now, sends back to client
		// client will send user+db now to backend
		// update startup msg
		startupMsg, err = backend.ReceiveStartupMessage()
		if err != nil {
			return nil, fmt.Errorf(ErrReadStartupMsgAfterSSLDecline.Error(), err)
		}

	}
	sm, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		return nil, fmt.Errorf(ErrUnexpectedStartupMsg.Error(), startupMsg)
	}

	// can extract these two fields too now
	clientState.username = sm.Parameters["user"]
	clientState.database = sm.Parameters["database"]

	// Open raw TCP connection to real postgres now
	// upto now we were handling the messages with the client
	// auth ahead will just be blindly forwarded now
	//pgConn, err := net.Dial("tcp", postgresAddr(s.config.PostgresDSN))
	return nil, nil
}




// parses the DSN string ( e.g., postgres://user:pass@myhost:5433/mydb) using pgx's built-in parser,
// then pulls out just the host and port and returns them as "myhost:5433" — the format net.Dial needs.
// if malformed fallback to localhost:5432
// or shall we panic?
func postgresAddr(dsn string) string {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "localhost:5432"
	}
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}

