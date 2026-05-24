package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/DivyanshuShekhar55/catfish/backpressure"
	"github.com/DivyanshuShekhar55/catfish/config"
	"github.com/DivyanshuShekhar55/catfish/pool"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ErrPoolCreation error = errors.New("catfish/proxy : error creating pool, closing all pools ")
	ErrProxyTcpLlistener             error = errors.New("catfish/proxy : error starting tcp listener ")
	ErrReadStartupMsg                error = errors.New("catfish/proxy : error reading auth stratup msg ")
	ErrReadStartupMsgAfterSSLDecline error = errors.New("catfish/proxy : error reading auth stratup msg after SSL declined ")
	ErrUnexpectedStartupMsg          error = errors.New("catfish/proxy : unexpected startup message ")
)

// use the close once function, so multiple goroutines do not call close at the same time.
// Calling close() from multiple goroutines at the same time causes panic
// which is against the graceful shutdown process
type CatfishServer struct {
	config    *config.Config
	pools     map[string]*pool.Pool // we will have one pool per user-db pair
	semaphore *backpressure.Semaphore

	userIndex      map[string]config.User
	clientListener net.Listener
	wg             sync.WaitGroup
	closeOnce      sync.Once
	done           chan struct{}
}

func New(ctx context.Context, cfg *config.Config, semaphore *backpressure.Semaphore) (*CatfishServer, error) {
	pools := make(map[string]*pool.Pool, len(cfg.Users))
	userIndex := make(map[string]config.User, len(cfg.Users))

	for _, user := range cfg.Users {
		// we will create a key to identify an unique pair of user-db
		key:= poolKey(user.Username, user.Database)

		// dsn goes like some particular user wants to connect to some database
		// the machine address (PostgresHost) will be fixed and fetched from config, same with PostgresPort
		dsn:= fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			user.Username,
			user.Password,
			cfg.PostgresHost,
			cfg.PostgresPort,
			user.Database,
		)
		
		// create a new pool for this (user, db) pair
		// TODO : set other parts of pool.Config too here
		p, err:= pool.New(ctx, pool.Config{DSN: dsn})
		if err!=nil {
			// close all other pools too
			// TODO: IS IT A GOOD DECISION ?
			for _, existing := range pools {
				existing.Close()
			}

			return nil, fmt.Errorf(ErrPoolCreation.Error(), user.Username, user.Database, err)
		}

		// all good, add to pools
		pools[key] = p 
		userIndex[user.Username] = user
	}

	return &CatfishServer{
		config:    cfg,
		pools:      pools,
		userIndex: userIndex,
		semaphore: semaphore,
		done:      make(chan struct{}),
	}, nil
}

func (s *CatfishServer) Listen() error {
	ln, err := net.Listen("tcp", s.config.ListenerAddr)
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

func poolKey(username, database string) string {
	return username + "/" + database
}