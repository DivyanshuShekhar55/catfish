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
	ErrPoolCreation                    error = errors.New("catfish/proxy : error creating pool, closing all pools ")
	ErrProxyTcpLlistener               error = errors.New("catfish/proxy : error starting tcp listener ")
	ErrReadStartupMsg                  error = errors.New("catfish/proxy : error reading auth stratup msg ")
	ErrReadStartupMsgAfterSSLDecline   error = errors.New("catfish/proxy : error reading auth stratup msg after SSL declined ")
	ErrStartupMsgUnexpectedFormat      error = errors.New("catfish/proxy : unexpected startup message ")
	ErrAuthFailed                      error = errors.New("catfish/proxy : authentication failed ")
	ErrUnknownUser                     error = errors.New("catfish/proxy : Unknown User ")
	ErrDatabaseConnectionNotConfigured error = errors.New("catfish/proxy : user not configured to connect to this database ")
	ErrClearTextAuthChallengeSend      error = errors.New("catfish/proxy : error during sending clear text challenge to user ")
	ErrClearTextAuthRead               error = errors.New("catfish/proxy : error reading response ")
	ErrClearTextAuthUnexpectedFormat   error = errors.New("catfish/proxy : cleartext unexpected PasswordMessage format ")
	ErrClearTextAuthInvalidPassword    error = errors.New("catfish.proxy : wrong password ")
	ErrMD5AuthSaltGen                  error = errors.New("catfish/proxy : md5 salt generation error ")
	ErrMD5AuthChallengeSend            error = errors.New("catfish/proxy : md5 send challenge ")
	ErrMD5AuthRead                     error = errors.New("catfish/proxy : md5 read response error ")
	ErrMD5AuthUnexpectedFormat         error = errors.New("catfish/proxy : md5 unexpected PasswordMessage format ")
	ErrMD5AuthInvalidCredentials       error = errors.New("catfish/proxy : md5 wrong credentials for user ")
	ErrSCRAMAuthChallengeSend          error = errors.New("catfish/proxy : scram send mechanism list ")
	ErrSCRAMAuthRead                   error = errors.New("catfish/proxy : scram auth read error ")
	ErrSCRAMAuthUnexpectedFormat       error = errors.New("catfish/proxy : unexpected SASLInitResponse ")
	ErrSCRAMAuthUnexpectedMethod       error = errors.New("catfish/proxy : client chose unexpected mechanism scram ")

	ErrCodeAuthFailed string = "28P01"
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
		key := poolKey(user.Username, user.Database)

		// dsn goes like some particular user wants to connect to some database
		// the machine address (PostgresHost) will be fixed and fetched from config, same with PostgresPort
		dsn := fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s",
			user.Username,
			user.Password,
			cfg.PostgresHost,
			cfg.PostgresPort,
			user.Database,
		)

		// create a new pool for this (user, db) pair
		// TODO : set other parts of pool.Config too here
		p, err := pool.New(ctx, pool.Config{DSN: dsn})
		if err != nil {
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
		pools:     pools,
		userIndex: userIndex,
		semaphore: semaphore,
		done:      make(chan struct{}),
	}, nil
}

func (s *CatfishServer) Listen() error {
	ln, err := net.Listen("tcp", s.config.ListenerAddr)
	if err != nil {
		return fmt.Errorf(ErrProxyTcpLlistener.Error(), s.config.ListenerAddr, err)
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

		// close all the pools too
		for _, p := range s.pools {
			p.Close()
		}
	})
}

// CLIENT STATE
// keep one instance per connected app, track the user/app

type clientState struct {
	username        string
	database        string
	tier            string
	queryInProgress bool   // is there any running query, not finished yet (used during flush logic in pooler)
	txStatus        byte   // 'I'(idle), 'E'(error), 'T'(in tx), used in pooler afterRelease logic
	backendPID      uint32 // to track and cancel
	backendSecret   uint32
}

// after it is accepted by our server
// the tcp conn now should be able to send queries and receive results
func (s *CatfishServer) handleClient(appConn net.Conn) {
	defer appConn.Close()

	// backend reads messages FROM the app (app is the client/frontend).
	backend := pgproto3.NewBackend(appConn, appConn)
	//clientState := &clientState{txStatus: 'I'}

	// Step 1: auth — forward the full handshake to real Postgres.
	err := s.doAuth(backend, appConn)
	if err != nil {
		sendError(backend, ErrCodeAuthFailed, ErrAuthFailed.Error()+err.Error())
		// since this client failed to
		sendReadyForQuery(backend, 'I')
		return
	}

	// TODO : COME BACK AFTER FINISHING AUTH

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

func sendError(backend *pgproto3.Backend, code, message string) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  message,
	})
	backend.Flush(); // ignore error, best effort
}

// sends a signal that next query can be run now
// kinda like backend yelling "I am free now"
func sendReadyForQuery(backend *pgproto3.Backend, txStatus byte) {
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
	backend.Flush(); // ignore error, best effort
}
