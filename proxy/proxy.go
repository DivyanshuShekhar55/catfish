package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	ErrPoolCreation                      error = errors.New("catfish/proxy : error creating pool, closing all pools ")
	ErrProxyTcpLlistener                 error = errors.New("catfish/proxy : error starting tcp listener ")
	ErrReadStartupMsg                    error = errors.New("catfish/proxy : error reading auth stratup msg ")
	ErrReadStartupMsgAfterSSLDecline     error = errors.New("catfish/proxy : error reading auth stratup msg after SSL declined ")
	ErrStartupMsgUnexpectedFormat        error = errors.New("catfish/proxy : unexpected startup message ")
	ErrAuthFailed                        error = errors.New("catfish/proxy : authentication failed ")
	ErrUnknownUser                       error = errors.New("catfish/proxy : unknown user ")
	ErrDatabaseConnectionNotConfigured   error = errors.New("catfish/proxy : user not configured to connect to this database ")
	ErrClearTextAuthChallengeSend        error = errors.New("catfish/proxy : error during sending clear text challenge to user ")
	ErrClearTextAuthRead                 error = errors.New("catfish/proxy : error reading response ")
	ErrClearTextAuthUnexpectedFormat     error = errors.New("catfish/proxy : cleartext unexpected PasswordMessage format ")
	ErrClearTextAuthInvalidPassword      error = errors.New("catfish.proxy : wrong password ")
	ErrMD5AuthSaltGen                    error = errors.New("catfish/proxy : md5 salt generation error ")
	ErrMD5AuthChallengeSend              error = errors.New("catfish/proxy : md5 send challenge ")
	ErrMD5AuthRead                       error = errors.New("catfish/proxy : md5 read response error ")
	ErrMD5AuthUnexpectedFormat           error = errors.New("catfish/proxy : md5 unexpected PasswordMessage format ")
	ErrMD5AuthInvalidCredentials         error = errors.New("catfish/proxy : md5 wrong credentials for user ")
	ErrSCRAMAuthChallengeSend            error = errors.New("catfish/proxy : scram send mechanism list ")
	ErrSCRAMAuthRead                     error = errors.New("catfish/proxy : scram auth read error ")
	ErrSCRAMAuthUnexpectedInitFormat     error = errors.New("catfish/proxy : unexpected SASLInitResponse ")
	ErrSCRAMAuthUnexpectedMethod         error = errors.New("catfish/proxy : client chose unexpected mechanism scram ")
	ErrSCRAMAuthNonceGen                 error = errors.New("catfish/proxy : scram nonce generation failed ")
	ErrSCRAMAuthSaltGen                  error = errors.New("catfish/proxy : scram salt generation error ")
	ErrSCRAMAuthServerFirstSend          error = errors.New("catfish/proxy : scram send server-first ")
	ErrSCRAMAuthClientReadFinal          error = errors.New("catfish/proxy : scram read client-final ")
	ErrSCRAMAuthUnexpectedResponseFormat error = errors.New("catfish/proxy : scram unexpected SASLResponse ")
	ErrSCRAMAuthParseClientFirst         error = errors.New("catfish/proxy : scram parse client-first ")
	ErrSCRAMAuthParseClientFinal         error = errors.New("catfish/proxy : scram parse client-final ")
	ErrSCRAMAuthDecodeClientProof        error = errors.New("catfish/proxy : scram decode client proof ")
	ErrSCRAMAuthWrongPassword            error = errors.New("catfish/proxy : scram wrong password for user ")
	ErrSCRAMAuthServerFinalSend          error = errors.New("catfish/proxy : scram send server-final ")
	ErrUnknownAuthMethod                 error = errors.New("catfish/proxy : unknown auth method ")
	ErrAuthOKSend                        error = errors.New("catfish/proxy : error sending AuthenticationOk msg ")
	ErrParameterStatusSend               error = errors.New("catfish/proxy : error sending parameter statuses by server ")
	ErrReadyForQuerySend                 error = errors.New("catfish/proxy : error sending ReadyForQuery msg ")
	ErrPoolNotFound                      error = errors.New("catfish/proxy : no pool found for user/database pair")
	ErrPoolAcquire                       error = errors.New("catfish/proxy : failed to acquire connection from pool")
	ErrQueryForward                      error = errors.New("catfish/proxy : failed to forward query to postgres")
	ErrPostgresRead                      error = errors.New("catfish/proxy : lost connection to postgres mid-response")

	ErrCodeAuthFailed string = "28P01"
	ErrCodeTooBusy           = "53300"
	ErrCodeNoPool            = "3D000"
	ErrCodeConnFailed        = "08006"
)

// use the close once function, so multiple goroutines do not call close at the same time.
// Calling close() from multiple goroutines at the same time causes panic
// which is against the graceful shutdown process
type CatfishServer struct {
	config            *config.Config
	pools             map[string]*pool.Pool // we will have one pool per user-db pair
	parameterStatuses map[string]string     // used after user authenticates succesfully, sends these statuses
	semaphore         *backpressure.Semaphore
	userIndex         map[string]config.User // fast lookup by username
	tierIndex         map[string]int         // tier name to semaphore index
	clientListener    net.Listener
	wg                sync.WaitGroup
	closeOnce         sync.Once
	done              chan struct{}
}

func New(ctx context.Context, cfg *config.Config, semaphore *backpressure.Semaphore) (*CatfishServer, error) {
	pools := make(map[string]*pool.Pool, len(cfg.Users))
	userIndex := make(map[string]config.User, len(cfg.Users))
	var parameterStatuses map[string]string

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

			return nil, fmt.Errorf("%w: user=%s db=%s: %w", ErrPoolCreation, user.Username, user.Database, err)
		}

		// all good, add to pools
		pools[key] = p
		userIndex[user.Username] = user

		parameterStatuses, err = p.ParameterStatuses(ctx)
		if err != nil {
			continue
			// i.e., leave this pool
			// use the next pool in the loop to look for statuses
		}
	}

	// store the server's parameter status
	// this shall be used when we send response back to the client after auth finishes successfully
	if parameterStatuses == nil {
		// fallback — no pool responded, use safe defaults
		// not a big enough error to panic i guess
		parameterStatuses = map[string]string{
			"server_version":    "15.0",
			"client_encoding":   "UTF8",
			"DateStyle":         "ISO, MDY",
			"TimeZone":          "UTC",
			"integer_datetimes": "on",
		}
	}

	// tiers in config are ordered highest → lowest priority, matching semaphore index 0, 1, 2...
	tierIndex := make(map[string]int, len(cfg.Tiers))
	for i, tier := range cfg.Tiers {
		tierIndex[tier.Name] = i
	}

	return &CatfishServer{
		config:            cfg,
		pools:             pools,
		userIndex:         userIndex,
		semaphore:         semaphore,
		done:              make(chan struct{}),
		parameterStatuses: parameterStatuses,
	}, nil
}

func (s *CatfishServer) Listen() error {
	ln, err := net.Listen("tcp", s.config.ListenerAddr)
	if err != nil {
		return fmt.Errorf("%w: addr=%s: %w", ErrProxyTcpLlistener, s.config.ListenerAddr, err)
	}

	s.clientListener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				// no error, Close() was called, clean shutdown
				return nil
			default:
				return fmt.Errorf("%w: %w", ErrProxyTcpLlistener, err)

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
// the fields will be populated only after being authenticated and checked
type clientState struct {
	username        string
	database        string
	tier            string
	queryInProgress bool   // is there any running query, not finished yet (used during flush logic in pooler)
	txStatus        byte   // 'I'(idle), 'E'(error), 'T'(in tx), used in pooler afterRelease logic
	backendPID      uint32 // to track and cancel pg process
	backendSecret   uint32 // cancel secret, paired with backendPID
}

// after it is accepted by our server
// the tcp conn now should be able to send queries and receive results
func (s *CatfishServer) handleClient(appConn net.Conn) {
	defer appConn.Close()

	// backend reads messages FROM the app (app is the client/frontend).
	// will also write responses to the app
	backend := pgproto3.NewBackend(appConn, appConn)

	// create a fresh state for this client — populated during doAuth
	// every client accepted through Tcp gets its own new clientState
	clientState := &clientState{}

	// one context per client connection lifetime
	// cancels automatically when this function returns (i.e., when client disconnects)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//	step 1 : auth —
	err := s.doAuth(backend, appConn, clientState)
	if err != nil {
		sendError(backend, ErrCodeAuthFailed, ErrAuthFailed.Error()+err.Error())
		// since this client failed to authenticate, we can run other queries here
		sendReadyForQuery(backend, 'I')
		return
	}

	// step 2 : find pool for this user
	// from the auth function the client state fields have been populated
	pool, ok := s.pools[poolKey(clientState.username, clientState.database)]
	if !ok {
		sendError(backend, ErrCodeNoPool, fmt.Sprintf("%s: user%s, db:%s", ErrPoolNotFound.Error(), clientState.username, clientState.database))
		sendReadyForQuery(backend, 'I')
		return
	}

	// step 3: hold one raw postgres connection for this client's entire session
	// WithConn blocks here until clientLoop returns (i.e. client disconnects)
	// when clientLoop returns → fn returns → WithConn releases conn back to pool
	// note that backend talks via the appConn (received from ttcp.listen)
	// frontend talks via rawConn (one created from a acquired pool, the WithConn creates that)
	if err := pool.WithConn(ctx, func(rawConn net.Conn) error {
		frontend := pgproto3.NewFrontend(rawConn, rawConn)
		// step 4 : handle queries
		s.clientLoop(ctx, backend, frontend, clientState)
		return nil
	}); err != nil {
		// only reaches here if Acquire itself failed, not if client disconnected
		sendError(backend, ErrCodeConnFailed, ErrPoolAcquire.Error()+": "+err.Error())
		sendReadyForQuery(backend, 'I')
	}

}

// client loop
func (s *CatfishServer) clientLoop(
	ctx context.Context,
	backend *pgproto3.Backend,
	frontend *pgproto3.Frontend,
	clientState *clientState,
) {
	for {
		// wait till any app sends a message
		msg, err := backend.Receive()
		if err != nil {
			if isClosedErr(err) && clientState.queryInProgress {
				// app disconnected mid-query, cleanup
				// TODO : pg_cancel_backend via cancel pool.
				// for now: closing pgxConn (via defer Release in handleClient)
				// causes postgres to notice the disconnect and clean up eventually
				
			}
			
			return // any error = connection dead, exit goroutine
		}

		switch m := msg.(type) {
		case *pgproto3.Query:
			s.handleQuery(backend, frontend, m, clientState) // actual work 
			// above one blocks until response is fully streamed back
		case *pgproto3.Terminate:
			frontend.Send(m) // tell postgres server goodbye too
			frontend.Flush()
			return           // clean exit

		default:
			// extended query protocol (Parse/Bind/Execute) or anything else
			// we don't inspect — forward blindly and relay whatever postgres says back
			if frontendMsg, ok := msg.(pgproto3.FrontendMessage); ok {
				// just forward to backend
				frontend.Send(frontendMsg)
				if err := frontend.Flush(); err != nil {
					return
				}
			}
			// Postgres sends back multiple response messages for each of those frontend.Send() — and you need to forward ALL of them back to the app

			// relay response back to frontend until ReadyForQuery msg pops up
			if err := s.relayUntilReady(backend, frontend, clientState); err != nil {
				return
			}

		}
	}

}

func (s *CatfishServer) handleQuery(
	backend *pgproto3.Backend,
	frontend *pgproto3.Frontend,
	msg *pgproto3.Query,
	clientState *clientState,
) {
	ctx := context.Background()

	// acquire a semaphore slot for this user's tier
	if err := s.semaphore.Acquire(ctx, 1, s.tierIndex[clientState.tier]); err != nil {

	}
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
	backend.Flush() // ignore error, best effort
}

// sends a signal that next query can be run now
// kinda like backend yelling "I am free now"
func sendReadyForQuery(backend *pgproto3.Backend, txStatus byte) {
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus})
	backend.Flush() // ignore error, best effort
}

// isClosedErr returns true if the error represents a normal connection close
// rather than an unexpected failure. Used to distinguish clean disconnects
// from real errors in clientLoop.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	// normal TCP close — client called conn.Close() or process exited
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// network-level error (connection reset, broken pipe, etc.)
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return !netErr.Temporary()
	}
	return false
}

// if its not a simple uqery we keep forwarding messages until its over
// make a loop that just checks what type of data it is
// and forwards it properly
func (s *CatfishServer) relayUntilReady (
	backend *pgproto3.Backend,
	frontend *pgproto3.Frontend,
	clientState *clientState,
) error {
	for {
		msg, err := frontend.Receive() // read from postgres
		if err != nil {
			return err
		}
		if backendMsg, ok := msg.(pgproto3.BackendMessage);ok {
			backend.Send(backendMsg) // forward to app/frontend 'msg'
		}
		// ReadyForQuery signals end of this command — update txStatus and return
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			clientState.txStatus = rfq.TxStatus
			backend.Flush()
			return nil 
		}
	}
}