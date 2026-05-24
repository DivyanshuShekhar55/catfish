package proxy

import (
	"fmt"
	"net"
	"sync"
	"time"
	"errors"

	"github.com/DivyanshuShekhar55/catfish/backpressure"
	"github.com/DivyanshuShekhar55/catfish/pool"
)

var (
	ErrProxyTcpLlistener error = errors.New("catfish/proxy : error starting tcp listener")
)


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
		config: cfg,
		pool: pool,
		semaphore: semaphore,
		done: make(chan struct{}),
	}
}

func (s* CatfishServer) Listen() error {
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

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			fmt.Printf(conn.LocalAddr().Network())
			//s.handleClient(conn)
		}()
	}
}
