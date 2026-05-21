package pool

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	dsn                 string
	minConns            int32
	maxConns            int32
	maxIdleTime         time.Duration
	maxConnTime         time.Duration
	drainRowsBeforeKill int32
}

func (cfg *Config) SetDefault() {
	if cfg.minConns == 0 {
		cfg.minConns = 5
	}
	if cfg.maxConns == 0 {
		cfg.maxConns = 25
	}

	if cfg.maxConnTime == 0 {
		cfg.maxConnTime = 1 * time.Hour
	}
	if cfg.maxIdleTime == 0 {
		cfg.maxIdleTime = 20 * time.Minute
	}
	if cfg.drainRowsBeforeKill == 0 {
		cfg.drainRowsBeforeKill = 100
	}
}

type Pool struct {
	inner  *pgxpool.Pool
	config Config
}

func New(ctx context.Context, cfg Config) (*Pool, error) {
	cfg.SetDefault()
	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)

	if err != nil {
		return nil, fmt.Errorf("error with parsing connection string %w", err)
	}
	poolCfg.MaxConns = cfg.maxConns
	poolCfg.MinConns = cfg.minConns
	poolCfg.MaxConnLifetime = cfg.maxConnTime
	poolCfg.MaxConnIdleTime = cfg.maxIdleTime

	poolCfg.AfterRelease = func(conn *pgx.Conn) bool {
		// true returns connection back to pool, false destroys it
		// create a new context here as the original ctx was already over
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// try to rollback the changes if any (only those which didn't finish)
		if err := rollbackIfNeeded(ctx, conn); err != nil {
			// this one is unsafee kill the conn
			return false
		}

		// rollback was success try to drain any unread rows
		// if number of unread rows < drainRowsBeforeKill then read all (cheaper than kkilling the conn)
		// else killing the conn and creating a newer one is cheaper
		drained, err := drainRows(ctx, conn, cfg.drainRowsBeforeKill)
		if err != nil {
			return false
		}
		if !drained {
			return false
		}

		return true

	}

	inner, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pgkeeper/pool: create pool: %w", err)
	}

	p := &Pool{inner: inner, config: cfg}

	// Warm the pool: block until MinConns idle connections are established.
	if err := p.warm(ctx); err != nil {
		inner.Close()
		return nil, fmt.Errorf("pgkeeper/pool: warming: %w", err)
	}

	return p, nil
}

// returns pool stat using pgxpool functions
func (p *Pool) Stat(ctx context.Context) *pgxpool.Stat {
	return p.inner.Stat()
}

// Close shuts down the pool, closing all connections.
func (p *Pool) Close() {
	p.inner.Close()
}

// warm blocks until the pool has at least MinConns idle connections or ctx
// is cancelled. It pings the pool on a tight loop — pgxpool establishes
// connections in the background as soon as it is created.
func (p *Pool) warm(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while warming pool: %w", ctx.Err())
		case <-ticker.C:
			stat := p.inner.Stat()
			if stat.IdleConns() >= p.config.minConns {
				return nil
			}
		}
	}
}

func rollbackIfNeeded(ctx context.Context, conn *pgx.Conn) error {
	connStatus := conn.PgConn().TxStatus()

	if connStatus == 'I' {
		// conn was idle, it was doing nothing
		// return nil so conn can return to pool normally
		return nil
	}

	// rest conn was either 'E'(error in txn) or 'T'(still in txn)
	// both are wrong, as we are already at the step for releasing the conn
	// try to rollback
	_, err := conn.Exec(ctx, "ROLLBACK")
	if err != nil {
		return fmt.Errorf("error rolling back conn after release %w", err)
	}
	return nil
}

func drainRows(ctx context.Context, conn *pgx.Conn, maxDrainRows int32) (bool, error) {
	// if more than drainRowsBeforeKill unread rows, delete the conn
	// for majority however has less than drainRowsBeforeKill rows and killing the conn is resource-wise cheaper
	// send false to delete connection, send true to keep it alive
	rows, err := conn.Query(ctx, "SELECT 1 WHERE FALSE")
	if err != nil {
		fmt.Errorf("error during row drain probe %w", err)
		return false, err
	}
	defer rows.Close()

	var count int32 = 0
	for rows.Next() {
		count++
		if count > maxDrainRows {
			return false, nil
		}
	}

	if err := rows.Err(); err != nil {
		// Underlying connection error during drain
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false, err
		}
		// Other errors (e.g. broken connection) then discard
		return false, nil
	}

	return true, nil

}
