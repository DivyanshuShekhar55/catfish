package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	DSN                 string
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
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)

	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParseConnectionString, err)
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
		return nil, fmt.Errorf("%w: %w", ErrCreatePool, err)
	}

	p := &Pool{inner: inner, config: cfg}

	// Warm the pool: block until MinConns idle connections are established.
	if err := p.warm(ctx); err != nil {
		inner.Close()
		return nil, fmt.Errorf("%w: %w", ErrWarmPool, err)
	}

	return p, nil
}

// returns pool stat using pgxpool functions
func (p *Pool) Stat() *pgxpool.Stat {
	return p.inner.Stat()
}

// Close shuts down the pool, closing all connections.
func (p *Pool) Close() {
	p.inner.Close()
}

// exec returns no rows, used for insert/delete/update ops
// returns a command tag (like "DELETE 8" which means 8 rows were deleted)
func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("%w: %w", ErrAcquireConn, err)
	}
	defer conn.Release()

	cmdTag, err := conn.Exec(ctx, sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("%w: %w", ErrExecConn, err)
	}
	return cmdTag, nil
}

// query is used for queries that return row(s)
// caller must call rows.Close() when done reading required row(s)
// but here is the problem with queries (not with exec):
// we get a handler after conn.Query() is run, NOT the row(s)'s data itself
// we return this rows handler to the user, it's upto the user to use the handler whenever he wishes (using rows.Next() loop)
// we can't say conn.Release() in our function, cause the user might not have read the values yet
// so we need a way to auto close the conenction once the user calls rows.Close()
// in short we need to create a func that closes both rows and conn with the calling of rows.Close()
// this is just because by design we don't give users the feature to get individual conns, so they can't close it either

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAcquireConn, err)
	}
	// we do NOT write defer conn.Release() due to reason mentioned before
	// rows is not the data, just a handler to get the data
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		// if error call conn.Release()
		conn.Release()
		return nil, fmt.Errorf("%w: %w", ErrQueryConn, err)
	}

	// clever part : (thanks to Claude) now we can hijack the real rows here
	// pgx.Rows are interface so by defining a method on a struct same as pgx.Rows
	// pgx thinks that struct is same as pgx.Rows, that struct is autoReleaseRows()
	return &autoReleaseRows{Rows: rows, conn: conn}, nil

}

// query row reads just a single row
// if no row found returns an error on calling .Scan() on the row handler
// just like we released the connection only after rows.Close() was called
// we release the connection after row.Scan() will be called on the returned row handler
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		// pgx.Row surfaces the error on .Scan(), so wrap it there
		return &errRow{err: fmt.Errorf("%w: %w", ErrAcquireRow, err)}
	}

	row := conn.QueryRow(ctx, sql, args...)
	return &autoReleaseRow{Row: row, conn: conn}
}

// BeginTx starts a transaction with the given options.
// The caller is responsible for calling tx.Commit() or tx.Rollback().
// If neither is called (e.g. panic), AfterRelease will issue a ROLLBACK
// automatically when the connection is returned to the pool.
//
// Example:
// tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
// use this tx to run multiple queries, also remember to call defer.Rollback() or tx.Commit()
func (p *Pool) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAcquireConn, err)
	}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("%w: %w", ErrBeginTxConn, err)
	}

	// Wrap so we release the connection after commit or rollback.
	return &autoReleaseTx{Tx: tx, conn: conn}, nil
}

// returns the parameter status values from the connected server
// this is used once when user passes auth in the proxy layer
// and has to be returned these status values
func (p *Pool) ParameterStatuses(ctx context.Context) (map[string]string, error) {

	statuses := make(map[string]string)

	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return nil, errors.Join(ErrConnAcquire, err)
	}
	defer conn.Release()

	// Access the underlying *pgx.Conn and get parameter statuses
	pgxConn := conn.Conn()

	// Get specific parameter statuses
	params := []string{
		"server_version",
		"server_encoding",
		"client_encoding",
		"default_transaction_read_only",
		"in_hot_standby",
		"integer_datetimes",
		"TimeZone",
		"DateStyle",
	}

	for _, param := range params {
		value := pgxConn.PgConn().ParameterStatus(param)
		if value != "" {
			statuses[param] = value
		}
	}

	return statuses, nil
}

// WithConn acquires a connection, calls fn with the underlying net.Conn,
// and releases it when fn returns.
// creating this was necessary so we can call frontend's queries
// without exposing any acquire method
func (p *Pool) WithConn(ctx context.Context, fn func(net.Conn) error) error {
	conn, err := p.inner.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrWithConnAcquire, err)
	}
	defer conn.Release()

	// get the raw connection from this conn
	pgxConn := conn.Conn().PgConn()
	// get the raw net conn
	netConn := pgxConn.Conn()
	return fn(netConn)
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
			return fmt.Errorf("%w: %w", ErrWarmCancelled, ctx.Err())
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
		return fmt.Errorf("%w: %w", ErrRollbackAfterRelease, err)
	}
	return nil
}

func drainRows(ctx context.Context, conn *pgx.Conn, maxDrainRows int32) (bool, error) {
	// if more than drainRowsBeforeKill unread rows, delete the conn
	// for majority however has less than drainRowsBeforeKill rows and killing the conn is resource-wise cheaper
	// send false to delete connection, send true to keep it alive
	rows, err := conn.Query(ctx, "SELECT 1 WHERE FALSE")
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrDrainRowsProbe, err)
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

// read why we need it in the explaination on .Query() method
type autoReleaseRows struct {
	pgx.Rows
	conn            *pgxpool.Conn
	isClosedAlready bool
}

func (r *autoReleaseRows) Close() {
	if r.isClosedAlready {
		return
	}

	// not closed already and user called pgx.Rows.Close()
	// instead of running the normal close, we overload the function with our own implementation
	r.Rows.Close() // pgx drains remaining rows internally here
	r.isClosedAlready = true
	r.conn.Release() // return connection to pool, also triggers AfterRelease()

}

// autoReleaseRow releases the pool connection after Scan() is called
// pgx.Row reads exactly one row internally — Scan() is the last step
type autoReleaseRow struct {
	pgx.Row
	conn *pgxpool.Conn
}

// dest is any dstination struct user will use
func (r *autoReleaseRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)
	r.conn.Release()
	return err
}

// errRow surfaces an acquire error through the pgx.Row interface
// This way QueryRow().Scan() always returns an error cleanly
// even if we never got a connection
type errRow struct {
	err error
}

func (r *errRow) Scan(...any) error { return r.err }

// autoReleaseTx releases the pool connection after Commit or Rollback.
// Calling Rollback after Commit is a safe no-op (matches pgx behaviour).
type autoReleaseTx struct {
	pgx.Tx
	conn *pgxpool.Conn
	done bool
}

// user called Commit after tx, release the connection
func (t *autoReleaseTx) Commit(ctx context.Context) error {
	if t.done {
		return ErrTxAlreadyClosed
	}
	t.done = true
	err := t.Tx.Commit(ctx)
	t.conn.Release()
	return err
}

func (t *autoReleaseTx) Rollback(ctx context.Context) error {
	if t.done {
		// Already committed or rolled back — safe no-op.
		return nil
	}
	t.done = true
	err := t.Tx.Rollback(ctx)
	t.conn.Release()
	return err
}
