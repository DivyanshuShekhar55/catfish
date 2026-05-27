package pool

import "errors"

var (
	ErrConnAcquire           error = errors.New("catfish/pool : error acquiring a connection from the pool")
	ErrParseConnectionString error = errors.New("catfish/pool: parse connection string")
	ErrCreatePool            error = errors.New("catfish/pool: create pool")
	ErrWarmPool              error = errors.New("catfish/pool: warming")
	ErrAcquireConn           error = errors.New("catfish/pool: acquire")
	ErrExecConn              error = errors.New("catfish/pool: exec")
	ErrQueryConn             error = errors.New("catfish/pool: query")
	ErrBeginTxConn           error = errors.New("catfish/pool: begin tx")
	ErrWithConnAcquire       error = errors.New("catfish/pool : acquire error")
	ErrWarmCancelled         error = errors.New("catfish/pool: context cancelled while warming pool")
	ErrRollbackAfterRelease  error = errors.New("catfish/pool: error rolling back conn after release")
	ErrDrainRowsProbe        error = errors.New("catfish/pool: error during row drain probe")
	ErrTxAlreadyClosed       error = errors.New("catfish/pool: tx already closed")
	ErrAcquireRow            error = errors.New("catfish/pool: acquire row")
)
