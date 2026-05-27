package config

import "errors"

var (
	ErrReadConfigFile         error = errors.New("catfish/config : read config file")
	ErrParseConfigFile        error = errors.New("catfish/config : parse config file")
	ErrUnknownTier            error = errors.New("catfish/config: user references unknown tier")
	ErrPasswordEnvMissing     error = errors.New("catfish/config: password env var is not set or empty")
	ErrPostgresHostRequired   error = errors.New("catfish/config: postgres_host is required")
	ErrAtLeastOneTierRequired error = errors.New("catfish/config: at least one tier is required")
	ErrAtLeastOneUserRequired error = errors.New("catfish/config: at least one user is required")
	ErrUserMissingPasswordEnv error = errors.New("catfish/config: user is missing password_env")
	ErrTiersNotOrdered        error = errors.New("catfish/config : errors must be in higher to lower order")
)
