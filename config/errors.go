package config

import "errors"

var (
	ErrReadConfigFile         error = errors.New("catfish/config : read config file")
	ErrParseConfigFile        error = errors.New("catfish/config : parse config file")
	ErrUnknownTier            error = errors.New("config: user references unknown tier")
	ErrPasswordEnvMissing     error = errors.New("config: password env var is not set or empty")
	ErrPostgresHostRequired   error = errors.New("config: postgres_host is required")
	ErrAtLeastOneTierRequired error = errors.New("config: at least one tier is required")
	ErrAtLeastOneUserRequired error = errors.New("config: at least one user is required")
	ErrUserMissingPasswordEnv error = errors.New("config: user is missing password_env")
)
