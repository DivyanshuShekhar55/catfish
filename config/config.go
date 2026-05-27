package config

import (
	"fmt"
	"os"
	"time"

	"go.yaml.in/yaml/v4"
)

// catfish server's config struct
type Config struct {
	ListenerAddr    string // like ":6432"
	PostgresHost    string // like my-db.us-east-1.rds.amazonaws.com or localhost
	PostgresPort    int
	ShutdownTimeout time.Duration // duration after server shutdown, until which we wait for unfinished processes to sort out
	// if they can't resolve, kill them forcefully

	Tiers         []Tier // must be arranged in higher to lower order
	Users         []User
	Pool          PoolConfig
	MaxConcurrent int // max tokens available in the semaphore, so it is also num of concurrent ops
}

type Tier struct {
	Name      string // like "critical", "moderate", "low"
	Weight    int    // higher weight means higher priority and lower priority number
	QueueSize int    // the queue size
}

type User struct {
	Username string // user name like "analytics_user"
	Database string // db they want to connect to "products"
	Password string // loaded from env at load time
	// tier name like "moderate", all pools belonging to this user gets a moderate priority then
	// a pool is made per (user, db) pair
	Tier       string
	AuthMethod string // "cleartext", "md5" or "scram-sha-256"
}

type PoolConfig struct {
	MinConns        int32
	MaxConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// reads a yaml config file and parses into the following structs:
type File struct {
	Listen          string        `yaml:"listen"`
	PostgresHost    string        `yaml:"postgres_host"`
	PostgresPort    int           `yaml:"postgres_port"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	Tiers           []TierFile    `yaml:"tiers"`
	Users           []UserFile    `yaml:"users"`
	Pool            PoolFile      `yaml:"pool"`
	MaxConcurrent   int           `yaml:"max_concurrent"`
}

type PoolFile struct {
	MinConns        int32         `yaml:"min_conns"`
	MaxConns        int32         `yaml:"max_conns"`
	MaxConnLifetime time.Duration `yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `yaml:"max_conn_idle_time"`
}

type TierFile struct {
	Name      string `yaml:"name"`
	Weight    int    `yaml:"weight"`
	QueueSize int    `yaml:"queue_size"`
}

type UserFile struct {
	Username    string `yaml:"username"`
	Database    string `yaml:"database"`
	Tier        string `yaml:"tier"`
	PasswordEnv string `yaml:"password_env"` // name of env var holding password for thiss particular user
	AuthMethod  string `yaml:"auth_method"`  // cleartext | md5 | scram-sha-256 (default)
}

// Load reads the yaml file and resolves all passwords from env vars.
// Fails fast if any password env var is missing or empty.

func Load(path string) (*Config, error) {
	// as file won't be too big, read it fully
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: path=%s: %w", ErrReadConfigFile, path, err)
	}

	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("%w: path=%s: %w", ErrParseConfigFile, path, err)
	}

	if err := validate(&f); err != nil {
		return nil, err
	}

	cfg := &Config{
		ListenerAddr:    f.Listen,
		PostgresHost:    f.PostgresHost,
		PostgresPort:    f.PostgresPort,
		ShutdownTimeout: f.ShutdownTimeout,
	}

	cfg.Pool = PoolConfig{
		MinConns:        f.Pool.MinConns,
		MaxConns:        f.Pool.MaxConns,
		MaxConnLifetime: f.Pool.MaxConnLifetime,
		MaxConnIdleTime: f.Pool.MaxConnIdleTime,
	}

	cfg.MaxConcurrent = f.MaxConcurrent

	// Apply defaults.
	if cfg.ListenerAddr == "" {
		cfg.ListenerAddr = ":6432"
	}
	if cfg.PostgresPort == 0 {
		cfg.PostgresPort = 5432
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}

	if cfg.Pool.MinConns == 0 {
		cfg.Pool.MinConns = 5
	}
	if cfg.Pool.MaxConns == 0 {
		cfg.Pool.MaxConns = 20
	}
	if cfg.Pool.MaxConnLifetime == 0 {
		cfg.Pool.MaxConnLifetime = 1 * time.Hour
	}
	if cfg.Pool.MaxConnIdleTime == 0 {
		cfg.Pool.MaxConnIdleTime = 30 * time.Minute
	}

	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 20
	}

	// Copy tiers with defaults into config
	tierNames := make(map[string]bool, len(f.Tiers))
	for _, t := range f.Tiers {
		if t.Weight == 0 {
			// 0 is default go value for int
			// means user didn't specify
			t.Weight = 1
		}
		if t.QueueSize == 0 {
			// TODO : come up with a sound reason for a default value
			t.QueueSize = 200
		}
		cfg.Tiers = append(cfg.Tiers, Tier{
			Name:      t.Name,
			Weight:    t.Weight,
			QueueSize: t.QueueSize,
		})
		tierNames[t.Name] = true
	}

	// Resolve user passwords from env vars.
	// loop over users, check if their tier is valid, verify password exists, get it
	for _, u := range f.Users {
		if !tierNames[u.Tier] {
			return nil, fmt.Errorf("%w: user=%q tier=%q", ErrUnknownTier, u.Username, u.Tier)
		}

		// TODO : check if this is actually safe
		// like can this memory value be swapped into disk by the OS
		// or any other likely issue
		password := os.Getenv(u.PasswordEnv)
		if password == "" {
			return nil, fmt.Errorf("%w: env=%q user=%q", ErrPasswordEnvMissing, u.PasswordEnv, u.Username)
		}

		cfg.Users = append(cfg.Users, User{
			Username:   u.Username,
			Password:   password,
			Database:   u.Database,
			Tier:       u.Tier,
			AuthMethod: u.AuthMethod,
		})
	}

	return cfg, nil

}

func validate(f *File) error {
	if f.PostgresHost == "" {
		return ErrPostgresHostRequired
	}
	if len(f.Tiers) == 0 {
		return ErrAtLeastOneTierRequired
	}
	if len(f.Users) == 0 {
		return ErrAtLeastOneUserRequired
	}
	for _, u := range f.Users {
		if u.PasswordEnv == "" {
			return fmt.Errorf("%w: user=%q", ErrUserMissingPasswordEnv, u.Username)
		}
	}

	// tiers must be higher → lower order
	for i := 1; i < len(f.Tiers); i++ {
		if f.Tiers[i].Weight > f.Tiers[i-1].Weight {
			return fmt.Errorf("%w: tier %q has higher weight than %q but comes after it",
				ErrTiersNotOrdered, f.Tiers[i].Name, f.Tiers[i-1].Name)
		}
	}

	return nil
}
