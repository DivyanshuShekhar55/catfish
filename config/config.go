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

	Tiers []Tier // must be arranged in higher to lower order
	Users []User
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

// reads a yaml config file and parses into the following structs:
type File struct {
	Listen          string        `yaml:"listen"`
	PostgresHost    string        `yaml:"postgres_host"`
	PostgresPort    int           `yaml:"postgres_port"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	Tiers           []TierFile    `yaml:"tiers"`
	Users           []UserFile    `yaml:"users"`
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
		return nil, fmt.Errorf("catfish/config : read %s: %w", path, err)
	}

	var f File
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("catfish/config : parse error %s: %w", path, err)
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
			return nil, fmt.Errorf("config: user %q references unknown tier %q", u.Username, u.Tier)
		}

		// TODO : check if this is actually safe
		// like can this memory value be swapped into disk by the OS
		// or any other likely issue
		password := os.Getenv(u.PasswordEnv)
		if password == "" {
			return nil, fmt.Errorf(
				"config: password env var %q for user %q is not set or empty",
				u.PasswordEnv, u.Username,
			)
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
		return fmt.Errorf("config: postgres_host is required")
	}
	if len(f.Tiers) == 0 {
		return fmt.Errorf("config: at least one tier is required")
	}
	if len(f.Users) == 0 {
		return fmt.Errorf("config: at least one user is required")
	}
	for _, u := range f.Users {
		if u.PasswordEnv == "" {
			return fmt.Errorf("config: user %q is missing password_env", u.Username)
		}
	}
	return nil
}
