package config

import "time"

type Config struct {
	ListenerAddr    string // like ":5432"
	PostgresHost    string // like my-db.us-east-1.rds.amazonaws.com or localhost
	PostgresPort    int
	ShutdownTimeout time.Duration

	Tiers []Tier
	Users []User
}

type Tier struct {
	Name      string // like "critical" or "low"
	Weight    int // higher weight means higher priority and lower priority number
	QueueSize int // the queue size
}

type User struct {
	Username string // user name like "analytics_user"
	Database string // db they want to connect to "products"
	Password string // loaded from env at load time
	Tier string 
}
