# 🐟 catfish

> A priority-aware Postgres connection pooler. No, it's not a PgBouncer killer.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What is this?

PgBouncer is great. Seriously — use it for most things. But it has a blind spot: **it treats every query the same**. When your connection pool fills up, PgBouncer doesn't care if the waiting query is a critical payment flow or someone running a full analytics scan at 3am. Everything gets the same flat queue.

**Catfish fixes that.**

Catfish is a proxy server that sits between your app and Postgres. It assigns connections based on *priority tiers* with configurable weights — so your critical queries always get served before the low-priority ones. And when things are busy, queries that do wait are managed by a **CoDel (Controlled Delay)** algorithm-based queue that prevents buffer bloat and keeps latency predictable.

The design is heavily inspired by PgKeeper, an internal tool built at Figma. You can read about the implementation in [this blog post](#) and the original PgKeeper writeup [here](#).

```
Your App ──► catfish :6432 ──► Postgres :5432
               │
               ├── critical tier (weight 3) ──► pool
               ├── normal tier  (weight 2) ──► pool
               └── low tier     (weight 1) ──► pool
```

---

## How it works

### Priority tiers + weighted semaphore

You define tiers (e.g. `critical`, `normal`, `low`) and assign each a weight. Catfish uses a **prioritized semaphore** with a total concurrency cap (`max_concurrent`). When a slot opens up, it's awarded to the tier with the highest relative weight. A critical query at weight 3 gets picked 3x more often than a low-priority one at weight 1.

Each user in the config is assigned to a tier — so `admin_user` might be `critical` while `analytics_user` is `normal`.

### CoDel queue

When no slot is immediately available, queries don't just spin or get rejected — they wait in a **CoDel-based queue**. CoDel (Controlled Delay) is a queue management algorithm originally designed for network packets. It keeps queuing delay bounded by actively dropping requests that have been waiting *too long*, which prevents the queue from becoming a "bufferbloat" problem that just pushes latency forward indefinitely.

### Connection pools

Catfish creates one backend connection pool per `(username, database)` pair. Pools can be tuned globally or overridden per user. The pool handles connection lifetime, idle eviction, and minimum warm connections so you're not cold-starting on the first request.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     catfish                         │
│                                                     │
│  Client ──► Auth ──► Tier Router                   │
│                          │                         │
│              ┌───────────┼───────────┐             │
│              ▼           ▼           ▼             │
│         [critical]   [normal]     [low]            │
│          CoDel Q     CoDel Q     CoDel Q           │
│              │           │           │             │
│              └───────────┴───────────┘             │
│                          │                         │
│               Prioritized Semaphore                │
│               (max_concurrent slots)               │
│                          │                         │
│              ┌───────────┴───────────┐             │
│              ▼                       ▼             │
│        Pool (user A)           Pool (user B)       │
│              │                       │             │
└──────────────┼───────────────────────┼─────────────┘
               ▼                       ▼
                      Postgres
```

---

## Getting started

### Prerequisites

- Go 1.21+
- A running Postgres instance
- A YAML config file (see below)

### Run directly

```bash
go run ./cmd -config ./config.yml
```

### Build and run

```bash
go build -o catfish ./cmd
./catfish -config ./config.yml
```

### Windows

```powershell
go build -o catfish.exe ./cmd
.\catfish.exe -config .\config.yml
```

---

## Configuration

Catfish is configured entirely through a YAML file. Here's a full example:

```yaml
listen: ":6432"
postgres_host: "localhost"
postgres_port: 5432
shutdown_timeout: 10s
max_concurrent: 100

pool:
  min_conns: 5
  max_conns: 25
  max_conn_time: 1h
  max_idle_time: 20m

tiers:
  - name: critical
    weight: 3
    queue_size: 500
  - name: normal
    weight: 2
    queue_size: 200
  - name: low
    weight: 1
    queue_size: 100

users:
  - username: admin_user
    database: products
    tier: critical
    password_env: CATFISH_ADMIN_USER_PASSWORD
    auth_method: scram-sha-256

  - username: analytics_user
    database: products
    tier: normal
    password_env: CATFISH_ANALYTICS_USER_PASSWORD
    auth_method: md5
    pool:
      min_conns: 2
      max_conns: 10
```

Passwords **never** go in the config file — they come from environment variables:

```bash
export CATFISH_ADMIN_USER_PASSWORD='supersecret'
export CATFISH_ANALYTICS_USER_PASSWORD='alsosecret'
```

If an env var is missing at startup, catfish fails on purpose. No silent defaults on credentials.

### Top-level fields

| Field | Default | Description |
|---|---|---|
| `listen` | `:6432` | Address catfish listens on |
| `postgres_host` | *(required)* | Postgres backend host |
| `postgres_port` | `5432` | Postgres backend port |
| `shutdown_timeout` | `10s` | Grace period for in-flight queries on shutdown |
| `max_concurrent` | *(required)* | Total concurrent query slots across all tiers |

### Pool fields (global and per-user)

| Field | Default | Description |
|---|---|---|
| `min_conns` | `5` | Minimum idle connections to keep warm |
| `max_conns` | `25` | Maximum connections per pool |
| `max_conn_time` | `1h` | Max lifetime of a connection |
| `max_idle_time` | `20m` | Max idle time before a connection is evicted |

### Tier fields

| Field | Default | Description |
|---|---|---|
| `name` | *(required)* | Tier identifier — referenced by users |
| `weight` | `1` | Relative priority weight |
| `queue_size` | `200` | Max requests waiting in this tier's queue |

> **Order matters.** List tiers from highest to lowest priority in the YAML.

### User fields

| Field | Default | Description |
|---|---|---|
| `username` | *(required)* | Postgres username to accept |
| `database` | *(required)* | Database this user is allowed to connect to |
| `tier` | *(required)* | Which tier this user belongs to |
| `password_env` | *(required)* | Env var name holding the password |
| `auth_method` | `scram-sha-256` | One of `scram-sha-256`, `md5`, `cleartext` |
| `pool` | *(inherits global)* | Per-user pool overrides |

---

## Things to know

- **It's a service, not a library.** You run catfish as a sidecar/process. You don't import it.
- **One pool per (username, database) pair.** Two users on different databases get separate pools even if same user.
- **Tiers are not suggestions.** Weights directly affect how the semaphore hands out slots. Assign them thoughtfully.
- **Startup warms the pools.** Catfish connects to Postgres on boot. If Postgres is down, catfish won't start.
- **No SSL from clients (yet).** Catfish declines TLS during the client handshake. Plain Postgres protocol only.
- **Unknown users are rejected.** If a client connects with a username/database pair not in your config, it gets an auth error. No fallthrough.

---

## Project structure

```
catfish/
├── cmd/             # Binary entrypoint
├── backpressure/    # CoDel queue + prioritized semaphore
├── config/          # YAML parsing and validation
├── pool/            # Backend connection pool management
├── proxy/           # Client auth, protocol handling, query routing
└── utils/deque/     # Lock-free deque used by the queue
```

---

## Comparison with PgBouncer

| Feature | PgBouncer | catfish |
|---|---|---|
| Connection pooling | ✅ | ✅ |
| Priority tiers | ❌ | ✅ |
| Weighted scheduling | ❌ | ✅ |
| CoDel queue management | ❌ | ✅ |
| Per-user pool config | ✅ | ✅ |
| SSL client support | ✅ | ❌ (not yet) |
| Maturity / production use | Very high | Experimental |
| Written in | C | Go |

> Catfish is an experiment and a learning project — not a production PgBouncer replacement. PgBouncer has years of battle-testing. Use catfish if you need priority-aware pooling and are okay with the tradeoffs.

---

## Roadmap / ideas

- [ ] SSL/TLS support for client connections
- [ ] Prometheus metrics endpoint (`/metrics`)
- [ ] Hot config reload without restart
- [ ] Admin interface for live pool stats
- [ ] Per-tier latency and queue depth monitoring
- [ ] Support for `transaction` and `statement` pooling modes

---

## Contributing

PRs and issues are welcome. If something's broken or the docs are confusing, open an issue.

```bash
git clone https://github.com/DivyanshuShekhar55/catfish
cd catfish
go test ./...
```

---

## License

MIT. See [LICENSE](LICENSE).