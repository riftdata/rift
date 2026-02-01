# RiftDB

Instant, copy-on-write database branches for Postgres. Self-hosted. Every PR gets its own database.

> **Status: Early Development** — Not ready for production use.

## The Problem

You need isolated database environments for testing, but:

- `pg_dump` takes forever on large databases
- Spinning up copies eats storage and money
- Shared staging databases cause conflicts
- Neon/PlanetScale are SaaS with vendor lock-in

## The Solution

rift creates instant database branches using copy-on-write. A 500GB production database branches in milliseconds,
storing only the rows you change.
```bash
# Create a branch (instant, regardless of DB size)
rift create feature-auth

# Connect with standard Postgres tools
psql postgres://localhost:6432/feature-auth

# Break things freely — production is untouched
psql -c "DROP TABLE users CASCADE"

# Delete when done
rift delete feature-auth
```

## How It Works
```
Production "users" table (1M rows):
┌────┬─────────┬──────────────┐
│ id │ name    │ email        │
├────┼─────────┼──────────────┤
│ 1  │ Alice   │ a@test.com   │
│ 2  │ Bob     │ b@test.com   │
│... │ ...     │ ...          │
└────┴─────────┴──────────────┘

Branch "feature-auth" modifies row 2, adds row 1000001:
┌────┬─────────┬──────────────┐
│ id │ name    │ email        │  ← Only changed rows stored
├────┼─────────┼──────────────┤
│ 2  │ Robert  │ bob@new.com  │  ← UPDATE
│1M+1│ Charlie │ c@test.com   │  ← INSERT
└────┴─────────┴──────────────┘

Storage used: 2 rows, not 1,000,002
```

rift acts as a Postgres proxy. Reads fall through to the parent branch. Writes go to an overlay. Your application
connects normally—it just sees an isolated database.

## Features

- [ ] **Instant branching** — Create branches in milliseconds, any database size
- [ ] **Copy-on-write** — Store only changed rows, not full copies
- [ ] **Postgres-native** — Standard wire protocol, works with any Postgres client
- [ ] **Schema tracking** — DDL changes tracked per branch
- [ ] **Branch diff** — See what changed between branches
- [ ] **CI integration** — GitHub Actions, GitLab CI support
- [ ] **Web dashboard** — Visualize branches, storage, connections
- [ ] **Zero vendor lock-in** — Self-hosted, works with your existing database

## Installation

### Homebrew (macOS / Linux)

```bash
brew install riftdata/tap/rift
```

### npm / npx

```bash
npm install -g @rift-data/cli
# or run without installing
npx @rift-data/cli --help
```

### Shell installer
```bash
curl -fsSL https://riftdata.io/install.sh | sh
```

### Docker

```bash
docker pull riftdata/rift:latest
docker run -e rift_UPSTREAM_URL=postgres://host:5432/mydb riftdata/rift
```

### From source

```bash
git clone https://github.com/riftdata/rift.git
cd rift
make build
./bin/rift --help
```

## Quick Start

```bash
# Point at your existing Postgres
rift init --upstream postgres://localhost:5432/myapp

# Start the proxy
rift serve

# Create a branch
rift create my-feature

# Connect to it
psql postgres://localhost:6432/my-feature
```

## Docker Compose

A full local development environment is provided:

```bash
# Start Postgres + rift
docker compose -f docker/docker-compose.yml up -d

# Optionally include pgAdmin
docker compose -f docker/docker-compose.yml --profile tools up -d
```

Services:
| Service | Port | Description |
|---------|------|-------------|
| postgres | 5432 | Upstream PostgreSQL 17 |
| rift | 6432, 8080 | Proxy + HTTP API |
| pgadmin | 5050 | Database admin UI (optional) |

## Configuration

rift is configured via YAML file, environment variables, or CLI flags (highest priority wins).

### Environment Variables

All config keys use the `rift_` prefix with underscores replacing dots:

| Variable            | Default      | Description                          |
|---------------------|--------------|--------------------------------------|
| `rift_UPSTREAM_URL` | *(required)* | PostgreSQL connection string         |
| `rift_LISTEN_ADDR`  | `:6432`      | Proxy listen address                 |
| `rift_API_ADDR`     | `:8080`      | HTTP API listen address              |
| `rift_DATA_DIR`     | `~/.rift`    | Data storage directory               |
| `rift_LOG_LEVEL`    | `info`       | Log level (debug, info, warn, error) |
| `rift_LOG_FORMAT`   | `text`       | Log format (text, json)              |

### Config File

Config is searched in order: `./config.yaml` → `~/.rift/config.yaml` → `/etc/rift/config.yaml`.

```yaml
upstream:
  url: postgres://localhost:5432/myapp
  max_connections: 10
  ssl_mode: prefer

proxy:
  listen_addr: ":6432"
  max_connections: 100

api:
  enabled: true
  listen_addr: ":8080"

storage:
  data_dir: ~/.rift
  retention_days: 30

log:
  level: info
  format: text
```

### CLI Commands

```
rift init          Initialize rift with an upstream database
rift serve         Start the proxy server
rift create        Create a new branch
rift list          List all branches
rift delete        Delete a branch
rift status        Show branch/system status
rift diff          Compare branches
rift merge         Generate merge SQL
rift connect       Open psql session to a branch
rift config        Manage configuration (show, set, path)
rift version       Show version information
rift completion    Generate shell completions (bash, zsh, fish, powershell)
```

## CI Integration
```yaml
# .github/workflows/test.yml
name: Test
on: pull_request

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      rift:
        image: riftdata/rift:latest
        env:
          rift_UPSTREAM: ${{ secrets.DATABASE_URL }}

    steps:
      - uses: actions/checkout@v4

      - name: Create branch
        run: rift create pr-${{ github.event.number }}

      - name: Run migrations
        run: npm run db:migrate
        env:
          DATABASE_URL: postgres://localhost:6432/pr-${{ github.event.number }}

      - name: Run tests
        run: npm test
```

## Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Application                          │
│                   postgres://localhost:6432/branch-name          │
└────────────────────────────────┬────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────┐
│                         rift proxy                           │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐    │
│  │ Wire Protocol  │  │ Query Router   │  │ CoW Engine     │    │
│  │ (Postgres)     │  │ (per-branch)   │  │ (row-level)    │    │
│  └────────────────┘  └────────────────┘  └────────────────┘    │
└────────────────────────────────┬────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────┐
│                   PostgreSQL (upstream)                          │
│              Your existing database, unchanged                   │
└─────────────────────────────────────────────────────────────────┘
```

## Comparison

| Feature                | rift | Neon | PlanetScale | pg_dump |
|------------------------|------|------|-------------|---------|
| Self-hosted            | Y    | N    | N           | Y       |
| Instant branches       | Y    | Y    | Y           | N       |
| Copy-on-write          | Y    | Y    | Y           | N       |
| Works with existing DB | Y    | N    | N           | Y       |
| Postgres native        | Y    | Y    | N           | Y       |
| Free                   | Y    | N    | N           | Y       |

## Roadmap

**Phase 1: Core** (in progress)
- [ ] Postgres wire protocol proxy
- [ ] Basic branch creation/deletion
- [ ] Read-through to upstream
- [ ] Copy-on-write for writes

**Phase 2: Usable**
- [ ] Transaction support
- [ ] Connection pooling
- [ ] DDL tracking
- [ ] Branch TTL and auto-cleanup

**Phase 3: Production-ready**
- [ ] Web dashboard
- [ ] Metrics and observability
- [ ] CI/CD integrations
- [ ] Documentation site

## Project Structure
```
rift/
├── cmd/rift/              # CLI entry point (cobra commands)
├── internal/
│   ├── config/            # Configuration loading (viper)
│   └── ui/                # Terminal UI (bubbletea, lipgloss)
├── docker/                # Dockerfiles + docker-compose
├── scripts/               # Install and release scripts
├── npm/                   # Platform-specific npm packages
├── tests/integration/     # Integration tests
├── .github/workflows/     # CI, Release, Docker pipelines
├── .goreleaser.yml        # Cross-platform build config
└── Makefile               # Development automation
```

## Development

```bash
# Prerequisites: Go 1.25.6+, Docker, Make

# Clone and build
git clone https://github.com/riftdata/rift.git
cd rift && make build

# Start a dev database
make db-up

# Run tests
make test           # Unit tests
make test-race      # With race detector
make test-integration  # Integration tests (needs PostgreSQL)
make check          # Full pre-merge gate (fmt + lint + vet + test)

# Hot reload (requires air)
make dev
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed contribution guidelines.

## License

Apache 2.0 — See [LICENSE](LICENSE) for details.

## Acknowledgments

Inspired by the branching capabilities of [Neon](https://neon.tech) and [PlanetScale](https://planetscale.com), with the goal of bringing this power to self-hosted environments.