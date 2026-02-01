# RiftDB

Instant, copy-on-write database branches for Postgres. Self-hosted. Every PR gets its own database.

> ⚠️ **Status: Early Development** — Not ready for production use.

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

## Quick Start
```bash
# Install (coming soon...)
curl -fsSL https://riftdata.io/install.sh | sh

# Point at your existing Postgres
rift init --upstream postgres://localhost:5432/myapp

# Start the proxy
rift serve

# Create a branch
rift create my-feature

# Connect to it
psql postgres://localhost:6432/my-feature
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
| Self-hosted            | ✅    | ❌    | ❌           | ✅       |
| Instant branches       | ✅    | ✅    | ✅           | ❌       |
| Copy-on-write          | ✅    | ✅    | ✅           | ❌       |
| Works with existing DB | ✅    | ❌    | ❌           | ✅       |
| Postgres native        | ✅    | ✅    | ❌           | ✅       |
| Free                   | ✅    | ❌    | ❌           | ✅       |

## Roadmap

**Phase 1: Core** (in progress)
- [ ] Postgres wire protocol proxy
- [ ] Basic branch creation/deletion
- [ ] Read-through to upstream
- [ ] Copy-on-write for writes

**Phase 2: Usable**
- [ ] CLI with branch management
- [ ] Transaction support
- [ ] Connection pooling
- [ ] DDL tracking

**Phase 3: Production-ready**
- [ ] Web dashboard
- [ ] Metrics and observability
- [ ] CI/CD integrations
- [ ] Documentation site

## Project Structure
```
rift/
├── cmd/rift/         # CLI entry point
├── internal/
│   ├── proxy/            # Postgres wire protocol
│   ├── branch/           # Branch management
│   ├── cow/              # Copy-on-write engine
│   ├── storage/          # Delta storage layer
│   └── router/           # Query routing
├── pkg/
│   └── pgwire/           # Postgres protocol implementation
└── web/                  # Dashboard (later)
```

## Contributing

This project is in early development. Contributions welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing`)
5. Open a Pull Request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

Apache 2.0 — See [LICENSE](LICENSE) for details.

## Acknowledgments

Inspired by the branching capabilities of [Neon](https://neon.tech) and [PlanetScale](https://planetscale.com), with the goal of bringing this power to self-hosted environments.
