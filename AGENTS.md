# AGENTS.md

Guidelines for AI agents and automated tools working on the Rift codebase.

## Project Context

Rift is an early-stage Go project (v0.0.1) that provides instant, copy-on-write database branching for PostgreSQL. The
CLI scaffolding and infrastructure are in place; the core database proxy functionality is not yet implemented.

**Module path:** `github.com/riftdata/rift`
**Go version:** 1.25.6
**License:** Apache 2.0

## Critical Rules

1. **Never commit secrets.** The `.env` file, `credentials.json`, API keys, and database passwords must never be staged
   or committed. The `.gitignore` already excludes `.env`.

2. **Never push to main without CI passing.** All changes go through pull requests. CI runs lint, test, vet, security
   scans, and multi-platform builds.

3. **Do not modify generated files.** `go.sum` is managed by `go mod tidy`. GoReleaser dist output is ephemeral.

4. **Docker images are built by `docker.yml` only.** GoReleaser intentionally does not build Docker images because it
   cannot attach SBOM/provenance attestations. Do not re-add `dockers_v2` to `.goreleaser.yml`.

5. **Do not add `CGO_ENABLED=1` dependencies.** The project cross-compiles to 5 platform/arch combinations with
   `CGO_ENABLED=0`. Any dependency requiring cgo will break the build matrix.

## Code Conventions

### Go Style

- Follow standard Go idioms. Run `make fmt` (gofmt -s + go mod tidy) before committing.
- Maximum cyclomatic complexity: 15 (enforced by gocyclo in `.golangci.yml`).
- Linters: gocritic (diagnostic, style, performance), gosec, misspell, unconvert.
- Error wrapping: `fmt.Errorf("context: %w", err)`. Use sentinel errors for expected conditions.
- Early returns. No deeply nested if/else.
- Structured logging with key-value pairs, not printf-style.

### Commit Messages

Conventional Commits format:

```
type(scope): short description

Optional body explaining why.
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `ci`.
Scopes: `branch`, `proxy`, `cli`, `config`, `cow`, `pgwire`, `docker`, `ci`, `npm`.

### Branch Naming

`feature/description`, `fix/description`, `docs/description`, `refactor/description`, `test/description`.

## Build & Test

```bash
make build              # Build binary → bin/rift
make test               # Unit tests (short mode)
make test-race          # Unit tests + race detector
make test-integration   # Integration tests (requires PostgreSQL on localhost:5432)
make test-all           # test-race + test-integration
make lint               # golangci-lint
make check              # fmt + lint + vet + test (full pre-merge gate)
```

### Running Locally

```bash
make db-up                    # Start PostgreSQL 16 in Docker
cp .env.example .env          # Configure rift_UPSTREAM_URL
make build && ./bin/rift serve
```

### Integration Tests

Integration tests use the `//go:build integration` tag and require a running PostgreSQL instance. Set
`rift_UPSTREAM_URL` in your environment. Run with `make test-integration`.

## Architecture

### Directory Structure

| Path                 | Purpose                                                  |
|----------------------|----------------------------------------------------------|
| `cmd/rift/main.go`   | All CLI commands (single file, ~760 lines)               |
| `internal/config/`   | Viper-based config loading (YAML + env + defaults)       |
| `internal/ui/`       | Terminal output, prompts, spinners, styles               |
| `docker/`            | Dockerfiles + docker-compose for local dev               |
| `scripts/`           | install.sh (user installer), release.sh (tag automation) |
| `npm/`               | Platform-specific npm package scaffolds                  |
| `bin/rift.js`        | Node.js shim for npm binary resolution                   |
| `tests/integration/` | Integration test suite                                   |

### Configuration

Config is loaded via Viper with this priority (highest first):

1. CLI flags
2. Environment variables (`rift_` prefix, dots → underscores)
3. YAML config file (`./config.yaml`, `~/.rift/config.yaml`, `/etc/rift/config.yaml`)
4. Hardcoded defaults in `DefaultConfig()`

Key environment variables:

- `rift_UPSTREAM_URL` — PostgreSQL connection string (required for `serve`)
- `rift_LISTEN_ADDR` — Proxy listen address (default `:6432`)
- `rift_API_ADDR` — HTTP API address (default `:8080`)
- `rift_DATA_DIR` — Data directory (default `~/.rift`)
- `rift_LOG_LEVEL` — Log level: debug, info, warn, error
- `rift_LOG_FORMAT` — Log format: text, json

### Ports

- **6432** — Postgres wire protocol (proxy)
- **8080** — HTTP API / web dashboard
- **5432** — Upstream PostgreSQL (dev container)
- **5050** — pgAdmin (optional, `docker compose --profile tools up`)

## CI/CD Pipelines

### ci.yml (on push to main, PRs)

1. **Lint:** golangci-lint with 5m timeout
2. **Test:** go vet, go fmt check, unit tests (race detector + coverage), integration tests with PostgreSQL 17 service,
   Codecov upload
3. **Build:** Cross-platform binaries (5 targets), artifact upload (7-day retention)
4. **Security:** gosec + Trivy filesystem scan (CRITICAL/HIGH)

### release.yml (on v* tags)

1. **Release:** Run tests → GoReleaser (binaries, archives, checksums, Homebrew cask, GitHub release)
2. **npm Publish:** Download release archives → extract per-platform → publish `@rift-data/cli-*` packages → publish
   `@rift-data/cli`
3. **Notify:** Announce release

### docker.yml (on push to main, v* tags, PRs)

1. **Build:** Multi-arch Docker image (linux/amd64 + linux/arm64) with SBOM + provenance attestations
2. **Push:** To Docker Hub (`riftdata/rift`) and GHCR (`ghcr.io/riftdata/rift`)
3. **Scan:** Trivy vulnerability scan → SARIF upload to GitHub Code Scanning

## Planned Architecture (Not Yet Implemented)

The following packages are referenced in documentation but do not exist yet:

- `internal/pgwire/` — Postgres wire protocol implementation
- `internal/proxy/` — Connection proxying
- `internal/branch/` — Branch lifecycle management
- `internal/cow/` — Copy-on-write engine (row-level deltas)
- `internal/router/` — Query routing per branch
- `internal/parser/` — SQL parsing
- `internal/storage/` — Delta persistence
- `internal/catalog/` — Schema tracking
- `internal/api/` — HTTP API handlers
- `pkg/rift/` — Public Go client library

When implementing these, follow the patterns established in `internal/config/` and `internal/ui/`: separate packages
with clear interfaces, exported types with unexported fields where possible, and comprehensive error wrapping.

## Security Considerations

- The proxy will handle raw PostgreSQL wire protocol traffic. Input validation and buffer overflow prevention are
  critical in `pgwire`.
- `gosec` is enabled in CI. Do not add `//nolint:gosec` suppressions without documented justification.
- Docker images run as non-root user `rift` (uid 1000).
- The API supports an optional `auth_token` for access control.
- Never log connection strings or credentials. Mask sensitive fields in debug output.

## Dependencies

Direct dependencies are intentionally minimal:

- **cobra/viper** — CLI and config (industry standard)
- **charmbracelet stack** — TUI components (bubbletea, bubbles, huh, lipgloss)
- **yaml.v3** — YAML serialization

No database drivers are included yet. When adding PostgreSQL support, prefer `github.com/jackc/pgx/v5` (pure Go, no
cgo).