# CLAUDE.md

This file provides context for Claude Code when working on the Rift codebase.

## Project Overview

Rift is an instant, copy-on-write database branching system for PostgreSQL. It acts as a Postgres proxy (
`localhost:6432`) that enables creating isolated database branches in milliseconds without duplicating data—only changed
rows are stored. Self-hosted, Apache 2.0 licensed.

**Status:** Early development (v0.0.1). The CLI framework, configuration system, and build/release infrastructure are
production-ready. The core proxy, copy-on-write engine, and branch management are stubbed but not yet implemented.

## Repository Layout

```
rift/
├── cmd/rift/main.go          # Single-file CLI entry point (all 11 cobra commands)
├── internal/
│   ├── config/config.go      # Viper-based config (YAML + env vars)
│   └── ui/                   # Terminal UI components
│       ├── output.go         # Multi-format output (table/JSON/YAML/plain)
│       ├── prompt.go         # Interactive forms (charmbracelet/huh)
│       ├── spinner.go        # Loading indicators (bubbletea)
│       ├── progress.go       # Progress bars
│       └── styles.go         # Color/style constants
├── bin/rift.js               # Node.js shim for npm distribution
├── npm/cli-*/                # Platform-specific npm packages
├── docker/
│   ├── Dockerfile            # Multi-stage build (builder + alpine runtime)
│   ├── Dockerfile.goreleaser # Single-stage runtime (pre-built binary) [UNUSED - kept for reference]
│   └── docker-compose.yml    # Local dev: postgres + rift + pgadmin
├── scripts/
│   ├── install.sh            # Curl-based installer from GitHub releases
│   └── release.sh            # Tag-and-push release script
├── tests/integration/        # Integration tests (//go:build integration)
├── .github/workflows/
│   ├── ci.yml                # Lint, test, build, security scan
│   ├── release.yml           # GoReleaser + npm publish on v* tags
│   └── docker.yml            # Docker build/push with SBOM + provenance
├── .goreleaser.yml           # Cross-platform binary builds, Homebrew, GitHub releases
├── .golangci.yml             # Linter config (gocritic, gocyclo, gosec, misspell)
└── Makefile                  # Development automation (build, test, lint, docker, db)
```

## Tech Stack

- **Language:** Go 1.25.6
- **CLI:** spf13/cobra (commands) + spf13/viper (config)
- **TUI:** charmbracelet/bubbletea, bubbles, huh, lipgloss
- **Build:** Make, GoReleaser v2
- **CI/CD:** GitHub Actions (3 workflows)
- **Docker:** Multi-arch (linux/amd64, linux/arm64), alpine-based
- **Distribution:** Docker Hub, GHCR, Homebrew, npm, direct binary

## Build Commands

```bash
make build              # Build for current platform → bin/rift
make test               # Unit tests (-short)
make test-race          # Unit tests with race detector
make test-integration   # Integration tests (needs PostgreSQL)
make lint               # golangci-lint
make fmt                # gofmt + go mod tidy
make check              # fmt + lint + vet + test (pre-commit gate)
make docker             # Build Docker image locally
make db-up              # Start PostgreSQL dev container
make db-down            # Stop PostgreSQL dev container
```

## Architecture Patterns

### CLI Structure

All commands live in `cmd/rift/main.go`. The root command sets up signal handling (SIGINT/SIGTERM), config loading via
`PersistentPreRunE`, and global flags (`--config`, `--no-color`, `--quiet`, `--verbose`, `--output`).

Commands: `version`, `init`, `serve`, `create`, `list`, `delete`, `status`, `diff`, `merge`, `connect`, `config` (with
subcommands `show`, `set`, `path`), `completion`.

### Configuration Priority

1. CLI flags (highest)
2. Environment variables (prefix `rift_`, dots become underscores: `rift_UPSTREAM_URL`)
3. Config file (YAML): `./config.yaml` → `~/.rift/config.yaml` → `/etc/rift/config.yaml`
4. Defaults (lowest)

### Output System

The `ui.Output` type supports four formats: `table`, `json`, `yaml`, `plain`. Commands should use the semantic methods (
`out.Success()`, `out.Error()`, `out.Warning()`, `out.Info()`, `out.Table()`) rather than raw printing. All user-facing
output respects `--quiet` and `--no-color`.

### Error Handling

- Wrap errors with context: `fmt.Errorf("operation: %w", err)`
- Use sentinel errors for expected conditions: `ErrBranchNotFound`, `ErrBranchExists`
- Check with `errors.Is()`
- Early returns; no nested else blocks

### Logging

Structured logging with key-value pairs:

```go
logger.Info("branch created", "name", branch.Name, "parent", branch.Parent)
```

## Code Style

- Standard Go conventions; enforced by `golangci-lint` with gocritic, gosec, gocyclo (max complexity 15)
- `gofmt -s` for formatting
- No `CGO_ENABLED` — pure Go for cross-compilation
- Conventional Commits: `feat(scope):`, `fix(scope):`, `docs:`, `test:`, `refactor:`, `chore:`
- Branch naming: `feature/`, `fix/`, `docs/`, `refactor/`, `test/`

## Key Ports

| Port | Protocol               | Purpose             |
|------|------------------------|---------------------|
| 6432 | Postgres wire protocol | Proxy connections   |
| 8080 | HTTP                   | API / Web dashboard |
| 5432 | Postgres               | Upstream database   |

## Docker Images

Docker images are built exclusively by `.github/workflows/docker.yml` using `docker/build-push-action` with SBOM and
provenance attestations. GoReleaser does **not** build Docker images (it lacks BuildKit attestation support). Images are
published to both `riftdata/rift` (Docker Hub) and `ghcr.io/riftdata/rift` (GHCR).

## Testing

- Unit tests: `go test -v -short ./...` (race detector enabled in CI)
- Integration tests: `go test -v -tags=integration ./tests/integration/...` (require PostgreSQL)
- Coverage target: >80% on core packages
- CI runs gosec + Trivy for security scanning

## What's Implemented vs Stubbed

**Implemented:** CLI framework, config system, TUI components, multi-format output, build/release infra, Docker, npm
packaging, CI/CD, cross-platform support, shell completions.

**Stubbed (TODO):** Postgres wire protocol proxy, branch creation/deletion, copy-on-write engine, query routing,
connection pooling, storage layer, web dashboard, metrics/observability.

## npm Distribution

The `@rift-data/cli` npm package uses a platform-detection shim (`bin/rift.js`) that resolves the correct
platform-specific optional dependency (`@rift-data/cli-{darwin,linux,win32}-{x64,arm64}`) and executes the compiled Go
binary.