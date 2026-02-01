# Contributing to RiftDB

Thanks for your interest in contributing to Rift! This project is in early development, and contributions are welcome.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Making Changes](#making-changes)
- [Code Style](#code-style)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)
- [Community](#community)

## Getting Started

### Prerequisites

- Go 1.25.6 or later
- PostgreSQL 14+ (for testing)
- Docker (optional, for integration tests)
- Make

### Quick Start
```bash
# Clone the repository
git clone https://github.com/riftdata/rift.git
cd rift

# Install dependencies
go mod download

# Run tests
make test

# Build
make build

# Run locally
./bin/rift --help
```

## Development Setup

### 1. Fork and Clone
```bash
git clone https://github.com/riftdata/rift.git
cd rift
git remote add upstream https://github.com/riftdata/rift.git
```

### 2. Set Up PostgreSQL

You need a local PostgreSQL instance for development and testing:
```bash
# Using Docker (recommended)
docker run -d \
  --name rift-dev \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:16

# Or use your system PostgreSQL
createdb rift_test
```

### 3. Environment Variables
```bash
# Create a .env file for local development
cat > .env << EOF
rift_UPSTREAM_URL=postgres://postgres:postgres@localhost:5432/postgres
rift_LISTEN_ADDR=:6432
rift_DATA_DIR=./data
rift_LOG_LEVEL=debug
EOF
```

### 4. Verify Setup
```bash
# Run the test suite
make test

# Run with race detector
make test-race

# Run integration tests (requires PostgreSQL)
make test-integration
```

## Project Structure
```
rift/
├── cmd/rift/         # CLI entry point
├── internal/
│   ├── pgwire/           # Postgres wire protocol (start here to understand)
│   ├── proxy/            # Connection proxying
│   ├── branch/           # Branch management
│   ├── cow/              # Copy-on-write engine
│   ├── router/           # Query routing
│   ├── parser/           # SQL parsing
│   ├── storage/          # Delta storage
│   ├── catalog/          # Schema tracking
│   ├── cli/              # CLI commands
│   └── api/              # HTTP API
├── pkg/rift/         # Public Go client
└── docs/                 # Documentation
```

### Key Packages to Understand

| Package   | Description                           | Good First Issues                 |
|-----------|---------------------------------------|-----------------------------------|
| `pgwire`  | Postgres wire protocol implementation | Message parsing, type handling    |
| `branch`  | Branch CRUD and lifecycle             | Metadata, TTL, garbage collection |
| `cli`     | User-facing commands                  | New commands, output formatting   |
| `storage` | Delta persistence                     | Compression, cleanup              |

## Making Changes

### 1. Create a Branch
```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

Branch naming conventions:
- `feature/` — New features
- `fix/` — Bug fixes
- `docs/` — Documentation changes
- `refactor/` — Code refactoring
- `test/` — Test additions or fixes

### 2. Make Your Changes

- Write clear, idiomatic Go code
- Add tests for new functionality
- Update documentation if needed
- Keep commits focused and atomic

### 3. Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):
```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat` — New feature
- `fix` — Bug fix
- `docs` — Documentation
- `test` — Tests
- `refactor` — Code refactoring
- `perf` — Performance improvement
- `chore` — Maintenance

Examples:
```
feat(branch): add TTL support for auto-deletion

fix(pgwire): handle extended query protocol correctly

docs(readme): add CI integration examples

test(cow): add concurrent write tests
```

## Code Style

### Go Guidelines

We follow standard Go conventions:
```go
// Good: Clear, idiomatic Go
func (b *Branch) Delete(ctx context.Context) error {
    if b.pinned {
        return ErrBranchPinned
    }
    
    if err := b.storage.DeleteOverlay(ctx, b.id); err != nil {
        return fmt.Errorf("delete overlay: %w", err)
    }
    
    return nil
}

// Avoid: Overly clever or dense code
func (b *Branch) Delete(ctx context.Context) error {
    return map[bool]func() error{true: func() error { return ErrBranchPinned }, false: func() error { return b.storage.DeleteOverlay(ctx, b.id) }}[b.pinned]()
}
```

### Formatting and Linting
```bash
# Format code
make fmt

# Run linter
make lint

# Both are required to pass before merging
```

### Error Handling
```go
// Wrap errors with context
if err != nil {
    return fmt.Errorf("failed to create branch %q: %w", name, err)
}

// Use sentinel errors for expected conditions
var (
    ErrBranchNotFound = errors.New("branch not found")
    ErrBranchExists   = errors.New("branch already exists")
)

// Check for errors.Is
if errors.Is(err, ErrBranchNotFound) {
    // handle not found
}
```

### Logging
```go
// Use structured logging
logger.Info("branch created",
    "name", branch.Name,
    "parent", branch.Parent,
    "created_at", branch.CreatedAt)

// Debug for verbose output
logger.Debug("query intercepted",
    "query", query,
    "tables", tables)

// Error for failures
logger.Error("failed to connect to upstream",
    "err", err,
    "upstream", config.UpstreamURL)
```

## Testing

### Running Tests
```bash
# Unit tests
make test

# With race detector
make test-race

# Integration tests (requires PostgreSQL)
make test-integration

# All tests
make test-all

# Specific package
go test -v ./internal/branch/...

# Specific test
go test -v -run TestBranchCreate ./internal/branch/
```

### Writing Tests
```go
func TestBranchCreate(t *testing.T) {
    // Setup
    mgr := setupTestManager(t)
    
    // Test
    branch, err := mgr.Create(context.Background(), "test-branch", CreateOptions{
        Parent: "main",
    })
    
    // Assert
    require.NoError(t, err)
    assert.Equal(t, "test-branch", branch.Name)
    assert.Equal(t, "main", branch.Parent)
}

func TestBranchCreate_AlreadyExists(t *testing.T) {
    mgr := setupTestManager(t)
    
    // Create first branch
    _, err := mgr.Create(context.Background(), "test-branch", CreateOptions{})
    require.NoError(t, err)
    
    // Try to create again
    _, err = mgr.Create(context.Background(), "test-branch", CreateOptions{})
    assert.ErrorIs(t, err, ErrBranchExists)
}
```

### Test Organization

- `*_test.go` — Unit tests, same package
- `internal/testutil/` — Shared test helpers
- `tests/integration/` — Integration tests

### Test Coverage

We aim for >80% coverage on core packages (`pgwire`, `cow`, `branch`, `router`).
```bash
# Generate coverage report
make test-cover

# View in browser
go tool cover -html=coverage.out
```

## Submitting Changes

### Pull Request Process

1. **Update your branch**

   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run all checks**

   ```bash
   make fmt
   make lint
   make test
   ```

3. **Push your branch**

   ```bash
   git push origin feature/your-feature-name
   ```

4. **Open a Pull Request**
    - Use a clear, descriptive title
    - Reference any related issues
    - Describe what changed and why
    - Include testing instructions if applicable

### PR Template
```markdown
## Description
Brief description of changes.

## Related Issues
Fixes #123

## Changes
- Added X
- Fixed Y
- Updated Z

## Testing
How to test these changes:
1. ...
2. ...

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] `make lint` passes
- [ ] `make test` passes
```

### Review Process

1. CI must pass (tests, lint, build)
2. At least one maintainer approval is required
3. Address review feedback
4. Squash and merge when approved

## Reporting Issues

### Bug Reports

Include:

- rift version (`rift --version`)
- PostgreSQL version
- Operating system
- Steps to reproduce
- Expected vs. actual behavior
- Relevant logs (with sensitive info removed)
```markdown
**Version:** rift v0.1.0, PostgreSQL 16.1, macOS 14.0

**Steps to reproduce:**

1. Create a branch: `rift create test`
2. Connect: `psql postgres://localhost:6432/test`
3. Run: `SELECT * FROM users`

**Expected:** Query returns results
**Actual:** Connection hangs

**Logs:**
```
[DEBUG] query intercepted: SELECT * FROM users
[ERROR] upstream timeout after 30 seconds
```
```

### Feature Requests

Include:
- Use-case / problem you're trying to solve
- Proposed solution (if any)
- Alternatives considered

## Community

### Getting Help

- **GitHub Discussions** — Questions, ideas, general discussion
- **GitHub Issues** — Bug reports, feature requests

### Good First Issues

Look for issues labeled `good first issue` — these are specifically chosen for new contributors.

Current areas where help is especially welcome:
- Documentation improvements
- CLI ergonomics
- Test coverage
- Error messages

### Code of Conduct

Be respectful and constructive. We're all here to build something useful.

- Be welcoming to newcomers
- Assume good intent
- Focus on the code, not the person
- Accept constructive criticism gracefully

---

## Thank You!

Every contribution matters, whether it's:
- Fixing a typo in docs
- Reporting a bug
- Suggesting a feature
- Writing code

We appreciate your time and effort in making rift better.
