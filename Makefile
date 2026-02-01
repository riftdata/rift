.PHONY: build run test lint fmt clean test-race test-cover test-integration dev install release docker docker-push help

# Variables
BINARY := rift
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)"

# Go settings
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
CGO_ENABLED ?= 1

# Directories
BIN_DIR := bin
DIST_DIR := dist
DATA_DIR := data

# Docker
DOCKER_IMAGE := riftdata/rift
DOCKER_TAG ?= $(VERSION)

# Colors
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

##@ General

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

build: ## Build the binary
	@echo "$(GREEN)Building $(BINARY)...$(NC)"
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=$(CGO_ENABLED) go build $(LDFLAGS) -o $(BIN_DIR)/$(BINARY) ./cmd/rift

build-all: ## Build for all platforms (requires zig for cross-compilation)
	@echo "$(GREEN)Building for all platforms...$(NC)"
	@echo "$(YELLOW)Note: Cross-compilation requires zig. Install: https://ziglang.org/download/$(NC)"
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=1 CC="zig cc -target x86_64-linux-gnu"   GOOS=linux   GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(BINARY)-linux-amd64 ./cmd/rift
	CGO_ENABLED=1 CC="zig cc -target aarch64-linux-gnu"  GOOS=linux   GOARCH=arm64 go build $(LDFLAGS) -o $(DIST_DIR)/$(BINARY)-linux-arm64 ./cmd/rift
	CGO_ENABLED=1 CC="zig cc -target x86_64-macos"       GOOS=darwin  GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(BINARY)-darwin-amd64 ./cmd/rift
	CGO_ENABLED=1 CC="zig cc -target aarch64-macos"      GOOS=darwin  GOARCH=arm64 go build $(LDFLAGS) -o $(DIST_DIR)/$(BINARY)-darwin-arm64 ./cmd/rift
	CGO_ENABLED=1 CC="zig cc -target x86_64-windows-gnu" GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(BINARY)-windows-amd64.exe ./cmd/rift

run: build ## Run the proxy server
	./$(BIN_DIR)/$(BINARY) serve

dev: ## Run with hot reload (requires air)
	@which air > /dev/null || go install github.com/cosmtrek/air@latest
	air

install: build ## Install to GOPATH/bin
	@echo "$(GREEN)Installing $(BINARY)...$(NC)"
	cp $(BIN_DIR)/$(BINARY) $(GOPATH)/bin/$(BINARY)

##@ Testing

test: ## Run unit tests
	@echo "$(GREEN)Running tests...$(NC)"
	go test -v -short ./...

test-race: ## Run tests with race detector
	@echo "$(GREEN)Running tests with race detector...$(NC)"
	go test -v -race -short ./...

test-cover: ## Run tests with coverage
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	go test -v -race -short -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report: coverage.html$(NC)"

test-integration: ## Run integration tests (requires PostgreSQL)
	@echo "$(GREEN)Running integration tests...$(NC)"
	go test -v -tags=integration ./tests/integration/...

test-all: test-race test-integration ## Run all tests

bench: ## Run benchmarks
	@echo "$(GREEN)Running benchmarks...$(NC)"
	go test -bench=. -benchmem ./...

##@ Code Quality

lint: ## Run linter
	@echo "$(GREEN)Running linter...$(NC)"
	@which golangci-lint > /dev/null || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run

fmt: ## Format code
	@echo "$(GREEN)Formatting code...$(NC)"
	gofmt -s -w .
	go mod tidy

vet: ## Run go vet
	go vet ./...

check: fmt lint vet test ## Run all checks

##@ Git Hooks

hooks: ## Install git hooks (pre-commit + commit-msg)
	@echo "$(GREEN)Installing git hooks...$(NC)"
	@ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
	@ln -sf ../../scripts/commit-msg .git/hooks/commit-msg
	@echo "$(GREEN)Git hooks installed$(NC)"

##@ Docker

docker: ## Build Docker image
	@echo "$(GREEN)Building Docker image...$(NC)"
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f docker/Dockerfile .
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_IMAGE):latest

docker-push: docker ## Push Docker image
	@echo "$(GREEN)Pushing Docker image...$(NC)"
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

docker-up: ## Start local dev environment with Docker Compose
	docker-compose -f docker/docker-compose.yml up -d

docker-down: ## Stop local dev environment
	docker-compose -f docker/docker-compose.yml down

docker-logs: ## View Docker Compose logs
	docker-compose -f docker/docker-compose.yml logs -f

##@ Database

db-up: ## Start PostgreSQL for development
	docker run -d \
		--name rift-postgres \
		-e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=rift \
		-p 5432:5432 \
		postgres:16
	@echo "$(GREEN)PostgreSQL started on localhost:5432$(NC)"

db-down: ## Stop development PostgreSQL
	docker stop rift-postgres || true
	docker rm rift-postgres || true

db-reset: db-down db-up ## Reset development database

##@ Release

release: ## Create a new release (usage: make release VERSION=v1.0.0)
ifndef VERSION
	$(error VERSION is required. Usage: make release VERSION=v1.0.0)
endif
	@echo "$(GREEN)Creating release $(VERSION)...$(NC)"
	./scripts/release.sh $(VERSION)

changelog: ## Generate changelog
	@which git-chglog > /dev/null || go install github.com/git-chglog/git-chglog/cmd/git-chglog@latest
	git-chglog -o CHANGELOG.md

##@ Documentation

docs: ## Generate documentation
	@echo "$(GREEN)Generating documentation...$(NC)"
	go doc -all ./... > docs/api.txt

docs-serve: ## Serve documentation locally
	@which godoc > /dev/null || go install golang.org/x/tools/cmd/godoc@latest
	@echo "$(GREEN)Documentation server: http://localhost:6060/pkg/github.com/riftdata/rift/$(NC)"
	godoc -http=:6060

##@ Cleanup

clean: ## Clean build artifacts
	@echo "$(GREEN)Cleaning...$(NC)"
	rm -rf $(BIN_DIR) $(DIST_DIR) $(DATA_DIR)
	rm -f coverage.out coverage.html
	go clean -testcache

clean-all: clean db-down docker-down ## Clean everything including Docker

##@ Demo

demo: build ## Run a demo
	@echo "$(GREEN)=== rift Demo ===$(NC)"
	@echo ""
	@echo "Starting PostgreSQL..."
	@make db-up 2>/dev/null || true
	@sleep 3
	@echo ""
	@echo "Initializing rift..."
	./$(BIN_DIR)/$(BINARY) init --upstream postgres://postgres:postgres@localhost:5432/postgres
	@echo ""
	@echo "Starting proxy..."
	./$(BIN_DIR)/$(BINARY) serve &
	@sleep 2
	@echo ""
	@echo "Creating a branch..."
	./$(BIN_DIR)/$(BINARY) create demo-branch
	@echo ""
	@echo "Listing branches..."
	./$(BIN_DIR)/$(BINARY) list
	@echo ""
	@echo "$(GREEN)Demo complete! Connect with: psql postgres://localhost:6432/demo-branch$(NC)"

.DEFAULT_GOAL := help
