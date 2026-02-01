package server

import (
	"context"
	"fmt"

	"github.com/riftdata/rift/internal/api"
	"github.com/riftdata/rift/internal/branch"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/proxy"
	"github.com/riftdata/rift/internal/router"
	"github.com/riftdata/rift/internal/storage"
)

// Config holds server configuration.
type Config struct {
	// Upstream PostgreSQL connection string
	UpstreamURL string

	// Proxy settings
	ListenAddr   string
	UpstreamAddr string
	UpstreamUser string
	UpstreamPass string

	// HTTP API settings
	APIAddr string // e.g. ":8080"

	// Limits
	MaxConnections int
}

// Server orchestrates all rift components: storage, engine, router, proxy, API.
type Server struct {
	config  *Config
	store   storage.Store
	engine  *cow.Engine
	manager *branch.StorageBackedManager
	proxy   *proxy.Proxy
	router  *router.Router
	api     *api.Server
}

// New creates a new server with the given config.
func New(cfg *Config) *Server {
	return &Server{config: cfg}
}

// Start initializes storage, engine, router, proxy and starts serving.
func (s *Server) Start(ctx context.Context) error {
	// Initialize storage
	store, err := storage.New(ctx, s.config.UpstreamURL)
	if err != nil {
		return fmt.Errorf("connect to upstream: %w", err)
	}
	s.store = store

	if err := store.Init(ctx); err != nil {
		store.Close()
		return fmt.Errorf("initialize storage: %w", err)
	}

	// Create engine and manager
	s.engine = cow.NewEngine(store)
	s.manager = branch.NewStorageBackedManager(store)

	// Create router
	s.router = router.New(store.Pool(), s.engine)

	// Create and configure proxy
	s.proxy = proxy.New(s.buildProxyConfig())
	s.proxy.Router = s.router

	// Set up authentication — accept any credentials that match upstream user,
	// or accept all if no upstream user is configured.
	s.proxy.Authenticate = func(user, database, password string) error {
		if s.config.UpstreamUser != "" && user != s.config.UpstreamUser {
			return fmt.Errorf("unknown user %q", user)
		}
		if s.config.UpstreamPass != "" && password != s.config.UpstreamPass {
			return fmt.Errorf("password authentication failed for user %q", user)
		}
		return nil
	}

	// Set up branch resolution hook
	s.proxy.OnConnect = func(database string) (string, error) {
		if database == "main" || database == "" {
			return database, nil
		}
		// Verify branch exists
		if !s.manager.Exists(ctx, database) {
			return "", fmt.Errorf("branch %q not found", database)
		}
		db, err := s.manager.ResolveDatabase(ctx, database)
		if err != nil {
			return "", err
		}
		return db, nil
	}

	// Start proxy
	if err := s.proxy.Start(); err != nil {
		store.Close()
		return fmt.Errorf("start proxy: %w", err)
	}

	// Start HTTP API if configured
	if s.config.APIAddr != "" {
		apiCfg := &api.Config{ListenAddr: s.config.APIAddr}
		s.api = api.New(apiCfg, store, s.engine, s.manager)
		if err := s.api.Start(); err != nil {
			_ = s.proxy.Stop()
			store.Close()
			return fmt.Errorf("start api: %w", err)
		}
	}

	return nil
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	var firstErr error

	if s.api != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*1e9) // 5s
		if err := s.api.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		cancel()
	}

	if s.proxy != nil {
		if err := s.proxy.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if s.store != nil {
		s.store.Close()
	}

	return firstErr
}

// Store returns the underlying storage for direct access.
func (s *Server) Store() storage.Store {
	return s.store
}

// Engine returns the CoW engine.
func (s *Server) Engine() *cow.Engine {
	return s.engine
}

// Manager returns the branch manager.
func (s *Server) Manager() *branch.StorageBackedManager {
	return s.manager
}

// Addr returns the proxy listen address.
func (s *Server) Addr() string {
	if s.proxy != nil && s.proxy.Addr() != nil {
		return s.proxy.Addr().String()
	}
	return ""
}

// APIAddr returns the HTTP API listen address.
func (s *Server) APIAddr() string {
	if s.api != nil {
		return s.api.Addr()
	}
	return ""
}

// buildProxyConfig creates a proxy config from the server config.
func (s *Server) buildProxyConfig() *proxy.Config {
	cfg := proxy.DefaultConfig()
	if s.config.ListenAddr != "" {
		cfg.ListenAddr = s.config.ListenAddr
	}
	if s.config.UpstreamAddr != "" {
		cfg.UpstreamAddr = s.config.UpstreamAddr
	}
	if s.config.UpstreamUser != "" {
		cfg.UpstreamUser = s.config.UpstreamUser
	}
	if s.config.UpstreamPass != "" {
		cfg.UpstreamPass = s.config.UpstreamPass
	}
	if s.config.MaxConnections > 0 {
		cfg.MaxConnections = s.config.MaxConnections
	}
	return cfg
}
