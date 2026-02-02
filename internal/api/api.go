package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/riftdata/rift/internal/branch"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/storage"
)

// Server is the HTTP API server for rift.
type Server struct {
	store   storage.Store
	engine  *cow.Engine
	manager *branch.StorageBackedManager
	server  *http.Server
	addr    string
}

// Config holds API server configuration.
type Config struct {
	ListenAddr string
}

// New creates a new API server.
func New(cfg *Config, store storage.Store, engine *cow.Engine, manager *branch.StorageBackedManager) *Server {
	s := &Server{
		store:   store,
		engine:  engine,
		manager: manager,
		addr:    cfg.ListenAddr,
	}

	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)

	// Branch API
	mux.HandleFunc("GET /api/v1/branches", s.handleListBranches)
	mux.HandleFunc("POST /api/v1/branches", s.handleCreateBranch)
	mux.HandleFunc("GET /api/v1/branches/{name}", s.handleGetBranch)
	mux.HandleFunc("DELETE /api/v1/branches/{name}", s.handleDeleteBranch)
	mux.HandleFunc("GET /api/v1/branches/{name}/status", s.handleBranchStatus)
	mux.HandleFunc("GET /api/v1/branches/{name}/diff", s.handleBranchDiff)

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.server.Addr = ln.Addr().String()

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Printf("api server error: %v\n", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// Addr returns the server's listen address.
func (s *Server) Addr() string {
	return s.server.Addr
}

// --- Health endpoints ---

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Check database connectivity
	if err := s.store.Pool().Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready",
			"error":  "database connection failed",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready",
	})
}

// --- Branch API ---

type branchResponse struct {
	Name        string `json:"name"`
	Parent      string `json:"parent,omitempty"`
	Database    string `json:"database"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	Pinned      bool   `json:"pinned"`
	DeltaSize   int64  `json:"delta_size"`
	RowsChanged int64  `json:"rows_changed"`
	TTLSeconds  *int   `json:"ttl_seconds,omitempty"`
	Status      string `json:"status"`
}

func toBranchResponse(b *storage.Branch) branchResponse {
	return branchResponse{
		Name:        b.Name,
		Parent:      b.Parent,
		Database:    b.Database,
		CreatedAt:   b.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   b.UpdatedAt.Format(time.RFC3339),
		Pinned:      b.Pinned,
		DeltaSize:   b.DeltaSize,
		RowsChanged: b.RowsChanged,
		TTLSeconds:  b.TTLSeconds,
		Status:      b.Status,
	}
}

func (s *Server) handleListBranches(w http.ResponseWriter, r *http.Request) {
	branches, err := s.store.ListBranches(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list branches: %v", err)
		return
	}

	resp := make([]branchResponse, len(branches))
	for i, b := range branches {
		resp[i] = toBranchResponse(b)
	}

	writeJSON(w, http.StatusOK, resp)
}

type createBranchRequest struct {
	Name   string `json:"name"`
	Parent string `json:"parent"`
	TTL    string `json:"ttl,omitempty"` // e.g. "1h", "24h"
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	var req createBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: %v", err)
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Parent == "" {
		req.Parent = "main"
	}

	var ttl *time.Duration
	if req.TTL != "" {
		d, err := time.ParseDuration(req.TTL)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid TTL: %v", err)
			return
		}
		ttl = &d
	}

	if err := s.engine.CreateBranch(r.Context(), req.Name, req.Parent, ttl); err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate key") {
			writeError(w, http.StatusConflict, "branch %q already exists", req.Name)
			return
		}
		writeError(w, http.StatusInternalServerError, "create branch: %v", err)
		return
	}

	b, err := s.store.GetBranch(r.Context(), req.Name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get created branch: %v", err)
		return
	}

	writeJSON(w, http.StatusCreated, toBranchResponse(b))
}

func (s *Server) handleGetBranch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	b, err := s.store.GetBranch(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "branch %q not found", name)
		return
	}

	writeJSON(w, http.StatusOK, toBranchResponse(b))
}

func (s *Server) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	if name == "main" {
		writeError(w, http.StatusBadRequest, "cannot delete main branch")
		return
	}

	if err := s.engine.DeleteBranch(r.Context(), name); err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "branch %q not found", name)
			return
		}
		writeError(w, http.StatusInternalServerError, "delete branch: %v", err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
		"branch": name,
	})
}

type branchStatusResponse struct {
	Branch branchResponse     `json:"branch"`
	Tables []trackedTableInfo `json:"tables"`
}

type trackedTableInfo struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	OverlayTable  string `json:"overlay_table"`
	HasTombstones bool   `json:"has_tombstones"`
	RowCount      int64  `json:"row_count"`
}

func (s *Server) handleBranchStatus(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	b, err := s.store.GetBranch(ctx, name)
	if err != nil {
		writeError(w, http.StatusNotFound, "branch %q not found", name)
		return
	}

	tables, err := s.store.ListTrackedTables(ctx, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list tables: %v", err)
		return
	}

	tableInfos := make([]trackedTableInfo, len(tables))
	for i, t := range tables {
		tableInfos[i] = trackedTableInfo{
			Schema:        t.SourceSchema,
			Table:         t.TableName,
			OverlayTable:  t.OverlayTable,
			HasTombstones: t.HasTombstones,
			RowCount:      t.RowCount,
		}
	}

	writeJSON(w, http.StatusOK, branchStatusResponse{
		Branch: toBranchResponse(b),
		Tables: tableInfos,
	})
}

type diffResponse struct {
	Branch       string          `json:"branch"`
	Parent       string          `json:"parent"`
	TotalChanges int64           `json:"total_changes"`
	Tables       []tableDiffInfo `json:"tables"`
}

type tableDiffInfo struct {
	Table   string `json:"table"`
	Schema  string `json:"schema"`
	Inserts int64  `json:"inserts"`
	Updates int64  `json:"updates"`
	Deletes int64  `json:"deletes"`
}

func (s *Server) handleBranchDiff(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	diff, err := s.engine.Diff(r.Context(), name)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "branch %q not found", name)
			return
		}
		writeError(w, http.StatusInternalServerError, "compute diff: %v", err)
		return
	}

	tables := make([]tableDiffInfo, len(diff.Tables))
	for i, t := range diff.Tables {
		tables[i] = tableDiffInfo{
			Table:   t.TableName,
			Schema:  t.SourceSchema,
			Inserts: t.Inserts,
			Updates: t.Updates,
			Deletes: t.Deletes,
		}
	}

	writeJSON(w, http.StatusOK, diffResponse{
		Branch:       diff.BranchName,
		Parent:       diff.Parent,
		TotalChanges: diff.TotalChanges(),
		Tables:       tables,
	})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	writeJSON(w, status, map[string]string{"error": msg})
}
