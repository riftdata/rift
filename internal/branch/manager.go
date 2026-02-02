package branch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/riftdata/rift/internal/storage"
)

var (
	ErrBranchNotFound = errors.New("branch not found")
	ErrBranchExists   = errors.New("branch already exists")
	ErrMainBranch     = errors.New("cannot modify main branch")
	ErrInvalidName    = errors.New("invalid branch name")
)

// Branch represents a database branch
type Branch struct {
	Name      string    `json:"name"`
	Parent    string    `json:"parent"`
	Database  string    `json:"database"` // Upstream database name
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	TTL       *Duration `json:"ttl,omitempty"`
	Pinned    bool      `json:"pinned"`

	// Stats
	DeltaSize   int64 `json:"delta_size"`
	RowsChanged int64 `json:"rows_changed"`
}

// Duration is a JSON-friendly time.Duration
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// Manager handles branch operations
type Manager struct {
	mu       sync.RWMutex
	branches map[string]*Branch
	dataDir  string

	// The upstream database that "main" points to
	upstreamDB string
}

// NewManager creates a new branch manager
func NewManager(dataDir, upstreamDB string) (*Manager, error) {
	m := &Manager{
		branches:   make(map[string]*Branch),
		dataDir:    dataDir,
		upstreamDB: upstreamDB,
	}

	// Create data directory
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// Load existing branches
	if err := m.load(); err != nil {
		return nil, fmt.Errorf("load branches: %w", err)
	}

	// Ensure main branch exists
	if _, ok := m.branches["main"]; !ok {
		m.branches["main"] = &Branch{
			Name:      "main",
			Parent:    "",
			Database:  upstreamDB,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
			Pinned:    true, // Main is always pinned
		}
		_ = m.save()
	}

	return m, nil
}

// Create creates a new branch
func (m *Manager) Create(name, parent string, ttl *time.Duration) (*Branch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate name
	if name == "" || name == "main" {
		return nil, ErrInvalidName
	}

	// Check if already exists
	if _, ok := m.branches[name]; ok {
		return nil, ErrBranchExists
	}

	// Validate parent
	if parent == "" {
		parent = "main"
	}
	parentBranch, ok := m.branches[parent]
	if !ok {
		return nil, fmt.Errorf("%w: parent %s", ErrBranchNotFound, parent)
	}

	// Create branch
	branch := &Branch{
		Name:      name,
		Parent:    parent,
		Database:  parentBranch.Database, // Inherit the upstream database
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if ttl != nil {
		d := Duration(*ttl)
		branch.TTL = &d
	}

	m.branches[name] = branch

	if err := m.save(); err != nil {
		delete(m.branches, name)
		return nil, err
	}

	return branch, nil
}

// Delete deletes a branch
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "main" {
		return ErrMainBranch
	}

	branch, ok := m.branches[name]
	if !ok {
		return ErrBranchNotFound
	}

	if branch.Pinned {
		return fmt.Errorf("branch is pinned")
	}

	// Check for children
	for _, b := range m.branches {
		if b.Parent == name {
			return fmt.Errorf("branch has children: %s", b.Name)
		}
	}

	delete(m.branches, name)

	// TODO: Clean up delta storage for this branch

	return m.save()
}

// Get returns a branch by name
func (m *Manager) Get(name string) (*Branch, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	branch, ok := m.branches[name]
	if !ok {
		return nil, ErrBranchNotFound
	}
	return branch, nil
}

// List returns all branches
func (m *Manager) List() []*Branch {
	m.mu.RLock()
	defer m.mu.RUnlock()

	branches := make([]*Branch, 0, len(m.branches))
	for _, b := range m.branches {
		branches = append(branches, b)
	}
	return branches
}

// Checks if a branch exists
func (m *Manager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.branches[name]
	return ok
}

// ResolveDatabase returns the upstream database for a branch
func (m *Manager) ResolveDatabase(branchName string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	branch, ok := m.branches[branchName]
	if !ok {
		return "", ErrBranchNotFound
	}

	return branch.Database, nil
}

// Pin pins a branch to prevent deletion
func (m *Manager) Pin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	branch, ok := m.branches[name]
	if !ok {
		return ErrBranchNotFound
	}

	branch.Pinned = true
	branch.UpdatedAt = time.Now()

	return m.save()
}

// Unpin unpins a branch
func (m *Manager) Unpin(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "main" {
		return ErrMainBranch
	}

	branch, ok := m.branches[name]
	if !ok {
		return ErrBranchNotFound
	}

	branch.Pinned = false
	branch.UpdatedAt = time.Now()

	return m.save()
}

// GC removes expired branches
func (m *Manager) GC() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var deleted []string

	for name, branch := range m.branches {
		if branch.TTL != nil && !branch.Pinned {
			expiresAt := branch.CreatedAt.Add(time.Duration(*branch.TTL))
			if now.After(expiresAt) {
				delete(m.branches, name)
				deleted = append(deleted, name)
			}
		}
	}

	if len(deleted) > 0 {
		if err := m.save(); err != nil {
			return nil, err
		}
	}

	return deleted, nil
}

func (m *Manager) metadataPath() string {
	return filepath.Join(m.dataDir, "branches.json")
}

func (m *Manager) load() error {
	data, err := os.ReadFile(m.metadataPath())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var branches []*Branch
	if err := json.Unmarshal(data, &branches); err != nil {
		return err
	}

	for _, b := range branches {
		m.branches[b.Name] = b
	}

	return nil
}

func (m *Manager) save() error {
	branches := make([]*Branch, 0, len(m.branches))
	for _, b := range m.branches {
		branches = append(branches, b)
	}

	data, err := json.MarshalIndent(branches, "", "  ")
	if err != nil {
		return err
	}

	// Atomic write here
	tmp := m.metadataPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, m.metadataPath())
}

// StorageBackedManager wraps a storage.Store to provide branch management
// using PostgreSQL persistence instead of JSON files.
type StorageBackedManager struct {
	store storage.Store
}

// NewStorageBackedManager creates a manager backed by PostgreSQL storage.
func NewStorageBackedManager(store storage.Store) *StorageBackedManager {
	return &StorageBackedManager{store: store}
}

// Create creates a new branch with optional TTL.
func (m *StorageBackedManager) Create(ctx context.Context, name, parent string, ttl *time.Duration) (*Branch, error) {
	if name == "" || name == "main" {
		return nil, ErrInvalidName
	}

	if err := storage.ValidateBranchName(name); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidName, err)
	}

	// Check if already exists
	if _, err := m.store.GetBranch(ctx, name); err == nil {
		return nil, ErrBranchExists
	}

	// Validate parent
	if parent == "" {
		parent = "main"
	}
	parentBranch, err := m.store.GetBranch(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("%w: parent %s", ErrBranchNotFound, parent)
	}

	now := time.Now()
	sb := &storage.Branch{
		Name:      name,
		Parent:    parent,
		Database:  parentBranch.Database,
		CreatedAt: now,
		UpdatedAt: now,
		Status:    "active",
	}

	if ttl != nil {
		secs := int(ttl.Seconds())
		sb.TTLSeconds = &secs
	}

	if err := m.store.CreateBranch(ctx, sb); err != nil {
		return nil, fmt.Errorf("create branch: %w", err)
	}

	// Create the overlay schema
	if err := m.store.CreateBranchSchema(ctx, name); err != nil {
		// Best-effort cleanup of the metadata row
		_ = m.store.DeleteBranch(ctx, name)
		return nil, fmt.Errorf("create branch schema: %w", err)
	}

	return storageBranchToBranch(sb), nil
}

// Delete deletes a branch and its overlay schema.
func (m *StorageBackedManager) Delete(ctx context.Context, name string) error {
	if name == "main" {
		return ErrMainBranch
	}

	b, err := m.store.GetBranch(ctx, name)
	if err != nil {
		return ErrBranchNotFound
	}

	if b.Pinned {
		return fmt.Errorf("branch is pinned")
	}

	// Check for children
	branches, err := m.store.ListBranches(ctx)
	if err != nil {
		return fmt.Errorf("list branches: %w", err)
	}
	for _, child := range branches {
		if child.Parent == name {
			return fmt.Errorf("branch has children: %s", child.Name)
		}
	}

	// Drop overlay schema first
	if err := m.store.DropBranchSchema(ctx, name); err != nil {
		return fmt.Errorf("drop branch schema: %w", err)
	}

	return m.store.DeleteBranch(ctx, name)
}

// Get returns a branch by name.
func (m *StorageBackedManager) Get(ctx context.Context, name string) (*Branch, error) {
	sb, err := m.store.GetBranch(ctx, name)
	if err != nil {
		return nil, ErrBranchNotFound
	}
	return storageBranchToBranch(sb), nil
}

// List returns all branches.
func (m *StorageBackedManager) List(ctx context.Context) ([]*Branch, error) {
	sbs, err := m.store.ListBranches(ctx)
	if err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}

	branches := make([]*Branch, len(sbs))
	for i, sb := range sbs {
		branches[i] = storageBranchToBranch(sb)
	}
	return branches, nil
}

// Checks if a branch exists
func (m *StorageBackedManager) Exists(ctx context.Context, name string) bool {
	_, err := m.store.GetBranch(ctx, name)
	return err == nil
}

// ResolveDatabase returns the upstream database for a branch.
func (m *StorageBackedManager) ResolveDatabase(ctx context.Context, branchName string) (string, error) {
	sb, err := m.store.GetBranch(ctx, branchName)
	if err != nil {
		return "", ErrBranchNotFound
	}
	return sb.Database, nil
}

// Pin pins a branch to prevent deletion.
func (m *StorageBackedManager) Pin(ctx context.Context, name string) error {
	sb, err := m.store.GetBranch(ctx, name)
	if err != nil {
		return ErrBranchNotFound
	}
	sb.Pinned = true
	return m.store.UpdateBranch(ctx, sb)
}

// Unpin unpins a branch.
func (m *StorageBackedManager) Unpin(ctx context.Context, name string) error {
	if name == "main" {
		return ErrMainBranch
	}
	sb, err := m.store.GetBranch(ctx, name)
	if err != nil {
		return ErrBranchNotFound
	}
	sb.Pinned = false
	return m.store.UpdateBranch(ctx, sb)
}

// GC removes expired branches and returns their names.
func (m *StorageBackedManager) GC(ctx context.Context) ([]string, error) {
	branches, err := m.store.ListBranches(ctx)
	if err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}

	now := time.Now()
	var deleted []string

	for _, b := range branches {
		if b.TTLSeconds != nil && !b.Pinned {
			expiresAt := b.CreatedAt.Add(time.Duration(*b.TTLSeconds) * time.Second)
			if now.After(expiresAt) {
				if err := m.store.DropBranchSchema(ctx, b.Name); err != nil {
					return deleted, fmt.Errorf("drop schema for %s: %w", b.Name, err)
				}
				if err := m.store.DeleteBranch(ctx, b.Name); err != nil {
					return deleted, fmt.Errorf("delete branch %s: %w", b.Name, err)
				}
				deleted = append(deleted, b.Name)
			}
		}
	}

	return deleted, nil
}

// Store returns the underlying storage.Store for direct access.
func (m *StorageBackedManager) Store() storage.Store {
	return m.store
}

// storageBranchToBranch converts a storage.Branch to a branch.Branch.
func storageBranchToBranch(sb *storage.Branch) *Branch {
	b := &Branch{
		Name:        sb.Name,
		Parent:      sb.Parent,
		Database:    sb.Database,
		CreatedAt:   sb.CreatedAt,
		UpdatedAt:   sb.UpdatedAt,
		Pinned:      sb.Pinned,
		DeltaSize:   sb.DeltaSize,
		RowsChanged: sb.RowsChanged,
	}

	if sb.TTLSeconds != nil {
		d := Duration(time.Duration(*sb.TTLSeconds) * time.Second)
		b.TTL = &d
	}

	return b
}
