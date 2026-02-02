package branch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Main branch should exist
	if !m.Exists("main") {
		t.Error("main branch should exist after NewManager")
	}

	main, err := m.Get("main")
	if err != nil {
		t.Fatalf("Get(main) error = %v", err)
	}
	if main.Database != "testdb" {
		t.Errorf("main.Database = %q, want %q", main.Database, "testdb")
	}
	if !main.Pinned {
		t.Error("main branch should be pinned")
	}
}

func TestManagerCreate(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	ttl := 1 * time.Hour
	branch, err := m.Create("feature", "main", &ttl)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if branch.Name != "feature" {
		t.Errorf("branch.Name = %q, want %q", branch.Name, "feature")
	}
	if branch.Parent != "main" {
		t.Errorf("branch.Parent = %q, want %q", branch.Parent, "main")
	}
	if branch.Database != "testdb" {
		t.Errorf("branch.Database = %q, want %q", branch.Database, "testdb")
	}
	if branch.TTL == nil {
		t.Error("branch.TTL should not be nil")
	}
}

func TestManagerCreateDuplicate(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("feature", "main", nil)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}

	_, err = m.Create("feature", "main", nil)
	if err != ErrBranchExists {
		t.Errorf("second Create() error = %v, want ErrBranchExists", err)
	}
}

func TestManagerCreateInvalidName(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("", "main", nil)
	if err != ErrInvalidName {
		t.Errorf("Create(\"\") error = %v, want ErrInvalidName", err)
	}

	_, err = m.Create("main", "", nil)
	if err != ErrInvalidName {
		t.Errorf("Create(\"main\") error = %v, want ErrInvalidName", err)
	}
}

func TestManagerCreateInvalidParent(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("feature", "nonexistent", nil)
	if err == nil {
		t.Error("Create() with nonexistent parent should fail")
	}
}

func TestManagerDelete(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("feature", "main", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = m.Delete("feature")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if m.Exists("feature") {
		t.Error("branch should not exist after Delete")
	}
}

func TestManagerDeleteMain(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	err = m.Delete("main")
	if err != ErrMainBranch {
		t.Errorf("Delete(main) error = %v, want ErrMainBranch", err)
	}
}

func TestManagerDeleteNonexistent(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	err = m.Delete("nonexistent")
	if err != ErrBranchNotFound {
		t.Errorf("Delete(nonexistent) error = %v, want ErrBranchNotFound", err)
	}
}

func TestManagerDeleteWithChildren(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("parent", "main", nil)
	if err != nil {
		t.Fatalf("Create(parent) error = %v", err)
	}

	_, err = m.Create("child", "parent", nil)
	if err != nil {
		t.Fatalf("Create(child) error = %v", err)
	}

	err = m.Delete("parent")
	if err == nil {
		t.Error("Delete(parent) should fail when it has children")
	}
}

func TestManagerDeletePinned(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m.Create("feature", "main", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	err = m.Pin("feature")
	if err != nil {
		t.Fatalf("Pin() error = %v", err)
	}

	err = m.Delete("feature")
	if err == nil {
		t.Error("Delete() should fail for pinned branch")
	}
}

func TestManagerList(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, _ = m.Create("b1", "main", nil)
	_, _ = m.Create("b2", "main", nil)

	branches := m.List()
	if len(branches) != 3 { // main + b1 + b2
		t.Errorf("List() returned %d branches, want 3", len(branches))
	}
}

func TestManagerPinUnpin(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, _ = m.Create("feature", "main", nil)

	err = m.Pin("feature")
	if err != nil {
		t.Fatalf("Pin() error = %v", err)
	}

	b, _ := m.Get("feature")
	if !b.Pinned {
		t.Error("branch should be pinned after Pin()")
	}

	err = m.Unpin("feature")
	if err != nil {
		t.Fatalf("Unpin() error = %v", err)
	}

	b, _ = m.Get("feature")
	if b.Pinned {
		t.Error("branch should not be pinned after Unpin()")
	}
}

func TestManagerUnpinMain(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	err = m.Unpin("main")
	if err != ErrMainBranch {
		t.Errorf("Unpin(main) error = %v, want ErrMainBranch", err)
	}
}

func TestManagerResolveDatabase(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	db, err := m.ResolveDatabase("main")
	if err != nil {
		t.Fatalf("ResolveDatabase() error = %v", err)
	}
	if db != "testdb" {
		t.Errorf("ResolveDatabase(main) = %q, want %q", db, "testdb")
	}

	_, err = m.ResolveDatabase("nonexistent")
	if err != ErrBranchNotFound {
		t.Errorf("ResolveDatabase(nonexistent) error = %v, want ErrBranchNotFound", err)
	}
}

func TestManagerGC(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Create a branch with a very short TTL
	ttl := time.Nanosecond
	_, err = m.Create("ephemeral", "main", &ttl)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Wait for it to expire
	time.Sleep(time.Millisecond)

	deleted, err := m.GC()
	if err != nil {
		t.Fatalf("GC() error = %v", err)
	}

	if len(deleted) != 1 || deleted[0] != "ephemeral" {
		t.Errorf("GC() deleted = %v, want [ephemeral]", deleted)
	}

	if m.Exists("ephemeral") {
		t.Error("ephemeral branch should be deleted after GC")
	}
}

func TestManagerPersistence(t *testing.T) {
	dir := t.TempDir()

	// Create manager and add a branch
	m1, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	_, err = m1.Create("persist-test", "main", nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	// Create new manager instance — should load from disk
	m2, err := NewManager(dir, "testdb")
	if err != nil {
		t.Fatalf("NewManager() reload error = %v", err)
	}

	if !m2.Exists("persist-test") {
		t.Error("branch should persist across manager instances")
	}
}

func TestManagerMetadataPath(t *testing.T) {
	dir := t.TempDir()
	m, _ := NewManager(dir, "testdb")

	expected := filepath.Join(dir, "branches.json")
	if m.metadataPath() != expected {
		t.Errorf("metadataPath() = %q, want %q", m.metadataPath(), expected)
	}
}

func TestManagerLoadCorruptedFile(t *testing.T) {
	dir := t.TempDir()

	// Write corrupted JSON
	err := os.WriteFile(filepath.Join(dir, "branches.json"), []byte("not json"), 0o600)
	if err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	_, err = NewManager(dir, "testdb")
	if err == nil {
		t.Error("NewManager() should fail with corrupted data file")
	}
}

func TestDurationJSON(t *testing.T) {
	d := Duration(5 * time.Minute)

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}

	var d2 Duration
	err = json.Unmarshal(data, &d2)
	if err != nil {
		t.Fatalf("Unmarshal error = %v", err)
	}

	if time.Duration(d2) != 5*time.Minute {
		t.Errorf("round-tripped duration = %v, want 5m", time.Duration(d2))
	}
}

func TestDurationJSONInvalid(t *testing.T) {
	var d Duration
	err := json.Unmarshal([]byte(`"not-a-duration"`), &d)
	if err == nil {
		t.Error("Unmarshal should fail for invalid duration string")
	}

	err = json.Unmarshal([]byte(`123`), &d)
	if err == nil {
		t.Error("Unmarshal should fail for non-string value")
	}
}
