//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/server"
	"github.com/riftdata/rift/internal/storage"
)

// testUpstreamURL returns the upstream PostgreSQL connection string.
// Uses RIFT_TEST_UPSTREAM_URL env var or defaults to local dev database.
func testUpstreamURL() string {
	if url := os.Getenv("RIFT_TEST_UPSTREAM_URL"); url != "" {
		return url
	}
	return "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
}

// setupTestDB creates a temporary test database and returns its connection URL.
func setupTestDB(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	baseURL := testUpstreamURL()
	conn, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to upstream: %v", err)
	}

	dbName := fmt.Sprintf("rift_test_%d", time.Now().UnixNano())
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		conn.Close(ctx)
		t.Fatalf("create test database: %v", err)
	}
	conn.Close(ctx)

	// Build URL for the test database
	testURL := fmt.Sprintf("postgres://postgres:postgres@localhost:5432/%s?sslmode=disable", dbName)

	cleanup := func() {
		conn, err := pgx.Connect(ctx, baseURL)
		if err != nil {
			return
		}
		defer conn.Close(ctx)
		// Terminate connections
		_, _ = conn.Exec(ctx, fmt.Sprintf(
			"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='%s'", dbName))
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	}

	return testURL, cleanup
}

func TestStorageInit(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Verify _rift schema exists
	var exists bool
	err = store.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '_rift')`).Scan(&exists)
	if err != nil {
		t.Fatalf("query schema: %v", err)
	}
	if !exists {
		t.Error("_rift schema should exist after Init")
	}

	// Verify main branch exists
	main, err := store.GetBranch(ctx, "main")
	if err != nil {
		t.Fatalf("get main: %v", err)
	}
	if main.Name != "main" {
		t.Errorf("main.Name = %q, want %q", main.Name, "main")
	}
}

func TestStorageBranchCRUD(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Create a branch
	now := time.Now()
	b := &storage.Branch{
		Name:      "test-branch",
		Parent:    "main",
		Database:  "testdb",
		CreatedAt: now,
		UpdatedAt: now,
		Status:    "active",
	}
	if err := store.CreateBranch(ctx, b); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}

	// Get
	got, err := store.GetBranch(ctx, "test-branch")
	if err != nil {
		t.Fatalf("GetBranch: %v", err)
	}
	if got.Name != "test-branch" || got.Parent != "main" {
		t.Errorf("GetBranch = %+v", got)
	}

	// List
	branches, err := store.ListBranches(ctx)
	if err != nil {
		t.Fatalf("ListBranches: %v", err)
	}
	if len(branches) != 2 { // main + test-branch
		t.Errorf("ListBranches returned %d branches, want 2", len(branches))
	}

	// Update
	got.Database = "updateddb"
	if err := store.UpdateBranch(ctx, got); err != nil {
		t.Fatalf("UpdateBranch: %v", err)
	}
	got2, _ := store.GetBranch(ctx, "test-branch")
	if got2.Database != "updateddb" {
		t.Errorf("after update, Database = %q, want %q", got2.Database, "updateddb")
	}

	// Delete
	if err := store.DeleteBranch(ctx, "test-branch"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}
	_, err = store.GetBranch(ctx, "test-branch")
	if err == nil {
		t.Error("GetBranch after delete should fail")
	}
}

func TestStorageBranchSchema(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Create branch schema
	if err := store.CreateBranchSchema(ctx, "test"); err != nil {
		t.Fatalf("CreateBranchSchema: %v", err)
	}

	schemaName := store.BranchSchemaName("test")
	var exists bool
	err = store.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schemaName).Scan(&exists)
	if err != nil || !exists {
		t.Errorf("schema %q should exist", schemaName)
	}

	// Drop
	if err := store.DropBranchSchema(ctx, "test"); err != nil {
		t.Fatalf("DropBranchSchema: %v", err)
	}

	err = store.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schemaName).Scan(&exists)
	if err != nil || exists {
		t.Errorf("schema %q should not exist after drop", schemaName)
	}
}

func TestStoragePrimaryKeyCache(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	pks := []storage.PrimaryKeyColumn{
		{SourceSchema: "public", TableName: "users", ColumnName: "id", Ordinal: 1},
		{SourceSchema: "public", TableName: "users", ColumnName: "tenant_id", Ordinal: 2},
	}

	if err := store.CachePrimaryKeys(ctx, pks); err != nil {
		t.Fatalf("CachePrimaryKeys: %v", err)
	}

	got, err := store.GetPrimaryKeys(ctx, "public", "users")
	if err != nil {
		t.Fatalf("GetPrimaryKeys: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetPrimaryKeys returned %d keys, want 2", len(got))
	}
	if got[0].ColumnName != "id" || got[1].ColumnName != "tenant_id" {
		t.Errorf("GetPrimaryKeys = %+v", got)
	}
}

func TestCowOverlayAndDiff(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Create source table
	_, err = pool.Exec(ctx, `
		CREATE TABLE public.users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT
		)`)
	if err != nil {
		t.Fatalf("create source table: %v", err)
	}

	// Insert source data
	_, err = pool.Exec(ctx,
		`INSERT INTO public.users (name, email) VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com')`)
	if err != nil {
		t.Fatalf("insert source data: %v", err)
	}

	// Create branch schema
	branchSchema := store.BranchSchemaName("test")
	if err := store.CreateBranchSchema(ctx, "test"); err != nil {
		t.Fatalf("CreateBranchSchema: %v", err)
	}

	// Create overlay table
	if err := cow.EnsureOverlayTable(ctx, pool, branchSchema, "public", "users"); err != nil {
		t.Fatalf("EnsureOverlayTable: %v", err)
	}

	// Verify overlay table exists
	exists, err := cow.TableExists(ctx, pool, branchSchema, "users")
	if err != nil || !exists {
		t.Fatal("overlay table should exist")
	}

	// Simulate insert (new row in overlay)
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s."users" (id, name, email, _rift_tombstone) VALUES (100, 'Charlie', 'charlie@test.com', false)`,
		pgQuoteIdent(branchSchema)))
	if err != nil {
		t.Fatalf("insert overlay row: %v", err)
	}

	// Simulate update (copy existing row to overlay)
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s."users" (id, name, email, _rift_tombstone) VALUES (2, 'Robert', 'bob@test.com', false)`,
		pgQuoteIdent(branchSchema)))
	if err != nil {
		t.Fatalf("insert update row: %v", err)
	}

	// Simulate delete (tombstone)
	_, err = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s."users" (id, name, email, _rift_tombstone) VALUES (1, 'Alice', 'alice@test.com', true)`,
		pgQuoteIdent(branchSchema)))
	if err != nil {
		t.Fatalf("insert tombstone: %v", err)
	}

	// Diff
	diff, err := cow.DiffTable(ctx, pool, branchSchema, "public", "users", []string{"id"})
	if err != nil {
		t.Fatalf("DiffTable: %v", err)
	}

	if diff.Inserts != 1 {
		t.Errorf("diff.Inserts = %d, want 1", diff.Inserts)
	}
	if diff.Updates != 1 {
		t.Errorf("diff.Updates = %d, want 1", diff.Updates)
	}
	if diff.Deletes != 1 {
		t.Errorf("diff.Deletes = %d, want 1", diff.Deletes)
	}

	// Overlay row count (non-tombstone)
	rowCount, err := cow.OverlayRowCount(ctx, pool, branchSchema, "users")
	if err != nil {
		t.Fatalf("OverlayRowCount: %v", err)
	}
	if rowCount != 2 {
		t.Errorf("OverlayRowCount = %d, want 2", rowCount)
	}

	// Tombstone count
	tombstones, err := cow.TombstoneCount(ctx, pool, branchSchema, "users")
	if err != nil {
		t.Fatalf("TombstoneCount: %v", err)
	}
	if tombstones != 1 {
		t.Errorf("TombstoneCount = %d, want 1", tombstones)
	}
}

func TestCowMergeSQL(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Create source table
	_, _ = pool.Exec(ctx, `
		CREATE TABLE public.products (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			price NUMERIC(10,2)
		)`)
	_, _ = pool.Exec(ctx,
		`INSERT INTO public.products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.99)`)

	branchSchema := store.BranchSchemaName("merge-test")
	_ = store.CreateBranchSchema(ctx, "merge-test")
	_ = cow.EnsureOverlayTable(ctx, pool, branchSchema, "public", "products")

	// Add overlay changes
	_, _ = pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s."products" (id, name, price, _rift_tombstone) VALUES (100, 'New Item', 29.99, false)`,
		pgQuoteIdent(branchSchema)))

	// Generate merge SQL
	mergeSQL, err := cow.GenerateMergeSQL(ctx, pool, branchSchema, "public", "products", []string{"id"})
	if err != nil {
		t.Fatalf("GenerateMergeSQL: %v", err)
	}

	if len(mergeSQL.Statements) < 3 { // BEGIN, at least one DML, COMMIT
		t.Errorf("expected at least 3 statements, got %d", len(mergeSQL.Statements))
	}

	if mergeSQL.Statements[0] != "BEGIN" {
		t.Errorf("first statement should be BEGIN, got %q", mergeSQL.Statements[0])
	}
	if mergeSQL.Statements[len(mergeSQL.Statements)-1] != "COMMIT" {
		t.Errorf("last statement should be COMMIT")
	}

	formatted := cow.FormatMergeSQL(mergeSQL)
	if formatted == "" {
		t.Error("FormatMergeSQL should not return empty string")
	}
}

func TestCowIntrospection(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	_, _ = pool.Exec(ctx, `
		CREATE TABLE public.test_introspect (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)`)

	// IntrospectTable
	cols, err := cow.IntrospectTable(ctx, pool, "public", "test_introspect")
	if err != nil {
		t.Fatalf("IntrospectTable: %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	// Verify id column
	var idCol cow.ColumnDef
	for _, c := range cols {
		if c.Name == "id" {
			idCol = c
			break
		}
	}
	if !idCol.IsPK {
		t.Error("id column should be marked as PK")
	}

	// GetTablePrimaryKeys
	pks, err := cow.GetTablePrimaryKeys(ctx, pool, "public", "test_introspect")
	if err != nil {
		t.Fatalf("GetTablePrimaryKeys: %v", err)
	}
	if len(pks) != 1 || pks[0] != "id" {
		t.Errorf("PKs = %v, want [id]", pks)
	}

	// TableExists
	exists, err := cow.TableExists(ctx, pool, "public", "test_introspect")
	if err != nil || !exists {
		t.Error("test_introspect should exist")
	}

	exists, err = cow.TableExists(ctx, pool, "public", "nonexistent_table")
	if err != nil || exists {
		t.Error("nonexistent_table should not exist")
	}
}

func TestServerLifecycle(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	cfg := &server.Config{
		UpstreamURL:  testURL,
		ListenAddr:   "127.0.0.1:0", // random port
		UpstreamAddr: "localhost:5432",
		UpstreamUser: "postgres",
		UpstreamPass: "postgres",
	}

	srv := server.New(cfg)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("server.Start: %v", err)
	}

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("server should have an address after start")
	}

	// Verify components are initialized
	if srv.Store() == nil {
		t.Error("Store should not be nil")
	}
	if srv.Engine() == nil {
		t.Error("Engine should not be nil")
	}
	if srv.Manager() == nil {
		t.Error("Manager should not be nil")
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("server.Stop: %v", err)
	}
}

func TestEngineCreateDeleteBranch(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	if err := store.Init(ctx); err != nil {
		t.Fatalf("store.Init: %v", err)
	}

	// Update main branch database
	main, _ := store.GetBranch(ctx, "main")
	main.Database = "testdb"
	_ = store.UpdateBranch(ctx, main)

	engine := cow.NewEngine(store)

	// Create branch
	if err := engine.CreateBranch(ctx, "feature", "main", nil); err != nil {
		t.Fatalf("CreateBranch: %v", err)
	}

	// Verify metadata
	b, err := store.GetBranch(ctx, "feature")
	if err != nil {
		t.Fatalf("GetBranch: %v", err)
	}
	if b.Parent != "main" {
		t.Errorf("branch parent = %q, want %q", b.Parent, "main")
	}

	// Verify schema exists
	schemaName := store.BranchSchemaName("feature")
	var exists bool
	store.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schemaName).Scan(&exists)
	if !exists {
		t.Error("branch schema should exist after create")
	}

	// Delete branch
	if err := engine.DeleteBranch(ctx, "feature"); err != nil {
		t.Fatalf("DeleteBranch: %v", err)
	}

	_, err = store.GetBranch(ctx, "feature")
	if err == nil {
		t.Error("branch should not exist after delete")
	}
}

func TestEngineProcessQueryMainBranch(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()
	_ = store.Init(ctx)

	engine := cow.NewEngine(store)

	// Main branch should always passthrough
	pq, err := engine.ProcessQuery(ctx, "main", "SELECT * FROM users")
	if err != nil {
		t.Fatalf("ProcessQuery: %v", err)
	}
	if !pq.IsPassthrough {
		t.Error("main branch queries should be passthrough")
	}
	if pq.RewrittenSQL != "SELECT * FROM users" {
		t.Errorf("main branch query should be unchanged, got %q", pq.RewrittenSQL)
	}
}

func TestEngineProcessQueryTransactionControl(t *testing.T) {
	testURL, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	store, err := storage.New(ctx, testURL)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()
	_ = store.Init(ctx)

	engine := cow.NewEngine(store)

	// Transaction control should passthrough even on non-main
	for _, sql := range []string{"BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"} {
		pq, err := engine.ProcessQuery(ctx, "feature", sql)
		if err != nil {
			t.Fatalf("ProcessQuery(%q): %v", sql, err)
		}
		if !pq.IsPassthrough {
			t.Errorf("%q should be passthrough on branch", sql)
		}
	}
}

// pgQuoteIdent is duplicated here since the cow package version is unexported.
func pgQuoteIdent(ident string) string {
	return `"` + ident + `"`
}
