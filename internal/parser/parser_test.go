package parser

import (
	"strings"
	"testing"
)

func TestParseSelect(t *testing.T) {
	pq, err := Parse("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QuerySelect {
		t.Errorf("expected QuerySelect, got %v", pq.Type)
	}
	if len(pq.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(pq.Tables))
	}
	if pq.Tables[0].Name != "users" {
		t.Errorf("expected table 'users', got %q", pq.Tables[0].Name)
	}
	if !pq.IsReadOnly() {
		t.Error("SELECT should be read-only")
	}
}

func TestParseSelectJoin(t *testing.T) {
	pq, err := Parse("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QuerySelect {
		t.Errorf("expected QuerySelect, got %v", pq.Type)
	}
	if len(pq.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(pq.Tables))
	}

	tableNames := make(map[string]bool)
	for _, tbl := range pq.Tables {
		tableNames[tbl.Name] = true
	}
	if !tableNames["users"] || !tableNames["orders"] {
		t.Errorf("expected tables users and orders, got %v", pq.Tables)
	}
}

func TestParseInsert(t *testing.T) {
	pq, err := Parse("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryInsert {
		t.Errorf("expected QueryInsert, got %v", pq.Type)
	}
	if len(pq.Tables) != 1 || pq.Tables[0].Name != "users" {
		t.Errorf("expected table 'users', got %v", pq.Tables)
	}
	if len(pq.TargetColumns) != 2 {
		t.Fatalf("expected 2 target columns, got %d", len(pq.TargetColumns))
	}
	if pq.TargetColumns[0] != "name" || pq.TargetColumns[1] != "email" {
		t.Errorf("expected columns [name, email], got %v", pq.TargetColumns)
	}
	if !pq.IsWrite() {
		t.Error("INSERT should be a write")
	}
}

func TestParseUpdate(t *testing.T) {
	pq, err := Parse("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryUpdate {
		t.Errorf("expected QueryUpdate, got %v", pq.Type)
	}
	if len(pq.Tables) != 1 || pq.Tables[0].Name != "users" {
		t.Errorf("expected table 'users', got %v", pq.Tables)
	}
}

func TestParseDelete(t *testing.T) {
	pq, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryDelete {
		t.Errorf("expected QueryDelete, got %v", pq.Type)
	}
	if len(pq.Tables) != 1 || pq.Tables[0].Name != "users" {
		t.Errorf("expected table 'users', got %v", pq.Tables)
	}
}

func TestParseDDLCreateTable(t *testing.T) {
	pq, err := Parse("CREATE TABLE orders (id SERIAL PRIMARY KEY, total NUMERIC)")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryDDL {
		t.Errorf("expected QueryDDL, got %v", pq.Type)
	}
	if pq.DDLType != DDLCreateTable {
		t.Errorf("expected DDLCreateTable, got %v", pq.DDLType)
	}
	if len(pq.Tables) != 1 || pq.Tables[0].Name != "orders" {
		t.Errorf("expected table 'orders', got %v", pq.Tables)
	}
}

func TestParseDDLAlterTable(t *testing.T) {
	pq, err := Parse("ALTER TABLE users ADD COLUMN age INTEGER")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryDDL {
		t.Errorf("expected QueryDDL, got %v", pq.Type)
	}
	if pq.DDLType != DDLAlterTable {
		t.Errorf("expected DDLAlterTable, got %v", pq.DDLType)
	}
}

func TestParseDDLDropTable(t *testing.T) {
	pq, err := Parse("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryDDL {
		t.Errorf("expected QueryDDL, got %v", pq.Type)
	}
	if pq.DDLType != DDLDropTable {
		t.Errorf("expected DDLDropTable, got %v", pq.DDLType)
	}
}

func TestParseUtilityStatements(t *testing.T) {
	tests := []struct {
		sql string
	}{
		{"SET search_path TO public"},
		{"SHOW search_path"},
		{"BEGIN"},
		{"COMMIT"},
		{"ROLLBACK"},
	}
	for _, tt := range tests {
		pq, err := Parse(tt.sql)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", tt.sql, err)
			continue
		}
		if pq.Type != QueryUtility {
			t.Errorf("Parse(%q): expected QueryUtility, got %v", tt.sql, pq.Type)
		}
	}
}

func TestParseSchemaQualified(t *testing.T) {
	pq, err := Parse("SELECT * FROM myschema.users")
	if err != nil {
		t.Fatal(err)
	}
	if len(pq.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(pq.Tables))
	}
	if pq.Tables[0].Schema != "myschema" || pq.Tables[0].Name != "users" {
		t.Errorf("expected myschema.users, got %s.%s", pq.Tables[0].Schema, pq.Tables[0].Name)
	}
}

func TestIsTransactionControl(t *testing.T) {
	tests := []struct {
		sql    string
		expect bool
	}{
		{"BEGIN", true},
		{"begin", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"START TRANSACTION", true},
		{"SAVEPOINT sp1", true},
		{"RELEASE SAVEPOINT sp1", true},
		{"END", true},
		{"SELECT 1", false},
		{"INSERT INTO t VALUES (1)", false},
	}
	for _, tt := range tests {
		if got := IsTransactionControl(tt.sql); got != tt.expect {
			t.Errorf("IsTransactionControl(%q) = %v, want %v", tt.sql, got, tt.expect)
		}
	}
}

func TestRewriteSelect(t *testing.T) {
	pq, err := Parse("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	configs := map[string]RewriteConfig{
		"users": {
			BranchSchema: "_rift_branch_dev",
			SourceSchema: "public",
			PKColumns:    []string{"id"},
		},
	}

	result, err := RewriteForBranch(pq, configs)
	if err != nil {
		t.Fatal(err)
	}

	if result.IsPassthrough {
		t.Error("expected non-passthrough result")
	}
	if !result.NeedsOverlay {
		t.Error("expected overlay needed")
	}

	// Check CTE structure
	if !strings.Contains(result.SQL, "WITH") {
		t.Error("expected WITH clause")
	}
	if !strings.Contains(result.SQL, "_rift_merged_users") {
		t.Error("expected merged CTE name")
	}
	if !strings.Contains(result.SQL, "_rift_branch_dev") {
		t.Error("expected branch schema reference")
	}
	if !strings.Contains(result.SQL, "NOT _rift_tombstone") {
		t.Error("expected tombstone filter")
	}
	if !strings.Contains(result.SQL, "NOT EXISTS") {
		t.Error("expected NOT EXISTS for dedup")
	}
}

func TestRewriteInsert(t *testing.T) {
	pq, err := Parse("INSERT INTO users (name) VALUES ('Charlie')")
	if err != nil {
		t.Fatal(err)
	}

	configs := map[string]RewriteConfig{
		"users": {
			BranchSchema: "_rift_branch_dev",
			SourceSchema: "public",
			PKColumns:    []string{"id"},
		},
	}

	result, err := RewriteForBranch(pq, configs)
	if err != nil {
		t.Fatal(err)
	}

	if !result.NeedsOverlay {
		t.Error("expected overlay needed")
	}
	if !strings.Contains(result.SQL, "_rift_branch_dev") {
		t.Error("expected branch schema reference")
	}
	if !strings.Contains(result.SQL, "ON CONFLICT") {
		t.Error("expected ON CONFLICT clause")
	}
}

func TestRewriteDelete(t *testing.T) {
	pq, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}

	configs := map[string]RewriteConfig{
		"users": {
			BranchSchema: "_rift_branch_dev",
			SourceSchema: "public",
			PKColumns:    []string{"id"},
		},
	}

	result, err := RewriteForBranch(pq, configs)
	if err != nil {
		t.Fatal(err)
	}

	if !result.NeedsOverlay {
		t.Error("expected overlay needed")
	}
	if !strings.Contains(result.SQL, "_rift_tombstone = true") {
		t.Error("expected tombstone set to true")
	}
	if !strings.Contains(result.SQL, "INSERT INTO") {
		t.Error("expected copy-on-write INSERT")
	}
}

func TestRewritePassthroughUtility(t *testing.T) {
	pq, err := Parse("SET search_path TO public")
	if err != nil {
		t.Fatal(err)
	}

	result, err := RewriteForBranch(pq, nil)
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsPassthrough {
		t.Error("utility statements should be passthrough")
	}
}

func TestRewriteSelectNoOverlay(t *testing.T) {
	pq, err := Parse("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}

	// Empty configs — no overlay tables
	result, err := RewriteForBranch(pq, map[string]RewriteConfig{})
	if err != nil {
		t.Fatal(err)
	}

	if !result.IsPassthrough {
		t.Error("expected passthrough when no overlay config")
	}
}

func TestExtractWhereClause(t *testing.T) {
	tests := []struct {
		sql    string
		expect string
	}{
		{"SELECT * FROM t WHERE id = 1", "id = 1"},
		{"DELETE FROM t WHERE x > 5 ORDER BY x", "x > 5"},
		{"SELECT * FROM t", ""},
		{"UPDATE t SET a = 1 WHERE b = 2 RETURNING *", "b = 2"},
	}
	for _, tt := range tests {
		got := extractWhereClause(tt.sql)
		if got != tt.expect {
			t.Errorf("extractWhereClause(%q) = %q, want %q", tt.sql, got, tt.expect)
		}
	}
}

func TestExtractDDLInfo(t *testing.T) {
	pq, err := Parse("CREATE TABLE orders (id INT PRIMARY KEY)")
	if err != nil {
		t.Fatal(err)
	}

	info := ExtractDDLInfo(pq)
	if info == nil {
		t.Fatal("expected DDLInfo")
	}
	if info.Type != DDLCreateTable {
		t.Errorf("expected DDLCreateTable, got %v", info.Type)
	}
	if info.TableName != "orders" {
		t.Errorf("expected table 'orders', got %q", info.TableName)
	}
}

func TestIsTableDDL(t *testing.T) {
	tests := []struct {
		sql    string
		expect bool
	}{
		{"CREATE TABLE t (id INT)", true},
		{"ALTER TABLE t ADD COLUMN x INT", true},
		{"DROP TABLE t", true},
		{"CREATE INDEX idx ON t (id)", false},
		{"SELECT * FROM t", false},
	}
	for _, tt := range tests {
		pq, err := Parse(tt.sql)
		if err != nil {
			t.Errorf("Parse(%q) error: %v", tt.sql, err)
			continue
		}
		if got := IsTableDDL(pq); got != tt.expect {
			t.Errorf("IsTableDDL(%q) = %v, want %v", tt.sql, got, tt.expect)
		}
	}
}

func TestQueryTypeString(t *testing.T) {
	tests := []struct {
		qt     QueryType
		expect string
	}{
		{QuerySelect, "SELECT"},
		{QueryInsert, "INSERT"},
		{QueryUpdate, "UPDATE"},
		{QueryDelete, "DELETE"},
		{QueryDDL, "DDL"},
		{QueryUtility, "UTILITY"},
		{QueryUnknown, "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.qt.String(); got != tt.expect {
			t.Errorf("QueryType(%d).String() = %q, want %q", tt.qt, got, tt.expect)
		}
	}
}
