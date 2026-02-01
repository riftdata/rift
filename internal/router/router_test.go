package router

import (
	"testing"
)

func TestIsBranchRouted(t *testing.T) {
	tests := []struct {
		name   string
		branch string
		want   bool
	}{
		{"main is not routed", "main", false},
		{"empty is not routed", "", false},
		{"feature branch is routed", "feature-auth", true},
		{"test branch is routed", "test", true},
		{"dev is routed", "dev", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBranchRouted(tt.branch); got != tt.want {
				t.Errorf("IsBranchRouted(%q) = %v, want %v", tt.branch, got, tt.want)
			}
		})
	}
}

func TestIsPassthroughBranch(t *testing.T) {
	tests := []struct {
		name   string
		branch string
		want   bool
	}{
		{"main is passthrough", "main", true},
		{"empty is passthrough", "", true},
		{"feature branch is not passthrough", "feature-auth", false},
		{"test is not passthrough", "test", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPassthroughBranch(tt.branch); got != tt.want {
				t.Errorf("IsPassthroughBranch(%q) = %v, want %v", tt.branch, got, tt.want)
			}
		})
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		expect []string
	}{
		{
			"single statement",
			"SELECT * FROM users",
			[]string{"SELECT * FROM users"},
		},
		{
			"two statements",
			"SELECT 1; SELECT 2",
			[]string{"SELECT 1", "SELECT 2"},
		},
		{
			"trailing semicolon",
			"SELECT 1;",
			[]string{"SELECT 1"},
		},
		{
			"semicolon in single quotes",
			"SELECT 'hello; world' FROM t",
			[]string{"SELECT 'hello; world' FROM t"},
		},
		{
			"semicolon in double quotes",
			`SELECT "col;name" FROM t`,
			[]string{`SELECT "col;name" FROM t`},
		},
		{
			"empty input",
			"",
			nil,
		},
		{
			"only whitespace",
			"   ",
			nil,
		},
		{
			"multiple semicolons",
			"SELECT 1;; SELECT 2",
			[]string{"SELECT 1", "SELECT 2"},
		},
		{
			"complex with quotes",
			"INSERT INTO t VALUES ('a;b'); UPDATE t SET x='c;d' WHERE id=1",
			[]string{"INSERT INTO t VALUES ('a;b')", "UPDATE t SET x='c;d' WHERE id=1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.sql)
			if len(got) != len(tt.expect) {
				t.Fatalf("splitStatements(%q) returned %d statements, want %d: %v",
					tt.sql, len(got), len(tt.expect), got)
			}
			for i := range got {
				if got[i] != tt.expect[i] {
					t.Errorf("splitStatements(%q)[%d] = %q, want %q",
						tt.sql, i, got[i], tt.expect[i])
				}
			}
		})
	}
}

func TestIsBegin(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"BEGIN", true},
		{"begin", true},
		{"BEGIN;", true},
		{"START TRANSACTION", true},
		{"start transaction", true},
		{"START TRANSACTION;", true},
		{"SELECT 1", false},
		{"BEGINNING", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := isBegin(tt.sql); got != tt.want {
				t.Errorf("isBegin(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestIsCommit(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"COMMIT", true},
		{"commit", true},
		{"COMMIT;", true},
		{"END", true},
		{"end", true},
		{"END;", true},
		{"SELECT 1", false},
		{"COMMITTED", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := isCommit(tt.sql); got != tt.want {
				t.Errorf("isCommit(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestIsRollback(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"ROLLBACK", true},
		{"rollback", true},
		{"ROLLBACK;", true},
		{"SELECT 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := isRollback(tt.sql); got != tt.want {
				t.Errorf("isRollback(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestExtendedState(t *testing.T) {
	ext := newExtendedState()

	if len(ext.stmts) != 0 {
		t.Error("new state should have empty stmts")
	}
	if len(ext.portals) != 0 {
		t.Error("new state should have empty portals")
	}

	// Add and retrieve statement
	ext.stmts["s1"] = &preparedStmt{name: "s1", sql: "SELECT 1"}
	if _, ok := ext.stmts["s1"]; !ok {
		t.Error("should find statement s1")
	}

	// Add and retrieve portal
	ext.portals["p1"] = &portal{name: "p1", stmt: ext.stmts["s1"]}
	if _, ok := ext.portals["p1"]; !ok {
		t.Error("should find portal p1")
	}

	// Delete
	delete(ext.stmts, "s1")
	if _, ok := ext.stmts["s1"]; ok {
		t.Error("should not find deleted statement")
	}

	delete(ext.portals, "p1")
	if _, ok := ext.portals["p1"]; ok {
		t.Error("should not find deleted portal")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name   string
		input  interface{}
		expect string
	}{
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int16", int16(42), "42"},
		{"int32", int32(1000), "1000"},
		{"int64", int64(999999), "999999"},
		{"float32", float32(3.14), "3.14"},
		{"float64", float64(2.71828), "2.71828"},
		{"bool true", true, "t"},
		{"bool false", false, "f"},
		{"negative int", int64(-5), "-5"},
		{"zero", int32(0), "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatValue(tt.input)
			if got != tt.expect {
				t.Errorf("formatValue(%v) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}
