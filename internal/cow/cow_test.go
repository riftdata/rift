package cow

import (
	"testing"
)

func TestPgQuoteIdent(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"simple", `"simple"`},
		{"with spaces", `"with spaces"`},
		{`has"quote`, `"has""quote"`},
		{"_rift_branch_test", `"_rift_branch_test"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := pgQuoteIdent(tt.input)
			if got != tt.expect {
				t.Errorf("pgQuoteIdent(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestQuoteIdents(t *testing.T) {
	input := []string{"id", "name", "email"}
	got := quoteIdents(input)

	if len(got) != 3 {
		t.Fatalf("quoteIdents returned %d elements, want 3", len(got))
	}
	if got[0] != `"id"` || got[1] != `"name"` || got[2] != `"email"` {
		t.Errorf("quoteIdents(%v) = %v", input, got)
	}
}

func TestBuildPKJoin(t *testing.T) {
	tests := []struct {
		name   string
		left   string
		right  string
		pkCols []string
		expect string
	}{
		{
			"single pk",
			"ovr", "src", []string{"id"},
			`ovr."id" = src."id"`,
		},
		{
			"composite pk",
			"ovr", "src", []string{"user_id", "order_id"},
			`ovr."user_id" = src."user_id" AND ovr."order_id" = src."order_id"`,
		},
		{
			"three columns",
			"a", "b", []string{"x", "y", "z"},
			`a."x" = b."x" AND a."y" = b."y" AND a."z" = b."z"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPKJoin(tt.left, tt.right, tt.pkCols)
			if got != tt.expect {
				t.Errorf("buildPKJoin(%q, %q, %v) = %q, want %q",
					tt.left, tt.right, tt.pkCols, got, tt.expect)
			}
		})
	}
}

func TestBranchDiffTotalChanges(t *testing.T) {
	diff := &BranchDiff{
		BranchName: "test",
		Parent:     "main",
		Tables: []TableDiff{
			{TableName: "users", Inserts: 3, Updates: 2, Deletes: 1},
			{TableName: "orders", Inserts: 5, Updates: 0, Deletes: 0},
		},
	}

	got := diff.TotalChanges()
	if got != 11 {
		t.Errorf("TotalChanges() = %d, want 11", got)
	}
}

func TestBranchDiffTotalChangesEmpty(t *testing.T) {
	diff := &BranchDiff{
		BranchName: "test",
		Parent:     "main",
	}

	got := diff.TotalChanges()
	if got != 0 {
		t.Errorf("TotalChanges() = %d, want 0", got)
	}
}

func TestFormatMergeSQL(t *testing.T) {
	m := &MergeSQL{
		Statements: []string{"BEGIN", "DELETE FROM public.users WHERE id=1", "COMMIT"},
		TableName:  "users",
	}

	got := FormatMergeSQL(m)
	expected := "BEGIN;\nDELETE FROM public.users WHERE id=1;\nCOMMIT;"
	if got != expected {
		t.Errorf("FormatMergeSQL() = %q, want %q", got, expected)
	}
}

func TestProcessedQueryTypes(t *testing.T) {
	// Verify the ProcessedQuery struct fields work correctly
	pq := &ProcessedQuery{
		OriginalSQL:   "SELECT * FROM users",
		RewrittenSQL:  "WITH branch_users AS (...) SELECT * FROM branch_users",
		NeedsOverlay:  true,
		IsPassthrough: false,
		TableName:     "users",
	}

	if pq.IsPassthrough {
		t.Error("expected IsPassthrough to be false")
	}
	if !pq.NeedsOverlay {
		t.Error("expected NeedsOverlay to be true")
	}
	if pq.TableName != "users" {
		t.Errorf("TableName = %q, want %q", pq.TableName, "users")
	}
}
