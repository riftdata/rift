package storage

import (
	"testing"
)

func TestValidateBranchName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid simple", "feature", false},
		{"valid with hyphen", "my-branch", false},
		{"valid with underscore", "my_branch", false},
		{"valid with numbers", "branch123", false},
		{"valid alphanumeric", "a1b2c3", false},
		{"empty", "", true},
		{"too long", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true}, // 67 chars
		{"starts with hyphen", "-branch", true},
		{"starts with underscore", "_branch", true},
		{"has spaces", "my branch", true},
		{"has dots", "my.branch", true},
		{"has slashes", "my/branch", true},
		{"has special chars", "my@branch", true},
		{"max length 63", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false}, // 63 chars
		{"64 chars", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", true},      // 64 chars
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBranchName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBranchName(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeBranchName(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"simple", "mybranch", "mybranch"},
		{"uppercase", "MyBranch", "mybranch"},
		{"hyphens to underscores", "my-branch", "my_branch"},
		{"dots to underscores", "my.branch", "my_branch"},
		{"slashes to underscores", "my/branch", "my_branch"},
		{"mixed", "My-Branch.v2/test", "my_branch_v2_test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeBranchName(tt.input)
			if got != tt.expect {
				t.Errorf("sanitizeBranchName(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestPgQuoteIdent(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"simple", `"simple"`},
		{"with spaces", `"with spaces"`},
		{`has"quote`, `"has""quote"`},
		{`multi""quotes`, `"multi""""quotes"`},
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

func TestNullIfEmpty(t *testing.T) {
	got := nullIfEmpty("")
	if got != nil {
		t.Errorf("nullIfEmpty(\"\") = %v, want nil", got)
	}

	got = nullIfEmpty("value")
	if got == nil || *got != "value" {
		t.Errorf("nullIfEmpty(\"value\") = %v, want pointer to \"value\"", got)
	}
}

func TestBranchSchemaName(t *testing.T) {
	store := &PgStore{}

	tests := []struct {
		branch string
		expect string
	}{
		{"test", "_rift_branch_test"},
		{"my-branch", "_rift_branch_my_branch"},
		{"My.Feature", "_rift_branch_my_feature"},
		{"feat/auth", "_rift_branch_feat_auth"},
	}

	for _, tt := range tests {
		t.Run(tt.branch, func(t *testing.T) {
			got := store.BranchSchemaName(tt.branch)
			if got != tt.expect {
				t.Errorf("BranchSchemaName(%q) = %q, want %q", tt.branch, got, tt.expect)
			}
		})
	}
}

func TestParseMigrationVersion(t *testing.T) {
	tests := []struct {
		filename string
		wantVer  int
		wantErr  bool
	}{
		{"001_init.sql", 1, false},
		{"002_add_index.sql", 2, false},
		{"100_major.sql", 100, false},
		{"notanumber_test.sql", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got, err := parseMigrationVersion(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMigrationVersion(%q) error = %v, wantErr %v", tt.filename, err, tt.wantErr)
				return
			}
			if got != tt.wantVer {
				t.Errorf("parseMigrationVersion(%q) = %d, want %d", tt.filename, got, tt.wantVer)
			}
		})
	}
}
