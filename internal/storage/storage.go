package storage

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Branch represents branch metadata stored in _rift.branches.
type Branch struct {
	Name        string
	Parent      string
	Database    string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	TTLSeconds  *int
	Pinned      bool
	DeltaSize   int64
	RowsChanged int64
	Status      string
}

// TrackedTable represents an overlay table entry in _rift.branch_tables.
type TrackedTable struct {
	BranchName    string
	SourceSchema  string
	TableName     string
	OverlayTable  string
	HasTombstones bool
	RowCount      int64
}

// PrimaryKeyColumn represents a column in a table's primary key.
type PrimaryKeyColumn struct {
	SourceSchema string
	TableName    string
	ColumnName   string
	Ordinal      int
}

// Store defines the interface for rift's PostgreSQL-backed storage.
type Store interface {
	// Init runs migrations and ensures the _rift schema exists.
	Init(ctx context.Context) error

	// Close releases the connection pool.
	Close()

	// Pool returns the underlying connection pool for direct queries.
	Pool() *pgxpool.Pool

	// --- Branch CRUD ---

	CreateBranch(ctx context.Context, b *Branch) error
	GetBranch(ctx context.Context, name string) (*Branch, error)
	ListBranches(ctx context.Context) ([]*Branch, error)
	UpdateBranch(ctx context.Context, b *Branch) error
	DeleteBranch(ctx context.Context, name string) error

	// --- Branch overlay schema ---

	// CreateBranchSchema creates the _rift_branch_<name> schema.
	CreateBranchSchema(ctx context.Context, branchName string) error

	// DropBranchSchema drops the _rift_branch_<name> schema and all its contents.
	DropBranchSchema(ctx context.Context, branchName string) error

	// BranchSchemaName returns the schema name for a branch.
	BranchSchemaName(branchName string) string

	// --- Table tracking ---

	TrackTable(ctx context.Context, t *TrackedTable) error
	UntrackTable(ctx context.Context, branchName, sourceSchema, tableName string) error
	ListTrackedTables(ctx context.Context, branchName string) ([]*TrackedTable, error)
	UpdateTrackedTableRowCount(ctx context.Context, branchName, sourceSchema, tableName string, rowCount int64) error

	// --- Primary key cache ---

	CachePrimaryKeys(ctx context.Context, keys []PrimaryKeyColumn) error
	GetPrimaryKeys(ctx context.Context, sourceSchema, tableName string) ([]PrimaryKeyColumn, error)
}
