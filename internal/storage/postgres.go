package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var branchNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// PgStore implements Store using a PostgreSQL connection pool.
type PgStore struct {
	pool *pgxpool.Pool
}

// New creates a new PgStore from a connection string.
func New(ctx context.Context, connString string) (*PgStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping upstream: %w", err)
	}
	return &PgStore{pool: pool}, nil
}

func (s *PgStore) Init(ctx context.Context) error {
	return runMigrations(ctx, s.pool)
}

func (s *PgStore) Close() {
	s.pool.Close()
}

func (s *PgStore) Pool() *pgxpool.Pool {
	return s.pool
}

// --- Branch CRUD ---

func (s *PgStore) CreateBranch(ctx context.Context, b *Branch) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO _rift.branches (name, parent, database, created_at, updated_at, ttl_seconds, pinned, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		b.Name, nullIfEmpty(b.Parent), b.Database,
		b.CreatedAt, b.UpdatedAt, b.TTLSeconds, b.Pinned, b.Status)
	if err != nil {
		return fmt.Errorf("insert branch: %w", err)
	}
	return nil
}

func (s *PgStore) GetBranch(ctx context.Context, name string) (*Branch, error) {
	b := &Branch{}
	var parent *string
	err := s.pool.QueryRow(ctx,
		`SELECT name, parent, database, created_at, updated_at, ttl_seconds, pinned, delta_size, rows_changed, status
		 FROM _rift.branches WHERE name = $1`, name).Scan(
		&b.Name, &parent, &b.Database, &b.CreatedAt, &b.UpdatedAt,
		&b.TTLSeconds, &b.Pinned, &b.DeltaSize, &b.RowsChanged, &b.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("branch %q not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("get branch: %w", err)
	}
	if parent != nil {
		b.Parent = *parent
	}
	return b, nil
}

func (s *PgStore) ListBranches(ctx context.Context) ([]*Branch, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT name, parent, database, created_at, updated_at, ttl_seconds, pinned, delta_size, rows_changed, status
		 FROM _rift.branches ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("list branches: %w", err)
	}
	defer rows.Close()

	var branches []*Branch
	for rows.Next() {
		b := &Branch{}
		var parent *string
		if err := rows.Scan(&b.Name, &parent, &b.Database, &b.CreatedAt, &b.UpdatedAt,
			&b.TTLSeconds, &b.Pinned, &b.DeltaSize, &b.RowsChanged, &b.Status); err != nil {
			return nil, fmt.Errorf("scan branch: %w", err)
		}
		if parent != nil {
			b.Parent = *parent
		}
		branches = append(branches, b)
	}
	return branches, rows.Err()
}

func (s *PgStore) UpdateBranch(ctx context.Context, b *Branch) error {
	b.UpdatedAt = time.Now()
	_, err := s.pool.Exec(ctx,
		`UPDATE _rift.branches SET parent=$2, database=$3, updated_at=$4, ttl_seconds=$5,
		 pinned=$6, delta_size=$7, rows_changed=$8, status=$9
		 WHERE name=$1`,
		b.Name, nullIfEmpty(b.Parent), b.Database, b.UpdatedAt,
		b.TTLSeconds, b.Pinned, b.DeltaSize, b.RowsChanged, b.Status)
	if err != nil {
		return fmt.Errorf("update branch: %w", err)
	}
	return nil
}

func (s *PgStore) DeleteBranch(ctx context.Context, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM _rift.branches WHERE name = $1`, name)
	if err != nil {
		return fmt.Errorf("delete branch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("branch %q not found", name)
	}
	return nil
}

// --- Branch overlay schema ---

func (s *PgStore) CreateBranchSchema(ctx context.Context, branchName string) error {
	schema := s.BranchSchemaName(branchName)
	_, err := s.pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pgQuoteIdent(schema)))
	if err != nil {
		return fmt.Errorf("create branch schema: %w", err)
	}
	return nil
}

func (s *PgStore) DropBranchSchema(ctx context.Context, branchName string) error {
	schema := s.BranchSchemaName(branchName)
	_, err := s.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", pgQuoteIdent(schema)))
	if err != nil {
		return fmt.Errorf("drop branch schema: %w", err)
	}
	return nil
}

func (s *PgStore) BranchSchemaName(branchName string) string {
	safe := sanitizeBranchName(branchName)
	return "_rift_branch_" + safe
}

// --- Table tracking ---

func (s *PgStore) TrackTable(ctx context.Context, t *TrackedTable) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO _rift.branch_tables (branch_name, source_schema, table_name, overlay_table, has_tombstones)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (branch_name, source_schema, table_name) DO NOTHING`,
		t.BranchName, t.SourceSchema, t.TableName, t.OverlayTable, t.HasTombstones)
	if err != nil {
		return fmt.Errorf("track table: %w", err)
	}
	return nil
}

func (s *PgStore) UntrackTable(ctx context.Context, branchName, sourceSchema, tableName string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM _rift.branch_tables WHERE branch_name=$1 AND source_schema=$2 AND table_name=$3`,
		branchName, sourceSchema, tableName)
	return err
}

func (s *PgStore) ListTrackedTables(ctx context.Context, branchName string) ([]*TrackedTable, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT branch_name, source_schema, table_name, overlay_table, has_tombstones, row_count
		 FROM _rift.branch_tables WHERE branch_name = $1 ORDER BY table_name`,
		branchName)
	if err != nil {
		return nil, fmt.Errorf("list tracked tables: %w", err)
	}
	defer rows.Close()

	var tables []*TrackedTable
	for rows.Next() {
		t := &TrackedTable{}
		if err := rows.Scan(&t.BranchName, &t.SourceSchema, &t.TableName,
			&t.OverlayTable, &t.HasTombstones, &t.RowCount); err != nil {
			return nil, fmt.Errorf("scan tracked table: %w", err)
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (s *PgStore) UpdateTrackedTableRowCount(ctx context.Context, branchName, sourceSchema, tableName string, rowCount int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE _rift.branch_tables SET row_count=$4
		 WHERE branch_name=$1 AND source_schema=$2 AND table_name=$3`,
		branchName, sourceSchema, tableName, rowCount)
	return err
}

// --- Primary key cache ---

func (s *PgStore) CachePrimaryKeys(ctx context.Context, keys []PrimaryKeyColumn) error {
	for _, k := range keys {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO _rift.table_primary_keys (source_schema, table_name, column_name, ordinal)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (source_schema, table_name, column_name) DO UPDATE SET ordinal = $4`,
			k.SourceSchema, k.TableName, k.ColumnName, k.Ordinal)
		if err != nil {
			return fmt.Errorf("cache primary key: %w", err)
		}
	}
	return nil
}

func (s *PgStore) GetPrimaryKeys(ctx context.Context, sourceSchema, tableName string) ([]PrimaryKeyColumn, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT source_schema, table_name, column_name, ordinal
		 FROM _rift.table_primary_keys
		 WHERE source_schema=$1 AND table_name=$2
		 ORDER BY ordinal`,
		sourceSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("get primary keys: %w", err)
	}
	defer rows.Close()

	var keys []PrimaryKeyColumn
	for rows.Next() {
		var k PrimaryKeyColumn
		if err := rows.Scan(&k.SourceSchema, &k.TableName, &k.ColumnName, &k.Ordinal); err != nil {
			return nil, fmt.Errorf("scan primary key: %w", err)
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// --- Helpers ---

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func sanitizeBranchName(name string) string {
	replacer := strings.NewReplacer("-", "_", ".", "_", "/", "_")
	return strings.ToLower(replacer.Replace(name))
}

// pgQuoteIdent quotes a SQL identifier to prevent injection.
func pgQuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// ValidateBranchName checks if a branch name is safe for use as a schema suffix.
func ValidateBranchName(name string) error {
	if name == "" {
		return fmt.Errorf("branch name cannot be empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("branch name too long (max 63 characters)")
	}
	if !branchNameRe.MatchString(name) {
		return fmt.Errorf("branch name must contain only alphanumeric characters, hyphens, and underscores")
	}
	return nil
}
