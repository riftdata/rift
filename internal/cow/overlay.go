package cow

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// EnsureOverlayTable creates an overlay table in the branch schema that mirrors the source table,
// with an additional _rift_tombstone column.
func EnsureOverlayTable(ctx context.Context, pool *pgxpool.Pool, branchSchema, sourceSchema, tableName string) error {
	overlayTable := pgQuoteIdent(branchSchema) + "." + pgQuoteIdent(tableName)
	sourceTable := pgQuoteIdent(sourceSchema) + "." + pgQuoteIdent(tableName)

	// Check if overlay already exists
	exists, err := TableExists(ctx, pool, branchSchema, tableName)
	if err != nil {
		return fmt.Errorf("check overlay exists: %w", err)
	}
	if exists {
		return nil
	}

	// Get PK columns for the source table
	pkCols, err := GetTablePrimaryKeys(ctx, pool, sourceSchema, tableName)
	if err != nil {
		return fmt.Errorf("get source PKs: %w", err)
	}
	if len(pkCols) == 0 {
		return fmt.Errorf("table %s.%s has no primary key; overlay requires a PK", sourceSchema, tableName)
	}

	// Create an overlay table using LIKE to mirror the structure
	createSQL := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)`,
		overlayTable, sourceTable)

	if _, err := pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create overlay table: %w", err)
	}

	// Add tombstone column
	addTombstone := fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN IF NOT EXISTS _rift_tombstone BOOLEAN NOT NULL DEFAULT false`,
		overlayTable)

	if _, err := pool.Exec(ctx, addTombstone); err != nil {
		return fmt.Errorf("add tombstone column: %w", err)
	}

	// Add a primary key only if one doesn't already exist.
	// LIKE - may or may not copy PK constraints depending on a PG version.
	var hasPK bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_constraint c
			JOIN pg_catalog.pg_class r ON r.oid = c.conrelid
			JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
			WHERE n.nspname = $1 AND r.relname = $2 AND c.contype = 'p'
		)`, branchSchema, tableName).Scan(&hasPK)
	if err != nil {
		return fmt.Errorf("check overlay PK: %w", err)
	}

	if !hasPK {
		pkList := strings.Join(quoteIdents(pkCols), ", ")
		addPK := fmt.Sprintf(`ALTER TABLE %s ADD PRIMARY KEY (%s)`, overlayTable, pkList)
		if _, err := pool.Exec(ctx, addPK); err != nil {
			return fmt.Errorf("add overlay PK: %w", err)
		}
	}

	return nil
}

// DropOverlayTable drops an overlay table if it exists.
func DropOverlayTable(ctx context.Context, pool *pgxpool.Pool, branchSchema, tableName string) error {
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		pgQuoteIdent(branchSchema), pgQuoteIdent(tableName))
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("drop overlay table: %w", err)
	}
	return nil
}

// OverlayRowCount returns the count of non-tombstone rows in an overlay table.
func OverlayRowCount(ctx context.Context, pool *pgxpool.Pool, branchSchema, tableName string) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE NOT _rift_tombstone",
			pgQuoteIdent(branchSchema), pgQuoteIdent(tableName))).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count overlay rows: %w", err)
	}
	return count, nil
}

// TombstoneCount returns the count of tombstone rows in an overlay table.
func TombstoneCount(ctx context.Context, pool *pgxpool.Pool, branchSchema, tableName string) (int64, error) {
	var count int64
	err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE _rift_tombstone",
			pgQuoteIdent(branchSchema), pgQuoteIdent(tableName))).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count tombstones: %w", err)
	}
	return count, nil
}

func pgQuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func quoteIdents(idents []string) []string {
	quoted := make([]string, len(idents))
	for i, id := range idents {
		quoted[i] = pgQuoteIdent(id)
	}
	return quoted
}
