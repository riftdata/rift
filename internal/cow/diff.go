package cow

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TableDiff summarizes changes for a single table in a branch.
type TableDiff struct {
	TableName    string
	SourceSchema string
	Inserts      int64
	Updates      int64
	Deletes      int64
}

// BranchDiff holds the diff for an entire branch.
type BranchDiff struct {
	BranchName string
	Parent     string
	Tables     []TableDiff
}

// TotalChanges returns the sum of all changes across all tables.
func (d *BranchDiff) TotalChanges() int64 {
	var total int64
	for _, t := range d.Tables {
		total += t.Inserts + t.Updates + t.Deletes
	}
	return total
}

// DiffTable computes the changes between a branch overlay table and its source.
// It compares rows in the overlay against the source table using PKs:
// - Rows in overlay with tombstone=true → deletes
// - Rows in overlay without tombstone that also exist in source → updates
// - Rows in overlay without tombstone that don't exist in source → inserts
func DiffTable(ctx context.Context, pool *pgxpool.Pool, branchSchema, sourceSchema, tableName string, pkCols []string) (*TableDiff, error) {
	ovrTable := pgQuoteIdent(branchSchema) + "." + pgQuoteIdent(tableName)
	srcTable := pgQuoteIdent(sourceSchema) + "." + pgQuoteIdent(tableName)

	diff := &TableDiff{
		TableName:    tableName,
		SourceSchema: sourceSchema,
	}

	// Count deletes (tombstones)
	err := pool.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE _rift_tombstone", ovrTable)).Scan(&diff.Deletes)
	if err != nil {
		return nil, fmt.Errorf("count deletes: %w", err)
	}

	// Count inserts (non-tombstone rows in overlay that don't exist in source)
	pkJoin := buildPKJoin("ovr", "src", pkCols)
	err = pool.QueryRow(ctx,
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %s ovr
			 WHERE NOT ovr._rift_tombstone
			 AND NOT EXISTS (SELECT 1 FROM %s src WHERE %s)`,
			ovrTable, srcTable, pkJoin)).Scan(&diff.Inserts)
	if err != nil {
		return nil, fmt.Errorf("count inserts: %w", err)
	}

	// Count updates (non-tombstone rows in overlay that also exist in source)
	err = pool.QueryRow(ctx,
		fmt.Sprintf(
			`SELECT COUNT(*) FROM %s ovr
			 WHERE NOT ovr._rift_tombstone
			 AND EXISTS (SELECT 1 FROM %s src WHERE %s)`,
			ovrTable, srcTable, pkJoin)).Scan(&diff.Updates)
	if err != nil {
		return nil, fmt.Errorf("count updates: %w", err)
	}

	return diff, nil
}

func buildPKJoin(leftAlias, rightAlias string, pkCols []string) string {
	result := ""
	for i, col := range pkCols {
		if i > 0 {
			result += " AND "
		}
		qcol := pgQuoteIdent(col)
		result += fmt.Sprintf("%s.%s = %s.%s", leftAlias, qcol, rightAlias, qcol)
	}
	return result
}
