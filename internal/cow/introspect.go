package cow

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ColumnDef describes a column in a table.
type ColumnDef struct {
	Name       string
	DataType   string
	IsNullable bool
	IsPK       bool
	Ordinal    int
	Default    string
}

// IntrospectTable returns the column definitions for a table.
func IntrospectTable(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]ColumnDef, error) {
	rows, err := pool.Query(ctx,
		`SELECT c.column_name, c.data_type, c.is_nullable = 'YES', c.ordinal_position,
		        COALESCE(c.column_default, '')
		 FROM information_schema.columns c
		 WHERE c.table_schema = $1 AND c.table_name = $2
		 ORDER BY c.ordinal_position`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("introspect columns: %w", err)
	}
	defer rows.Close()

	var cols []ColumnDef
	for rows.Next() {
		var col ColumnDef
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &col.Ordinal, &col.Default); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s.%s not found or has no columns", schema, table)
	}

	// Mark PK columns
	pkCols, err := GetTablePrimaryKeys(ctx, pool, schema, table)
	if err != nil {
		return nil, err
	}
	pkSet := make(map[string]bool, len(pkCols))
	for _, pk := range pkCols {
		pkSet[pk] = true
	}
	for i := range cols {
		cols[i].IsPK = pkSet[cols[i].Name]
	}

	return cols, nil
}

// GetTablePrimaryKeys returns the primary key column names for a table.
func GetTablePrimaryKeys(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]string, error) {
	rows, err := pool.Query(ctx,
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu
		   ON tc.constraint_name = kcu.constraint_name
		   AND tc.table_schema = kcu.table_schema
		 WHERE tc.constraint_type = 'PRIMARY KEY'
		   AND tc.table_schema = $1
		   AND tc.table_name = $2
		 ORDER BY kcu.ordinal_position`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("get primary keys: %w", err)
	}
	defer rows.Close()

	var pkCols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pk column: %w", err)
		}
		pkCols = append(pkCols, col)
	}
	return pkCols, rows.Err()
}

// TableExists checks if a table exists in the given schema.
func TableExists(ctx context.Context, pool *pgxpool.Pool, schema, table string) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`, schema, table).Scan(&exists)
	return exists, err
}
