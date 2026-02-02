package cow

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MergeSQL holds the generated SQL statements to merge a branch into its parent.
type MergeSQL struct {
	Statements []string
	TableName  string
}

// GenerateMergeSQL produces SQL to apply a branch's changes to the parent.
// The generated SQL handles inserts, updates, and deletes in the correct order.
func GenerateMergeSQL(ctx context.Context, pool *pgxpool.Pool, branchSchema, sourceSchema, tableName string, pkCols []string) (*MergeSQL, error) {
	if len(pkCols) == 0 {
		return nil, fmt.Errorf("merge table %q: empty primary key columns", tableName)
	}

	ovrTable := pgQuoteIdent(branchSchema) + "." + pgQuoteIdent(tableName)
	srcTable := pgQuoteIdent(sourceSchema) + "." + pgQuoteIdent(tableName)

	// Get all column names from the source table
	cols, err := IntrospectTable(ctx, pool, sourceSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("introspect table for merge: %w", err)
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	pkJoin := buildPKJoin("ovr", "src", pkCols)
	quotedPKs := quoteIdents(pkCols)
	quotedCols := quoteIdents(colNames)

	var stmts []string

	// Step 1: Delete rows marked as tombstones from source
	deleteSQL := fmt.Sprintf(
		"DELETE FROM %s src WHERE EXISTS (SELECT 1 FROM %s ovr WHERE %s AND ovr._rift_tombstone)",
		srcTable, ovrTable, pkJoin)
	stmts = append(stmts, deleteSQL)

	// Step 2: Update existing rows (non-tombstone overlay rows that exist in source)
	var setClauses []string
	for _, col := range quotedCols {
		setClauses = append(setClauses, fmt.Sprintf("%s = ovr.%s", col, col))
	}
	updateSQL := fmt.Sprintf(
		"UPDATE %s src SET %s FROM %s ovr WHERE %s AND NOT ovr._rift_tombstone",
		srcTable, strings.Join(setClauses, ", "), ovrTable, pkJoin)
	stmts = append(stmts, updateSQL)

	// Step 3: Insert new rows (non-tombstone overlay rows that don't exist in source)
	colList := strings.Join(quotedCols, ", ")
	ovrColList := make([]string, len(quotedCols))
	for i, col := range quotedCols {
		ovrColList[i] = "ovr." + col
	}

	pkJoinForInsert := buildPKJoin("src", "ovr", pkCols)
	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s ovr WHERE NOT ovr._rift_tombstone AND NOT EXISTS (SELECT 1 FROM %s src WHERE %s)",
		srcTable, colList, strings.Join(ovrColList, ", "),
		ovrTable, srcTable, pkJoinForInsert)
	stmts = append(stmts, insertSQL)

	// Wrap in a transaction
	txStmts := []string{"BEGIN"}
	txStmts = append(txStmts, stmts...)
	txStmts = append(txStmts, "COMMIT")

	_ = quotedPKs // used in pkJoin via buildPKJoin

	return &MergeSQL{
		Statements: txStmts,
		TableName:  tableName,
	}, nil
}

// FormatMergeSQL returns the merge SQL as a single string.
func FormatMergeSQL(m *MergeSQL) string {
	return strings.Join(m.Statements, ";\n") + ";"
}
