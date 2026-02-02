package parser

import (
	"fmt"
	"strings"
)

// RewriteConfig provides the information needed to rewrite a query for a branch.
type RewriteConfig struct {
	BranchSchema string   // e.g. "_rift_branch_dev"
	SourceSchema string   // e.g. "public"
	PKColumns    []string // primary key columns of the target table
}

// RewriteResult holds the rewritten SQL and metadata.
type RewriteResult struct {
	SQL           string
	IsPassthrough bool
	NeedsOverlay  bool
	TableName     string
}

// RewriteForBranch rewrites a parsed query for execution against a branch overlay.
// Main branch queries are passthrough — no rewriting needed.
func RewriteForBranch(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if pq == nil {
		return &RewriteResult{IsPassthrough: true}, nil
	}

	switch pq.Type {
	case QuerySelect:
		return rewriteSelect(pq, configs)
	case QueryInsert:
		return rewriteInsert(pq, configs)
	case QueryUpdate:
		return rewriteUpdate(pq, configs)
	case QueryDelete:
		return rewriteDelete(pq, configs)
	case QueryDDL:
		return rewriteDDL(pq, configs)
	default:
		// Utility statements pass through
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}
}

// rewriteSelect creates a CTE that merges overlay + source, filtering tombstones.
//
// For: SELECT * FROM users WHERE id = 1
// Produces:
//
//	WITH _rift_merged_users AS (
//	  SELECT * FROM _rift_branch_dev.users WHERE NOT _rift_tombstone
//	  UNION ALL
//	  SELECT src.* FROM public.users src
//	  WHERE NOT EXISTS (
//	    SELECT 1 FROM _rift_branch_dev.users ovr WHERE ovr.id = src.id
//	  )
//	)
//	SELECT * FROM _rift_merged_users WHERE id = 1
func rewriteSelect(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if len(pq.Tables) == 0 {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	sql := pq.Original
	var ctes []string
	hasOverlay := false

	for _, tbl := range pq.Tables {
		cfg, ok := configs[tbl.Name]
		if !ok {
			continue
		}
		if len(cfg.PKColumns) == 0 {
			return nil, fmt.Errorf("table %q requires a primary key for overlay semantics", tbl.Name)
		}
		hasOverlay = true

		mergedName := "_rift_merged_" + tbl.Name
		srcTable := qualifiedTable(cfg.SourceSchema, tbl.Name)
		ovrTable := qualifiedTable(cfg.BranchSchema, tbl.Name)

		pkJoin := buildPKJoin("ovr", "src", cfg.PKColumns)

		cte := fmt.Sprintf(
			`%s AS (
  SELECT * FROM %s WHERE NOT _rift_tombstone
  UNION ALL
  SELECT src.* FROM %s src
  WHERE NOT EXISTS (
    SELECT 1 FROM %s ovr WHERE %s
  )
)`,
			pgQuoteIdent(mergedName),
			ovrTable,
			srcTable,
			ovrTable,
			pkJoin,
		)
		ctes = append(ctes, cte)

		// Replace table references in the original query
		sql = replaceTableRef(sql, tbl, mergedName)
	}

	if !hasOverlay {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	result := "WITH " + strings.Join(ctes, ", ") + "\n" + sql
	return &RewriteResult{
		SQL:          result,
		NeedsOverlay: true,
		TableName:    pq.Tables[0].Name,
	}, nil
}

// rewriteInsert redirects the INSERT to the overlay table using ON CONFLICT upsert.
//
// For: INSERT INTO users (name) VALUES ('Charlie')
// Produces: INSERT INTO _rift_branch_dev.users (name, _rift_tombstone) VALUES ('Charlie', false)
//
//	ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, _rift_tombstone = false
func rewriteInsert(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if len(pq.Tables) == 0 {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	tbl := pq.Tables[0]
	cfg, ok := configs[tbl.Name]
	if !ok {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	ovrTable := qualifiedTable(cfg.BranchSchema, tbl.Name)
	srcRef := qualifiedTable(cfg.SourceSchema, tbl.Name)
	if tbl.Schema != "" {
		srcRef = qualifiedTable(tbl.Schema, tbl.Name)
	}

	// Replace the target table with overlay table
	sql := strings.Replace(pq.Original, srcRef, ovrTable, 1)
	if tbl.Schema == "" {
		sql = replaceTableRef(sql, tbl, cfg.BranchSchema+"."+tbl.Name)
	}

	// Add _rift_tombstone = false and ON CONFLICT upsert
	// For simplicity, we add the ON CONFLICT clause
	if len(cfg.PKColumns) > 0 {
		pkList := strings.Join(quoteIdents(cfg.PKColumns), ", ")

		// Build SET clause for upsert
		var setClauses []string
		for _, col := range pq.TargetColumns {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s",
				pgQuoteIdent(col), pgQuoteIdent(col)))
		}
		setClauses = append(setClauses, "_rift_tombstone = false")

		// Remove trailing semicolon before appending ON CONFLICT
		sql = strings.TrimRight(strings.TrimSpace(sql), ";")
		sql += fmt.Sprintf("\nON CONFLICT (%s) DO UPDATE SET %s",
			pkList, strings.Join(setClauses, ", "))
	}

	return &RewriteResult{
		SQL:          sql,
		NeedsOverlay: true,
		TableName:    tbl.Name,
	}, nil
}

// rewriteUpdate copies affected rows to overlay then updates them.
// This is a two-step operation:
//  1. Copy rows that match the WHERE clause from source to overlay (if not already there)
//  2. Execute the UPDATE against the overlay table
func rewriteUpdate(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if len(pq.Tables) == 0 {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	tbl := pq.Tables[0]
	cfg, ok := configs[tbl.Name]
	if !ok {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}
	if len(cfg.PKColumns) == 0 {
		return nil, fmt.Errorf("table %q requires a primary key for overlay semantics", tbl.Name)
	}

	ovrTable := qualifiedTable(cfg.BranchSchema, tbl.Name)
	srcTable := qualifiedTable(cfg.SourceSchema, tbl.Name)
	pkJoin := buildPKJoin("ovr", "src", cfg.PKColumns)

	// Step 1: Copy-on-write — insert matching rows from source that aren't already in overlay
	copySQL := fmt.Sprintf(
		`INSERT INTO %s SELECT src.*, false AS _rift_tombstone FROM %s src WHERE NOT EXISTS (SELECT 1 FROM %s ovr WHERE %s)`,
		ovrTable, srcTable, ovrTable, pkJoin)

	// Extract WHERE clause from original for the copy step.
	// Strip any table name, schema.table, or alias qualifiers so columns
	// resolve against the "src" alias used in the copy subquery.
	whereClause := extractWhereClause(pq.Original)
	qualifiers := []string{tbl.Name, tbl.Alias, tbl.QualifiedName()}
	if whereClause != "" {
		copySQL += " AND (" + requalifyWhereForAlias(whereClause, "src", qualifiers...) + ")"
	}

	// Step 2: Execute UPDATE on overlay (no alias, so strip qualifiers)
	updateSQL := replaceTableRef(pq.Original, tbl, cfg.BranchSchema+"."+tbl.Name)

	// Combine into a single DO block
	sql := copySQL + ";\n" + updateSQL

	return &RewriteResult{
		SQL:          sql,
		NeedsOverlay: true,
		TableName:    tbl.Name,
	}, nil
}

// rewriteDelete inserts a tombstone row in the overlay instead of actually deleting.
// Steps:
//  1. Copy-on-write matching rows into overlay (if not there)
//  2. Mark them as tombstones
func rewriteDelete(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if len(pq.Tables) == 0 {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	tbl := pq.Tables[0]
	cfg, ok := configs[tbl.Name]
	if !ok {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}
	if len(cfg.PKColumns) == 0 {
		return nil, fmt.Errorf("table %q requires a primary key for overlay semantics", tbl.Name)
	}

	ovrTable := qualifiedTable(cfg.BranchSchema, tbl.Name)
	srcTable := qualifiedTable(cfg.SourceSchema, tbl.Name)
	pkJoin := buildPKJoin("ovr", "src", cfg.PKColumns)

	// Step 1: Ensure rows exist in overlay
	copySQL := fmt.Sprintf(
		`INSERT INTO %s SELECT src.*, false AS _rift_tombstone FROM %s src WHERE NOT EXISTS (SELECT 1 FROM %s ovr WHERE %s)`,
		ovrTable, srcTable, ovrTable, pkJoin)

	whereClause := extractWhereClause(pq.Original)
	qualifiers := []string{tbl.Name, tbl.Alias, tbl.QualifiedName()}
	if whereClause != "" {
		copySQL += " AND (" + requalifyWhereForAlias(whereClause, "src", qualifiers...) + ")"
	}

	// Step 2: Set tombstone flag instead of deleting.
	// The UPDATE targets the overlay table directly (no alias), so strip qualifiers.
	tombstoneSQL := fmt.Sprintf("UPDATE %s SET _rift_tombstone = true", ovrTable)
	if whereClause != "" {
		tombstoneSQL += " WHERE " + stripTableQualifiers(whereClause, qualifiers...)
	}

	sql := copySQL + ";\n" + tombstoneSQL

	return &RewriteResult{
		SQL:          sql,
		NeedsOverlay: true,
		TableName:    tbl.Name,
	}, nil
}

// rewriteDDL redirects DDL to the branch overlay schema.
func rewriteDDL(pq *ParsedQuery, configs map[string]RewriteConfig) (*RewriteResult, error) {
	if len(pq.Tables) == 0 {
		return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
	}

	tbl := pq.Tables[0]
	cfg, ok := configs[tbl.Name]
	if !ok {
		// For new tables (CREATE TABLE), use any config's branch schema
		for _, c := range configs {
			cfg = c
			break
		}
		if cfg.BranchSchema == "" {
			return &RewriteResult{SQL: pq.Original, IsPassthrough: true}, nil
		}
	}

	sql := replaceTableRef(pq.Original, tbl, cfg.BranchSchema+"."+tbl.Name)

	return &RewriteResult{
		SQL:          sql,
		NeedsOverlay: true,
		TableName:    tbl.Name,
	}, nil
}

// --- Helpers ---

func qualifiedTable(schema, table string) string {
	return pgQuoteIdent(schema) + "." + pgQuoteIdent(table)
}

func buildPKJoin(leftAlias, rightAlias string, pkColumns []string) string {
	var clauses []string
	for _, col := range pkColumns {
		clauses = append(clauses, fmt.Sprintf("%s.%s = %s.%s",
			leftAlias, pgQuoteIdent(col), rightAlias, pgQuoteIdent(col)))
	}
	return strings.Join(clauses, " AND ")
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

// replaceTableRef replaces a table reference in SQL with a new name.
// This is a simple string replacement that handles common patterns.
func replaceTableRef(sql string, tbl TableRef, newRef string) string {
	// Try schema-qualified first
	if tbl.Schema != "" {
		old := tbl.Schema + "." + tbl.Name
		return strings.Replace(sql, old, newRef, 1)
	}

	// Replace standalone table name, being careful not to replace substrings.
	// This is a basic implementation; the full version will use the AST.
	return replaceWord(sql, tbl.Name, newRef)
}

// replaceWord replaces a whole word in SQL text.
func replaceWord(sql, old, newWord string) string {
	result := sql
	idx := 0
	for {
		pos := strings.Index(result[idx:], old)
		if pos == -1 {
			break
		}
		absPos := idx + pos
		endPos := absPos + len(old)

		// Check word boundaries
		before := true
		after := true
		if absPos > 0 {
			c := result[absPos-1]
			before = !isIdentChar(c)
		}
		if endPos < len(result) {
			c := result[endPos]
			after = !isIdentChar(c)
		}

		if before && after {
			result = result[:absPos] + newWord + result[endPos:]
			idx = absPos + len(newWord)
		} else {
			idx = endPos
		}
	}
	return result
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// extractWhereClause extracts the WHERE clause from a SQL string.
// Returns the clause without the "WHERE" keyword.
func extractWhereClause(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, " WHERE ")
	if idx == -1 {
		return ""
	}
	clause := sql[idx+7:]
	// Trim trailing clauses
	for _, kw := range []string{" ORDER BY ", " LIMIT ", " OFFSET ", " GROUP BY ", " HAVING ", " RETURNING "} {
		if pos := strings.Index(strings.ToUpper(clause), kw); pos != -1 {
			clause = clause[:pos]
		}
	}
	return strings.TrimRight(strings.TrimSpace(clause), ";")
}

// requalifyWhereForAlias strips known table qualifiers from column references
// in a WHERE clause and re-prefixes them with the given alias. For example,
// given table "users" (alias "u"), "u.id = 1 AND users.name = 'x'" becomes
// "src.id = 1 AND src.name = 'x'" when alias is "src".
//
// qualifiers collects the table name, schema.table, and alias that should be
// stripped. This is a best-effort, token-level transformation suitable for
// simple WHERE clauses.
func requalifyWhereForAlias(where, alias string, qualifiers ...string) string {
	result := where
	for _, q := range qualifiers {
		if q == "" {
			continue
		}
		// Replace "qualifier." with "alias." — use case-insensitive matching
		// by trying the original case and lowercase variant.
		for _, variant := range []string{q, strings.ToLower(q)} {
			result = strings.ReplaceAll(result, variant+".", alias+".")
		}
	}
	return result
}

// stripTableQualifiers removes known table qualifiers from column references,
// leaving bare column names. Used for clauses targeting a table without an alias
// (e.g., UPDATE overlay SET ... WHERE <clause>).
func stripTableQualifiers(where string, qualifiers ...string) string {
	result := where
	for _, q := range qualifiers {
		if q == "" {
			continue
		}
		for _, variant := range []string{q, strings.ToLower(q)} {
			result = strings.ReplaceAll(result, variant+".", "")
		}
	}
	return result
}
