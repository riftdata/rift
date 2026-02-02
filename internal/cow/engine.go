package cow

import (
	"context"
	"fmt"
	"time"

	"github.com/riftdata/rift/internal/parser"
	"github.com/riftdata/rift/internal/storage"
)

// Engine is the copy-on-write query processing engine. It coordinates SQL parsing,
// overlay table management, and query rewriting for branch isolation.
type Engine struct {
	store storage.Store
}

// NewEngine creates a new CoW engine.
func NewEngine(store storage.Store) *Engine {
	return &Engine{store: store}
}

// ProcessedQuery holds the result of processing a SQL query through the engine.
type ProcessedQuery struct {
	OriginalSQL   string
	RewrittenSQL  string
	Type          parser.QueryType
	NeedsOverlay  bool
	IsPassthrough bool
	TableName     string
}

// ProcessQuery parses and rewrites a SQL query for the given branch.
// For the "main" branch, queries pass through unmodified.
func (e *Engine) ProcessQuery(ctx context.Context, branchName, sql string) (*ProcessedQuery, error) {
	// Main branch is always passthrough
	if branchName == "main" {
		return &ProcessedQuery{
			OriginalSQL:   sql,
			RewrittenSQL:  sql,
			IsPassthrough: true,
		}, nil
	}

	// Transaction control passes through
	if parser.IsTransactionControl(sql) {
		return &ProcessedQuery{
			OriginalSQL:   sql,
			RewrittenSQL:  sql,
			Type:          parser.QueryUtility,
			IsPassthrough: true,
		}, nil
	}

	// Parse the SQL
	pq, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	// Utility statements pass through
	if pq.IsUtility() {
		return &ProcessedQuery{
			OriginalSQL:   sql,
			RewrittenSQL:  sql,
			Type:          pq.Type,
			IsPassthrough: true,
		}, nil
	}

	// Build rewrite configs for referenced tables
	configs, err := e.buildRewriteConfigs(ctx, branchName, pq)
	if err != nil {
		return nil, fmt.Errorf("build rewrite configs: %w", err)
	}

	// For write operations, ensure overlay tables exist
	if pq.IsWrite() || pq.IsDDL() {
		if err := e.ensureOverlays(ctx, branchName, pq); err != nil {
			return nil, fmt.Errorf("ensure overlays: %w", err)
		}
		// Rebuild configs after overlay creation (PKs may have been cached)
		configs, err = e.buildRewriteConfigs(ctx, branchName, pq)
		if err != nil {
			return nil, fmt.Errorf("rebuild rewrite configs: %w", err)
		}
	}

	// Rewrite the query
	result, err := parser.RewriteForBranch(pq, configs)
	if err != nil {
		return nil, fmt.Errorf("rewrite query: %w", err)
	}

	return &ProcessedQuery{
		OriginalSQL:   sql,
		RewrittenSQL:  result.SQL,
		Type:          pq.Type,
		NeedsOverlay:  result.NeedsOverlay,
		IsPassthrough: result.IsPassthrough,
		TableName:     result.TableName,
	}, nil
}

// CreateBranch creates a new branch with overlay schema.
func (e *Engine) CreateBranch(ctx context.Context, name, parent string, ttl *time.Duration) error {
	if err := storage.ValidateBranchName(name); err != nil {
		return err
	}

	// Get parent info
	parentBranch, err := e.store.GetBranch(ctx, parent)
	if err != nil {
		return fmt.Errorf("parent branch %q not found: %w", parent, err)
	}

	now := time.Now()
	b := &storage.Branch{
		Name:      name,
		Parent:    parent,
		Database:  parentBranch.Database,
		CreatedAt: now,
		UpdatedAt: now,
		Status:    "active",
	}

	if ttl != nil {
		secs := int(ttl.Seconds())
		b.TTLSeconds = &secs
	}

	if err := e.store.CreateBranch(ctx, b); err != nil {
		return fmt.Errorf("create branch metadata: %w", err)
	}

	if err := e.store.CreateBranchSchema(ctx, name); err != nil {
		_ = e.store.DeleteBranch(ctx, name)
		return fmt.Errorf("create branch schema: %w", err)
	}

	return nil
}

// DeleteBranch deletes a branch and its overlay schema.
// It verifies the branch exists, is not pinned, and has no children before proceeding.
func (e *Engine) DeleteBranch(ctx context.Context, name string) error {
	branch, err := e.store.GetBranch(ctx, name)
	if err != nil {
		return fmt.Errorf("get branch: %w", err)
	}

	if branch.Pinned {
		return fmt.Errorf("cannot delete pinned branch %q", name)
	}

	// Check for child branches that depend on this one.
	branches, err := e.store.ListBranches(ctx)
	if err != nil {
		return fmt.Errorf("list branches: %w", err)
	}
	for _, b := range branches {
		if b.Parent == name {
			return fmt.Errorf("cannot delete branch %q: has child branch %q", name, b.Name)
		}
	}

	if err := e.store.DropBranchSchema(ctx, name); err != nil {
		return fmt.Errorf("drop branch schema: %w", err)
	}
	return e.store.DeleteBranch(ctx, name)
}

// Diff computes changes between a branch and its parent.
func (e *Engine) Diff(ctx context.Context, branchName string) (*BranchDiff, error) {
	branch, err := e.store.GetBranch(ctx, branchName)
	if err != nil {
		return nil, fmt.Errorf("get branch: %w", err)
	}

	tables, err := e.store.ListTrackedTables(ctx, branchName)
	if err != nil {
		return nil, fmt.Errorf("list tracked tables: %w", err)
	}

	pool := e.store.Pool()
	branchSchema := e.store.BranchSchemaName(branchName)

	diff := &BranchDiff{
		BranchName: branchName,
		Parent:     branch.Parent,
	}

	for _, t := range tables {
		pks, err := e.store.GetPrimaryKeys(ctx, t.SourceSchema, t.TableName)
		if err != nil {
			return nil, fmt.Errorf("get PKs for %s: %w", t.TableName, err)
		}

		pkCols := make([]string, len(pks))
		for i, pk := range pks {
			pkCols[i] = pk.ColumnName
		}

		td, err := DiffTable(ctx, pool, branchSchema, t.SourceSchema, t.TableName, pkCols)
		if err != nil {
			return nil, fmt.Errorf("diff table %s: %w", t.TableName, err)
		}

		diff.Tables = append(diff.Tables, *td)
	}

	return diff, nil
}

// GenerateMerge produces SQL to apply branch changes to the parent.
func (e *Engine) GenerateMerge(ctx context.Context, branchName string) ([]MergeSQL, error) {
	tables, err := e.store.ListTrackedTables(ctx, branchName)
	if err != nil {
		return nil, fmt.Errorf("list tracked tables: %w", err)
	}

	pool := e.store.Pool()
	branchSchema := e.store.BranchSchemaName(branchName)

	var merges []MergeSQL
	for _, t := range tables {
		pks, err := e.store.GetPrimaryKeys(ctx, t.SourceSchema, t.TableName)
		if err != nil {
			return nil, fmt.Errorf("get PKs for %s: %w", t.TableName, err)
		}

		pkCols := make([]string, len(pks))
		for i, pk := range pks {
			pkCols[i] = pk.ColumnName
		}

		m, err := GenerateMergeSQL(ctx, pool, branchSchema, t.SourceSchema, t.TableName, pkCols)
		if err != nil {
			return nil, fmt.Errorf("generate merge for %s: %w", t.TableName, err)
		}

		merges = append(merges, *m)
	}

	return merges, nil
}

// buildRewriteConfigs creates parser.RewriteConfig for each table referenced in the query.
func (e *Engine) buildRewriteConfigs(ctx context.Context, branchName string, pq *parser.ParsedQuery) (map[string]parser.RewriteConfig, error) {
	configs := make(map[string]parser.RewriteConfig)
	branchSchema := e.store.BranchSchemaName(branchName)
	pool := e.store.Pool()

	for _, tbl := range pq.Tables {
		schema := tbl.Schema
		if schema == "" {
			schema = "public"
		}

		// Check if overlay exists for this table
		exists, err := TableExists(ctx, pool, branchSchema, tbl.Name)
		if err != nil {
			return nil, err
		}

		if !exists && pq.IsReadOnly() {
			// For reads, if no overlay exists, the table hasn't been modified in this branch.
			// Still create a config so reads see the source data correctly,
			// but only if we know the table has tracked changes.
			trackedTables, err := e.store.ListTrackedTables(ctx, branchName)
			if err != nil {
				return nil, err
			}
			tracked := false
			for _, tt := range trackedTables {
				if tt.TableName == tbl.Name && tt.SourceSchema == schema {
					tracked = true
					break
				}
			}
			if !tracked {
				continue // Not tracked, query goes direct to source
			}
		}

		// Get primary keys
		pkCols, err := e.getPKColumns(ctx, schema, tbl.Name)
		if err != nil {
			return nil, fmt.Errorf("get PKs for %s: %w", tbl.Name, err)
		}

		configs[tbl.Name] = parser.RewriteConfig{
			BranchSchema: branchSchema,
			SourceSchema: schema,
			PKColumns:    pkCols,
		}
	}

	return configs, nil
}

// ensureOverlays creates overlay tables for any tables that don't have them yet.
func (e *Engine) ensureOverlays(ctx context.Context, branchName string, pq *parser.ParsedQuery) error {
	pool := e.store.Pool()
	branchSchema := e.store.BranchSchemaName(branchName)

	for _, tbl := range pq.Tables {
		schema := tbl.Schema
		if schema == "" {
			schema = "public"
		}

		// Skip if it's a rift internal table
		if schema == "_rift" {
			continue
		}

		// Check if source table exists
		srcExists, err := TableExists(ctx, pool, schema, tbl.Name)
		if err != nil {
			return err
		}
		if !srcExists {
			// For DDL CREATE TABLE, skip — the table doesn't exist yet in source
			if pq.IsDDL() {
				continue
			}
			return fmt.Errorf("source table %s.%s does not exist", schema, tbl.Name)
		}

		// Create overlay table
		if err := EnsureOverlayTable(ctx, pool, branchSchema, schema, tbl.Name); err != nil {
			return fmt.Errorf("ensure overlay for %s: %w", tbl.Name, err)
		}

		// Cache PKs
		pkCols, err := GetTablePrimaryKeys(ctx, pool, schema, tbl.Name)
		if err != nil {
			return fmt.Errorf("get PKs for %s: %w", tbl.Name, err)
		}

		var pkEntries []storage.PrimaryKeyColumn
		for i, col := range pkCols {
			pkEntries = append(pkEntries, storage.PrimaryKeyColumn{
				SourceSchema: schema,
				TableName:    tbl.Name,
				ColumnName:   col,
				Ordinal:      i + 1,
			})
		}
		if err := e.store.CachePrimaryKeys(ctx, pkEntries); err != nil {
			return fmt.Errorf("cache PKs for %s: %w", tbl.Name, err)
		}

		// Track the table
		tracked := &storage.TrackedTable{
			BranchName:    branchName,
			SourceSchema:  schema,
			TableName:     tbl.Name,
			OverlayTable:  tbl.Name,
			HasTombstones: false,
		}
		if err := e.store.TrackTable(ctx, tracked); err != nil {
			return fmt.Errorf("track table %s: %w", tbl.Name, err)
		}
	}

	return nil
}

// getPKColumns returns PK column names, using cache first.
func (e *Engine) getPKColumns(ctx context.Context, schema, table string) ([]string, error) {
	// Try cache first
	cached, err := e.store.GetPrimaryKeys(ctx, schema, table)
	if err == nil && len(cached) > 0 {
		cols := make([]string, len(cached))
		for i, pk := range cached {
			cols[i] = pk.ColumnName
		}
		return cols, nil
	}

	// Fall back to information_schema
	return GetTablePrimaryKeys(ctx, e.store.Pool(), schema, table)
}
