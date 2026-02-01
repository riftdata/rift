package storage

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// runMigrations applies unapplied SQL migrations in order.
func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("reading migrations dir: %w", err)
	}

	// Sort migration files by name (numeric prefix ensures order)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, err := parseMigrationVersion(entry.Name())
		if err != nil {
			return fmt.Errorf("parsing migration filename %s: %w", entry.Name(), err)
		}

		applied, err := isMigrationApplied(ctx, pool, version)
		if err != nil {
			return fmt.Errorf("checking migration %d: %w", version, err)
		}
		if applied {
			continue
		}

		content, err := migrationFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", entry.Name(), err)
		}

		if err := applyMigration(ctx, pool, version, entry.Name(), string(content)); err != nil {
			return fmt.Errorf("applying migration %s: %w", entry.Name(), err)
		}
	}

	return nil
}

// parseMigrationVersion extracts the version number from a filename like "001_init.sql".
func parseMigrationVersion(filename string) (int, error) {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid migration filename: %s", filename)
	}
	return strconv.Atoi(parts[0])
}

// isMigrationApplied checks if a migration version has already been applied.
// Returns false if the schema_version table doesn't exist yet (first run).
func isMigrationApplied(ctx context.Context, pool *pgxpool.Pool, version int) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = '_rift' AND table_name = 'schema_version'
		)`).Scan(&exists)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	var applied bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM _rift.schema_version WHERE version = $1)`,
		version).Scan(&applied)
	return applied, err
}

// applyMigration executes a migration SQL and records it in schema_version.
func applyMigration(ctx context.Context, pool *pgxpool.Pool, version int, filename, sql string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("exec migration: %w", err)
	}

	// Record the migration (table is created by the first migration itself)
	_, err = tx.Exec(ctx,
		`INSERT INTO _rift.schema_version (version, description) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		version, filename)
	if err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	return tx.Commit(ctx)
}
