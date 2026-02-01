-- Rift internal schema for branch metadata and overlay tracking.
-- This schema is created inside the upstream database and never
-- collides with user data.

CREATE SCHEMA IF NOT EXISTS _rift;

-- Schema version tracking (simple migration bookkeeping)
CREATE TABLE IF NOT EXISTS _rift.schema_version
(
    version
    INTEGER
    PRIMARY
    KEY,
    applied_at
    TIMESTAMPTZ
    NOT
    NULL
    DEFAULT
    now
(
),
    description TEXT
    );

-- Branch metadata (replaces branches.json file storage)
CREATE TABLE IF NOT EXISTS _rift.branches
(
    name
    TEXT
    PRIMARY
    KEY,
    parent
    TEXT
    REFERENCES
    _rift
    .
    branches
(
    name
) ON DELETE SET NULL,
    database TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    ttl_seconds INTEGER,
    pinned BOOLEAN NOT NULL DEFAULT false,
    delta_size BIGINT NOT NULL DEFAULT 0,
    rows_changed BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active'
    );

-- Track which user tables have overlay copies in each branch schema
CREATE TABLE IF NOT EXISTS _rift.branch_tables
(
    branch_name
    TEXT
    NOT
    NULL
    REFERENCES
    _rift
    .
    branches
(
    name
) ON DELETE CASCADE,
    source_schema TEXT NOT NULL DEFAULT 'public',
    table_name TEXT NOT NULL,
    overlay_table TEXT NOT NULL,
    has_tombstones BOOLEAN NOT NULL DEFAULT false,
    row_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now
(
),
    PRIMARY KEY
(
    branch_name,
    source_schema,
    table_name
)
    );

-- Cache discovered primary keys for user tables (avoids repeated information_schema queries)
CREATE TABLE IF NOT EXISTS _rift.table_primary_keys
(
    source_schema
    TEXT
    NOT
    NULL
    DEFAULT
    'public',
    table_name
    TEXT
    NOT
    NULL,
    column_name
    TEXT
    NOT
    NULL,
    ordinal
    INTEGER
    NOT
    NULL,
    PRIMARY
    KEY
(
    source_schema,
    table_name,
    column_name
)
    );

-- Seed the main branch
INSERT INTO _rift.branches (name, database, pinned)
VALUES ('main', '', true) ON CONFLICT (name) DO NOTHING;