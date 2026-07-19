package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE fusekit_schema (
    component TEXT PRIMARY KEY,
    digest TEXT NOT NULL
);

CREATE TABLE convergence_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 3),
    payload BLOB NOT NULL
);

CREATE TABLE convergence_source (
	source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    head INTEGER NOT NULL CHECK (head >= 0)
);

CREATE TABLE convergence_changes (
    change_id BLOB PRIMARY KEY CHECK (length(change_id) = 16),
    source_operation_id BLOB NOT NULL UNIQUE CHECK (length(source_operation_id) = 16),
	source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'migration', 'on_demand')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    affected_keys_json BLOB NOT NULL,
    target_tenants_json BLOB NOT NULL,
    CHECK ((cause IN ('provider_mutation', 'on_demand') AND length(origin_domain) > 0 AND origin_generation > 0)
        OR (cause NOT IN ('provider_mutation', 'on_demand') AND origin_domain = '' AND origin_generation = 0))
	, UNIQUE (source_authority, source_revision)
);

CREATE TABLE convergence_outbox (
    catalog_operation_id BLOB PRIMARY KEY CHECK (length(catalog_operation_id) = 16),
    change_id BLOB NOT NULL REFERENCES convergence_changes(change_id),
    tenant TEXT NOT NULL,
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 3),
    UNIQUE (change_id, tenant),
    UNIQUE (tenant, catalog_revision)
);
CREATE INDEX convergence_outbox_unsettled
    ON convergence_outbox(state, change_id) WHERE state <> 3;

CREATE TABLE IF NOT EXISTS tenants (
    tenant TEXT PRIMARY KEY,
    root_id BLOB NOT NULL UNIQUE CHECK (length(root_id) = 16),
    case_policy INTEGER NOT NULL CHECK (case_policy IN (1, 2)),
	 presentation_set INTEGER NOT NULL CHECK (presentation_set BETWEEN 1 AND 3),
    head INTEGER NOT NULL CHECK (head > 0),
    floor INTEGER NOT NULL CHECK (floor >= 0 AND floor <= head)
);

CREATE TABLE desired_tenants (
    tenant TEXT PRIMARY KEY REFERENCES tenants(tenant),
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    presentation_root TEXT NOT NULL CHECK (length(presentation_root) > 0),
    backing_root TEXT NOT NULL CHECK (length(backing_root) > 0),
    content_source_id TEXT NOT NULL CHECK (length(content_source_id) > 0),
    access_mode INTEGER NOT NULL CHECK (access_mode IN (1, 2)),
    generation INTEGER NOT NULL CHECK (generation > 0)
);

CREATE TABLE source_watermarks (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16)
);

CREATE TABLE source_operations (
    operation_id BLOB PRIMARY KEY CHECK (length(operation_id) = 16),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    mode INTEGER NOT NULL CHECK (mode IN (1, 2)),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    result_json BLOB NOT NULL,
    UNIQUE (source_authority, source_revision)
);

CREATE TABLE source_object_ids (
    source_authority TEXT NOT NULL,
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
	PRIMARY KEY (source_authority, source_key),
	UNIQUE (source_authority, object_id)
);

CREATE TABLE source_object_bindings (
	source_authority TEXT NOT NULL,
	tenant TEXT NOT NULL REFERENCES tenants(tenant),
	source_key TEXT NOT NULL,
	PRIMARY KEY (source_authority, tenant, source_key),
	FOREIGN KEY (source_authority, source_key) REFERENCES source_object_ids(source_authority, source_key)
);

CREATE TABLE source_commits (
    catalog_operation_id BLOB PRIMARY KEY CHECK (length(catalog_operation_id) = 16),
    source_operation_id BLOB NOT NULL REFERENCES source_operations(operation_id),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    UNIQUE (source_operation_id, tenant),
    UNIQUE (tenant, catalog_revision)
);

CREATE TABLE IF NOT EXISTS tenant_state (
    tenant TEXT PRIMARY KEY REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
	 activated_generation INTEGER NOT NULL CHECK (activated_generation = 0 OR activated_generation = generation),
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    version INTEGER NOT NULL CHECK (version > 0),
    quarantine_lane INTEGER CHECK (quarantine_lane BETWEEN 1 AND 4),
    quarantine_revision INTEGER CHECK (quarantine_revision >= 0),
    quarantine_cause INTEGER CHECK (quarantine_cause BETWEEN 1 AND 4),
    quarantine_detail TEXT,
    quarantine_since INTEGER,
    CHECK (
        (quarantine_lane IS NULL AND quarantine_revision IS NULL AND
         quarantine_cause IS NULL AND quarantine_detail IS NULL AND quarantine_since IS NULL)
        OR
        (quarantine_lane IS NOT NULL AND quarantine_revision IS NOT NULL AND
         quarantine_cause IS NOT NULL AND quarantine_detail IS NOT NULL AND quarantine_since IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS objects (
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (tenant, object_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS objects_mount_live_name
    ON objects(tenant, parent_id, name_key) WHERE tombstone = 0 AND mount_visible = 1;
CREATE UNIQUE INDEX IF NOT EXISTS objects_file_provider_live_name
    ON objects(tenant, parent_id, name_key) WHERE tombstone = 0 AND file_provider_visible = 1;
CREATE INDEX IF NOT EXISTS objects_mount_live_parent
    ON objects(tenant, parent_id, name_key, object_id) WHERE tombstone = 0 AND mount_visible = 1;
CREATE INDEX IF NOT EXISTS objects_file_provider_live_parent
    ON objects(tenant, parent_id, name_key, object_id) WHERE tombstone = 0 AND file_provider_visible = 1;

CREATE TABLE IF NOT EXISTS object_versions (
    tenant TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (tenant, object_id, revision)
);
CREATE INDEX IF NOT EXISTS object_versions_snapshot
    ON object_versions(tenant, object_id, revision DESC);
CREATE INDEX IF NOT EXISTS object_versions_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC);
CREATE INDEX IF NOT EXISTS object_versions_mount_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC) WHERE mount_visible = 1;
CREATE INDEX IF NOT EXISTS object_versions_file_provider_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC) WHERE file_provider_visible = 1;
CREATE TRIGGER IF NOT EXISTS object_versions_immutable
    BEFORE UPDATE ON object_versions
    BEGIN SELECT RAISE(ABORT, 'object revisions are immutable'); END;

CREATE TABLE IF NOT EXISTS content_stages (
    stage_id BLOB PRIMARY KEY CHECK (length(stage_id) = 16),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    mutation_id BLOB CHECK (mutation_id IS NULL OR length(mutation_id) = 16),
    temp_name TEXT NOT NULL,
    hash BLOB CHECK (hash IS NULL OR length(hash) = 32),
    size INTEGER CHECK (size IS NULL OR size >= 0),
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    CHECK ((published = 0 AND hash IS NULL AND size IS NULL) OR
           (published = 1 AND hash IS NOT NULL AND size IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS changes (
    tenant TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
	 scope_kind INTEGER NOT NULL CHECK (scope_kind IN (1, 2)),
	 presentation INTEGER NOT NULL CHECK (presentation IN (1, 2)),
	 scope_parent BLOB NOT NULL CHECK (length(scope_parent) = 16),
	 scope_domain TEXT NOT NULL,
	 scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
	 object_revision INTEGER NOT NULL CHECK (object_revision > 0),
	 PRIMARY KEY (tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation, sequence),
	 CHECK ((scope_kind = 1 AND presentation = 2 AND length(scope_domain) > 0 AND scope_generation > 0)
	     OR (scope_kind = 2 AND scope_domain = '' AND scope_generation = 0))
);
CREATE INDEX IF NOT EXISTS changes_scope_since
	 ON changes(tenant, scope_kind, presentation, scope_parent, scope_domain, scope_generation, revision, sequence);

CREATE TABLE IF NOT EXISTS prepared_mutations (
    mutation_id BLOB PRIMARY KEY CHECK (length(mutation_id) = 16),
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    kind INTEGER NOT NULL CHECK (kind BETWEEN 2 AND 5),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    intent_json BLOB NOT NULL,
    source_id TEXT NOT NULL CHECK (length(source_id) > 0),
    expected_head INTEGER NOT NULL CHECK (expected_head > 0),
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 5),
    claim_owner BLOB CHECK (claim_owner IS NULL OR length(claim_owner) = 16),
    claim_epoch INTEGER CHECK (claim_epoch IS NULL OR claim_epoch > 0),
    CHECK ((claim_owner IS NULL AND claim_epoch IS NULL) OR
           (claim_owner IS NOT NULL AND claim_epoch IS NOT NULL)),
    CHECK (state = 1 OR (claim_owner IS NOT NULL AND claim_epoch IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS prepared_mutations_active_tenant
    ON prepared_mutations(tenant) WHERE state IN (1, 2, 3, 5);

CREATE TABLE IF NOT EXISTS mutation_journal (
    mutation_id BLOB PRIMARY KEY CHECK (length(mutation_id) = 16),
    tenant TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 7),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    revision INTEGER NOT NULL CHECK (revision > 0),
    primary_object BLOB NOT NULL CHECK (length(primary_object) = 16),
    secondary_object BLOB CHECK (secondary_object IS NULL OR length(secondary_object) = 16)
);
CREATE INDEX IF NOT EXISTS mutation_journal_tenant_revision
    ON mutation_journal(tenant, revision);

CREATE TABLE IF NOT EXISTS handles (
    handle_id BLOB PRIMARY KEY CHECK (length(handle_id) = 16),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    object_revision INTEGER NOT NULL CHECK (object_revision > 0),
    opened_head INTEGER NOT NULL CHECK (opened_head >= object_revision),
    closed INTEGER NOT NULL CHECK (closed IN (0, 1))
);
CREATE INDEX IF NOT EXISTS handles_object
    ON handles(tenant, object_id, object_revision) WHERE closed = 0;

CREATE TABLE IF NOT EXISTS materialization_interests (
    interest_id BLOB PRIMARY KEY CHECK (length(interest_id) = 16),
    tenant TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    owner_presentation INTEGER NOT NULL CHECK (owner_presentation IN (1, 2)),
    owner_domain TEXT NOT NULL CHECK (length(owner_domain) > 0),
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    desired_revision INTEGER NOT NULL CHECK (desired_revision > 0),
    created_revision INTEGER NOT NULL CHECK (created_revision > 0),
    removed_revision INTEGER CHECK (removed_revision IS NULL OR removed_revision >= created_revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS materialization_interests_live
    ON materialization_interests(tenant, object_id, owner_presentation, owner_domain, owner_generation)
    WHERE removed_revision IS NULL;
CREATE INDEX IF NOT EXISTS materialization_interests_snapshot
	 ON materialization_interests(tenant, object_id, created_revision, removed_revision);
`

type failpoint func(string) error

// Catalog is the durable filesystem catalog.
type Catalog struct {
	db        *sql.DB
	readDB    *sql.DB
	blobDir   string
	owner     HandleOwnerID
	failpoint failpoint
	blobGates keyedGate
}

// Open opens or creates a durable WAL catalog at path.
func Open(ctx context.Context, path string) (*Catalog, error) {
	return open(ctx, path, nil)
}

func open(ctx context.Context, path string, fp failpoint) (*Catalog, error) {
	if path == "" {
		return nil, errors.New("catalog: database path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("catalog: create database directory: %w", err)
	}
	blobDir := path + ".blobs"
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		return nil, fmt.Errorf("catalog: create blob directory: %w", err)
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, true))
	if err != nil {
		return nil, fmt.Errorf("catalog: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	owner, err := newHandleOwnerID()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c := &Catalog{db: db, blobDir: blobDir, owner: owner, failpoint: fp}
	if err := c.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	readDB, err := sql.Open("sqlite", sqliteDSN(path, false))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("catalog: open read-only sqlite: %w", err)
	}
	readDB.SetMaxOpenConns(8)
	readDB.SetMaxIdleConns(8)
	if err := readDB.PingContext(ctx); err != nil {
		_ = readDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("catalog: connect read-only sqlite: %w", err)
	}
	c.readDB = readDB
	return c, nil
}

func sqliteDSN(path string, writer bool) string {
	location := &url.URL{Scheme: "file", Path: path}
	query := location.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	if writer {
		query.Add("_pragma", "synchronous(FULL)")
		query.Set("_txlock", "immediate")
	} else {
		query.Set("mode", "ro")
	}
	location.RawQuery = query.Encode()
	return location.String()
}

func (c *Catalog) initialize(ctx context.Context) error {
	var mode string
	if err := c.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("catalog: enable WAL: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("catalog: journal mode %q, want wal", mode)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin schema validation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	digest := schemaDigest()
	var identityTable int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_schema
WHERE type = 'table' AND name = 'fusekit_schema'`).Scan(&identityTable); err != nil {
		return fmt.Errorf("catalog: inspect schema identity table: %w", err)
	}
	if identityTable == 1 {
		var stored string
		err = tx.QueryRowContext(ctx,
			"SELECT digest FROM fusekit_schema WHERE component = 'catalog'").Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: catalog schema identity is missing", ErrSchemaMismatch)
		}
		if err != nil {
			return fmt.Errorf("catalog: read schema identity: %w", err)
		}
		if stored != digest {
			return fmt.Errorf("%w: catalog digest %q, want %q", ErrSchemaMismatch, stored, digest)
		}
	} else {
		var objects int
		if countErr := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_schema
WHERE type IN ('table', 'index', 'trigger', 'view') AND name NOT LIKE 'sqlite_%'`).Scan(&objects); countErr != nil {
			return fmt.Errorf("catalog: inspect database schema: %w", countErr)
		}
		if objects != 0 {
			return fmt.Errorf("%w: database has %d unrecognized schema objects", ErrSchemaMismatch, objects)
		}
		if _, createErr := tx.ExecContext(ctx, schema); createErr != nil {
			return fmt.Errorf("catalog: initialize schema: %w", createErr)
		}
		if _, insertErr := tx.ExecContext(ctx,
			"INSERT INTO fusekit_schema(component, digest) VALUES ('catalog', ?)", digest); insertErr != nil {
			return fmt.Errorf("catalog: record schema identity: %w", insertErr)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit schema validation: %w", err)
	}
	if _, err := c.db.ExecContext(ctx,
		"UPDATE handles SET closed = 1 WHERE closed = 0 AND owner_id <> ?", c.owner[:]); err != nil {
		return fmt.Errorf("catalog: recover stale handle pins: %w", err)
	}
	return nil
}

func schemaDigest() string {
	digest := sha256.Sum256([]byte(schema))
	return hex.EncodeToString(digest[:])
}

// Close closes the catalog after all callers have stopped.
func (c *Catalog) Close() error {
	var readErr error
	if c.readDB != nil {
		if err := c.readDB.Close(); err != nil {
			readErr = fmt.Errorf("catalog: close read-only sqlite: %w", err)
		}
	}
	var writeErr error
	if err := c.db.Close(); err != nil {
		writeErr = fmt.Errorf("catalog: close sqlite: %w", err)
	}
	return errors.Join(readErr, writeErr)
}

func (c *Catalog) trip(point string) error {
	if c.failpoint == nil {
		return nil
	}
	return c.failpoint(point)
}
