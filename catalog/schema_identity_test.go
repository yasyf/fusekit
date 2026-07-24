package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogRecordsExactSchemaMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	catalog, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()
	var component, identity, digest string
	var version uint64
	if err := db.QueryRowContext(ctx, `
SELECT component, schema_identity, schema_version, digest FROM fusekit_schema`).
		Scan(&component, &identity, &version, &digest); err != nil {
		t.Fatalf("read schema metadata: %v", err)
	}
	if component != "catalog" || identity != SchemaIdentity || version != SchemaVersion || digest != mustCompiledSchemaDigest(t) {
		t.Fatalf("schema metadata = (%q, %q, %d, %q)", component, identity, version, digest)
	}
}

func TestOpenRejectsForeignSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open foreign database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE old_runtime_state (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create foreign schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close foreign database: %v", err)
	}

	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open foreign schema = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsDifferentCatalogDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	catalog, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE fusekit_schema SET digest = ? WHERE component = 'catalog'", strings.Repeat("f", 64)); err != nil {
		t.Fatalf("replace schema digest: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}

	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open mismatched digest = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsMissingSchemaMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	catalog, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM fusekit_schema"); err != nil {
		t.Fatalf("delete schema metadata: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}
	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open missing metadata = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsExtraSchemaMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE fusekit_schema (
    component TEXT NOT NULL PRIMARY KEY,
    schema_identity TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    digest TEXT NOT NULL
) STRICT;
INSERT INTO fusekit_schema VALUES ('catalog', ?, ?, ?);
INSERT INTO fusekit_schema VALUES ('extra', ?, ?, ?);`,
		SchemaIdentity, SchemaVersion, mustCompiledSchemaDigest(t), SchemaIdentity, SchemaVersion, mustCompiledSchemaDigest(t)); err != nil {
		t.Fatalf("create extra schema metadata: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}
	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open extra metadata = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsLegacySchemaMetadataShape(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE fusekit_schema (
    component TEXT PRIMARY KEY,
    digest TEXT NOT NULL
);
INSERT INTO fusekit_schema VALUES ('catalog', ?);`, mustCompiledSchemaDigest(t)); err != nil {
		t.Fatalf("create legacy schema metadata: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}
	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open legacy metadata = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsExtraSchemaMetadataColumn(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	catalog, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE fusekit_schema ADD COLUMN extra TEXT"); err != nil {
		t.Fatalf("add schema metadata column: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}
	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open extra metadata column = %v, want ErrSchemaMismatch", err)
	}
}

func TestOpenRejectsWrongSchemaIdentityOrVersion(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(context.Context, *sql.DB) error
	}{
		{name: "identity", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx,
				"UPDATE fusekit_schema SET schema_identity = 'github.com/yasyf/fusekit/old-catalog'")
			return err
		}},
		{name: "version", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "UPDATE fusekit_schema SET schema_version = 2")
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			catalog, err := Open(ctx, path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := catalog.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("open database: %v", err)
			}
			if err := test.mutate(ctx, db); err != nil {
				t.Fatalf("replace schema %s: %v", test.name, err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close modified database: %v", err)
			}
			if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
				t.Fatalf("Open mismatched %s = %v, want ErrSchemaMismatch", test.name, err)
			}
		})
	}
}

func TestOpenRejectsTamperedSchemaCatalog(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(context.Context, *sql.DB) error
	}{
		{name: "extra table", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE unauthorized_schema_object (id INTEGER PRIMARY KEY)")
			return err
		}},
		{name: "extra index", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "CREATE INDEX unauthorized_schema_index ON tenants(tenant)")
			return err
		}},
		{name: "sqlite-like extra table", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE sqliteX_extra (id INTEGER PRIMARY KEY)")
			return err
		}},
		{name: "missing object", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, "DROP INDEX changes_compaction")
			return err
		}},
		{name: "altered definition", mutate: func(ctx context.Context, db *sql.DB) error {
			_, err := db.ExecContext(ctx, `
DROP INDEX changes_compaction;
CREATE INDEX changes_compaction ON changes(tenant, object_id)`)
			return err
		}},
		{name: "stored marker spoof", mutate: func(ctx context.Context, db *sql.DB) error {
			if _, err := db.ExecContext(ctx, `
DROP INDEX changes_compaction;
CREATE INDEX changes_compaction ON changes(tenant, object_id)`); err != nil {
				return err
			}
			objects, err := readSchemaCatalog(ctx, db)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx,
				"UPDATE fusekit_schema SET digest = ? WHERE component = 'catalog'",
				schemaAttestationDigest(objects))
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			catalog, err := Open(ctx, path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := catalog.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("open database: %v", err)
			}
			if err := test.mutate(ctx, db); err != nil {
				_ = db.Close()
				t.Fatalf("tamper schema: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close tampered database: %v", err)
			}
			if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
				t.Fatalf("Open tampered schema = %v, want ErrSchemaMismatch", err)
			}
		})
	}
}

func TestOpenAcceptsSQLiteInternalSchemaObjects(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	catalog, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ANALYZE"); err != nil {
		_ = db.Close()
		t.Fatalf("ANALYZE: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close analyzed database: %v", err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen with SQLite internal objects: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened catalog: %v", err)
	}
}

func TestCompiledSchemaHasNoCompatibilityDDLOrRetiredActivationNames(t *testing.T) {
	t.Parallel()
	normalized := strings.ToUpper(schema)
	for _, forbidden := range []string{"IF NOT EXISTS", "ALTER TABLE", "CONVERGENCE_OUTBOX"} {
		if strings.Contains(normalized, forbidden) {
			t.Fatalf("compiled schema contains forbidden residue %q", forbidden)
		}
	}
}

func mustCompiledSchemaDigest(t *testing.T) string {
	t.Helper()
	attestation, err := compiledSchemaAttestation()
	if err != nil {
		t.Fatalf("compiledSchemaAttestation: %v", err)
	}
	return attestation.digest
}
