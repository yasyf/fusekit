package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

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
	if _, err := db.ExecContext(ctx, "UPDATE fusekit_schema SET digest = 'old-build' WHERE component = 'catalog'"); err != nil {
		t.Fatalf("replace schema digest: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close modified database: %v", err)
	}

	if _, err := Open(ctx, path); !errors.Is(err, ErrSchemaMismatch) {
		t.Fatalf("Open mismatched digest = %v, want ErrSchemaMismatch", err)
	}
}
