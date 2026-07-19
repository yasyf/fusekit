package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const handleAfterClose = "handle.after_close"

// SnapshotHandle is a pinned immutable object revision and streaming blob reader.
type SnapshotHandle struct {
	Handle Handle
	Object Object

	catalog  *Catalog
	owner    RetentionOwner
	file     *os.File
	fileOnce sync.Once
	fileErr  error
	closeMu  sync.Mutex
	closed   bool
}

// OpenAt pins and opens one exact object revision in a presentation.
func (c *Catalog) OpenAt(
	ctx context.Context,
	owner RetentionOwner,
	tenant TenantID,
	presentation Presentation,
	generation Generation,
	objectID ObjectID,
	revision Revision,
) (*SnapshotHandle, error) {
	if _, err := NewRetentionOwner(string(owner)); err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, fmt.Errorf("%w: handle generation is zero", ErrInvalidTransition)
	}
	if revision == 0 {
		return nil, fmt.Errorf("%w: handle revision is zero", ErrInvalidTransition)
	}
	id, err := NewHandleID()
	if err != nil {
		return nil, err
	}
	readTx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("catalog: begin pinned object lookup: %w", err)
	}
	obj, err := visibleObjectVersion(ctx, readTx, tenant, presentation, objectID, revision)
	if err != nil {
		_ = readTx.Rollback()
		return nil, err
	}
	if err := readTx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: finish pinned object lookup: %w", err)
	}
	if obj.Kind != KindFile {
		return nil, fmt.Errorf("%w: cannot open directory content", ErrInvalidObject)
	}
	if err := c.trip(contentBeforeVerify); err != nil {
		return nil, err
	}
	file, err := c.openBlob(ctx, ContentRef{Hash: obj.Hash, Size: obj.Size})
	if err != nil {
		return nil, fmt.Errorf("catalog: open pinned content: %w", err)
	}
	if err := c.trip(contentAfterOpen); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := verifyOpenFile(file, ContentRef{Hash: obj.Hash, Size: obj.Size}); err != nil {
		_ = file.Close()
		return nil, err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("catalog: begin open handle: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureRetentionOwner(ctx, tx, c.owner, owner); err != nil {
		_ = file.Close()
		return nil, err
	}
	confirmed, err := visibleObjectVersion(ctx, tx, tenant, presentation, objectID, revision)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if confirmed.Hash != obj.Hash || confirmed.Size != obj.Size || confirmed.Kind != obj.Kind {
		_ = file.Close()
		return nil, fmt.Errorf("%w: object revision changed during open", ErrIntegrity)
	}
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	var currentGeneration uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT generation FROM tenant_state WHERE tenant = ?`, string(tenant)).Scan(&currentGeneration); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("catalog: read open-handle generation: %w", err)
	}
	if Generation(currentGeneration) != generation {
		_ = file.Close()
		return nil, fmt.Errorf("%w: got %d, current %d", ErrGenerationMismatch, generation, currentGeneration)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO handles(
    handle_id, owner_id, session_owner, tenant, generation,
    object_id, object_revision, opened_head, closed
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		id[:], c.owner[:], string(owner), string(tenant), uint64(generation),
		objectID[:], uint64(obj.Revision), uint64(view.head)); err != nil {
		_ = file.Close()
		return nil, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("catalog: commit open handle: %w", err)
	}
	return &SnapshotHandle{
		Handle: Handle{
			ID: id, Tenant: tenant, Generation: generation,
			ObjectID: objectID, ObjectRevision: obj.Revision,
		},
		Object: obj, catalog: c, owner: owner, file: file,
	}, nil
}

func visibleObjectVersion(ctx context.Context, query rowQuerier, tenant TenantID, presentation Presentation, objectID ObjectID, revision Revision) (Object, error) {
	column, err := visibilityColumn(presentation)
	if err != nil {
		return Object{}, err
	}
	view, err := readCatalogView(ctx, query, tenant)
	if err != nil {
		return Object{}, err
	}
	if err := validateAnchor(revision, view.head, view.floor); err != nil {
		return Object{}, err
	}
	var statement string
	var args []any
	if len(view.publication) != 0 {
		statement = `WITH RECURSIVE publication_lineage(publication_id, predecessor_publication_id) AS (
    SELECT publication_id, predecessor_publication_id
    FROM source_driver_publications
    WHERE source_authority = ? AND publication_id = ?
    UNION
    SELECT predecessor.publication_id, predecessor.predecessor_publication_id
    FROM source_driver_publications predecessor
    JOIN publication_lineage successor
      ON predecessor.publication_id = successor.predecessor_publication_id
    WHERE predecessor.source_authority = ?
)
SELECT ` + versionColumns + `
FROM publication_lineage lineage
JOIN source_driver_publications publication
  ON publication.source_authority = ? AND publication.publication_id = lineage.publication_id
JOIN source_driver_publication_versions v
  ON v.source_authority = publication.source_authority
 AND v.publication_id = publication.publication_id
WHERE v.tenant = ? AND v.object_id = ? AND v.revision = ?
  AND v.tombstone = 0 AND v.` + column + ` = 1
ORDER BY publication.source_revision DESC LIMIT 1`
		args = []any{view.authority, view.publication, view.authority, view.authority,
			string(tenant), objectID[:], uint64(revision)}
	} else {
		statement = "SELECT " + versionColumns + `
FROM object_versions v
WHERE v.tenant = ? AND v.object_id = ? AND v.revision = ?
  AND v.tombstone = 0 AND v.` + column + ` = 1`
		args = []any{string(tenant), objectID[:], uint64(revision)}
	}
	obj, err := scanObject(query.QueryRowContext(ctx, statement, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read visible object revision: %w", err)
	}
	return obj, nil
}

// Read implements io.Reader over the pinned immutable content.
func (h *SnapshotHandle) Read(buffer []byte) (int, error) { return h.file.Read(buffer) }

// ReadAt implements io.ReaderAt over the pinned immutable content.
func (h *SnapshotHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return h.file.ReadAt(buffer, offset)
}

// Seek implements io.Seeker over the pinned immutable content.
func (h *SnapshotHandle) Seek(offset int64, whence int) (int64, error) {
	return h.file.Seek(offset, whence)
}

// Close releases the file descriptor before retiring its durable pin.
func (h *SnapshotHandle) Close() error {
	h.fileOnce.Do(func() {
		h.fileErr = h.file.Close()
	})
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if h.closed {
		return h.fileErr
	}
	tx, err := h.catalog.db.Begin()
	if err != nil {
		return errors.Join(h.fileErr, fmt.Errorf("catalog: begin handle retirement: %w", err))
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.Exec(`
UPDATE handles SET closed = 1
WHERE handle_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND object_id = ? AND object_revision = ? AND closed = 0`,
		h.Handle.ID[:], h.catalog.owner[:], string(h.owner),
		string(h.Handle.Tenant), h.Handle.ObjectID[:], uint64(h.Handle.ObjectRevision))
	if err != nil {
		return errors.Join(h.fileErr, fmt.Errorf("catalog: retire handle pin: %w", err))
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Join(h.fileErr, fmt.Errorf("catalog: inspect handle retirement: %w", err))
	}
	if rows == 0 {
		var closed bool
		err := tx.QueryRow(`
SELECT closed FROM handles
WHERE handle_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND object_id = ? AND object_revision = ?`,
			h.Handle.ID[:], h.catalog.owner[:], string(h.owner),
			string(h.Handle.Tenant), h.Handle.ObjectID[:], uint64(h.Handle.ObjectRevision)).
			Scan(&closed)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.Join(h.fileErr, ErrHandleClosed)
		}
		if err != nil {
			return errors.Join(h.fileErr, fmt.Errorf("catalog: read handle retirement replay: %w", err))
		}
		if !closed {
			return errors.Join(h.fileErr, ErrConflict)
		}
	} else if rows != 1 {
		return errors.Join(h.fileErr, fmt.Errorf("%w: handle retirement changed multiple rows", ErrIntegrity))
	}
	if err := h.catalog.trip(handleAfterClose); err != nil {
		return errors.Join(h.fileErr, err)
	}
	if rows == 1 {
		if err := enqueueCatalogMaintenance(context.Background(), tx, h.Handle.Tenant); err != nil {
			return errors.Join(h.fileErr, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.Join(h.fileErr, fmt.Errorf("catalog: commit handle retirement: %w", err))
	}
	h.closed = true
	return h.fileErr
}

// Forget removes one acknowledged closed-handle receipt. Missing receipts are
// an idempotent success for the exact still-live owner.
func (h *SnapshotHandle) Forget(ctx context.Context) error {
	if err := h.Close(); err != nil {
		return err
	}
	tx, err := h.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin handle forget: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireLiveRetentionOwner(ctx, tx, h.catalog.owner, h.owner); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM handles
WHERE handle_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND object_id = ? AND object_revision = ? AND closed = 1`,
		h.Handle.ID[:], h.catalog.owner[:], string(h.owner),
		string(h.Handle.Tenant), h.Handle.ObjectID[:], uint64(h.Handle.ObjectRevision))
	if err != nil {
		return fmt.Errorf("catalog: forget handle receipt: %w", err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("catalog: inspect handle forget: %w", err)
	}
	if err := enqueueCatalogMaintenance(ctx, tx, h.Handle.Tenant); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit handle forget: %w", err)
	}
	return nil
}

var _ interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
} = (*SnapshotHandle)(nil)
