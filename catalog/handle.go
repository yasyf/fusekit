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

// SnapshotHandle is a pinned immutable object revision and streaming blob reader.
type SnapshotHandle struct {
	Handle Handle
	Object Object

	catalog *Catalog
	file    *os.File
	once    sync.Once
	err     error
}

// OpenAt pins and opens one exact object revision in a presentation.
func (c *Catalog) OpenAt(ctx context.Context, tenant TenantID, presentation Presentation, generation Generation, objectID ObjectID, revision Revision) (*SnapshotHandle, error) {
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
	obj, err := visibleObjectVersion(ctx, c.readDB, tenant, presentation, objectID, revision)
	if err != nil {
		return nil, err
	}
	if obj.Kind != KindFile {
		return nil, fmt.Errorf("%w: cannot open directory content", ErrInvalidObject)
	}
	if err := c.trip(contentBeforeVerify); err != nil {
		return nil, err
	}
	file, err := c.openBlob(obj.Hash)
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
	confirmed, err := visibleObjectVersion(ctx, tx, tenant, presentation, objectID, revision)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if confirmed.Hash != obj.Hash || confirmed.Size != obj.Size || confirmed.Kind != obj.Kind {
		_ = file.Close()
		return nil, fmt.Errorf("%w: object revision changed during open", ErrIntegrity)
	}
	var head, currentGeneration uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT t.head, s.generation
FROM tenants t JOIN tenant_state s ON s.tenant = t.tenant
WHERE t.tenant = ?`, string(tenant)).Scan(&head, &currentGeneration); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("catalog: read open-handle generation: %w", err)
	}
	if Generation(currentGeneration) != generation {
		_ = file.Close()
		return nil, fmt.Errorf("%w: got %d, current %d", ErrGenerationMismatch, generation, currentGeneration)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO handles(
    handle_id, owner_id, tenant, generation, object_id, object_revision, opened_head, closed
) VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
		id[:], c.owner[:], string(tenant), uint64(generation),
		objectID[:], uint64(obj.Revision), head); err != nil {
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
		Object: obj, catalog: c, file: file,
	}, nil
}

func visibleObjectVersion(ctx context.Context, query rowQuerier, tenant TenantID, presentation Presentation, objectID ObjectID, revision Revision) (Object, error) {
	column, err := visibilityColumn(presentation)
	if err != nil {
		return Object{}, err
	}
	statement := "SELECT " + objectColumns + `
FROM object_versions
WHERE tenant = ? AND object_id = ? AND revision = ? AND tombstone = 0 AND ` + column + ` = 1`
	obj, err := scanObject(query.QueryRowContext(ctx, statement, string(tenant), objectID[:], uint64(revision)))
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
	h.once.Do(func() {
		fileErr := h.file.Close()
		result, err := h.catalog.db.Exec(`
UPDATE handles SET closed = 1 WHERE handle_id = ? AND closed = 0`, h.Handle.ID[:])
		if err != nil {
			h.err = errors.Join(fileErr, fmt.Errorf("catalog: retire handle pin: %w", err))
			return
		}
		rows, err := result.RowsAffected()
		if err != nil {
			h.err = errors.Join(fileErr, fmt.Errorf("catalog: inspect handle retirement: %w", err))
			return
		}
		if rows != 1 {
			h.err = errors.Join(fileErr, ErrHandleClosed)
			return
		}
		h.err = fileErr
	})
	return h.err
}

var _ interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
} = (*SnapshotHandle)(nil)
