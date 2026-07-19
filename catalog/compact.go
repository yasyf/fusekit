package catalog

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	compactAfterMark   = "compact.after_mark"
	compactBeforeSweep = "compact.before_sweep"
)

// Compact advances a tenant's anchor floor and removes unreferenced staged blobs.
func (c *Catalog) Compact(ctx context.Context, tenant TenantID, floor Revision) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin compaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, current, err := revisionState(ctx, tx, tenant)
	if err != nil {
		return err
	}
	if floor < current || floor > head {
		return fmt.Errorf("%w: compaction floor %d outside [%d,%d]", ErrInvalidTransition, floor, current, head)
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM changes WHERE tenant = ? AND revision < ?", string(tenant), uint64(floor)); err != nil {
		return fmt.Errorf("catalog: compact changes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM object_versions
WHERE rowid IN (
    SELECT old.rowid
    FROM object_versions old
    WHERE old.tenant = ?
      AND old.revision < ?
      AND old.revision <> (
          SELECT MAX(base.revision)
          FROM object_versions base
          WHERE base.tenant = old.tenant
            AND base.object_id = old.object_id
            AND base.revision <= ?
      )
      AND NOT EXISTS (
          SELECT 1 FROM handles pin
          WHERE pin.closed = 0
            AND pin.owner_id = ?
            AND pin.tenant = old.tenant
            AND pin.object_id = old.object_id
            AND pin.object_revision = old.revision
      )
)`, string(tenant), uint64(floor), uint64(floor), c.owner[:]); err != nil {
		return fmt.Errorf("catalog: compact object revisions: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE tenants SET floor = ? WHERE tenant = ?", uint64(floor), string(tenant)); err != nil {
		return fmt.Errorf("catalog: advance compaction floor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit compaction: %w", err)
	}
	if err := c.compactBlobs(ctx); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) compactBlobs(ctx context.Context) error {
	marked, _, err := c.blobReferences(ctx)
	if err != nil {
		return err
	}
	if err := c.trip(compactAfterMark); err != nil {
		return err
	}
	entries, err := os.ReadDir(c.blobDir)
	if err != nil {
		return fmt.Errorf("catalog: enumerate blob directory: %w", err)
	}
	candidates := make([]os.DirEntry, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		_, live := marked[name]
		if live {
			continue
		}
		if entry.IsDir() || (!strings.HasPrefix(name, ".stage-") && len(name) != hex.EncodedLen(len(ContentHash{}))) {
			return fmt.Errorf("catalog: unexpected blob-store entry %q", name)
		}
		candidates = append(candidates, entry)
	}
	if err := c.trip(compactBeforeSweep); err != nil {
		return err
	}
	for _, entry := range candidates {
		name := entry.Name()
		unlock := c.blobGates.lock(name)
		referenced, err := c.blobEntryReferenced(ctx, name)
		if err != nil {
			unlock()
			return err
		}
		if referenced {
			unlock()
			continue
		}
		if err := os.Remove(c.blobDir + string(os.PathSeparator) + name); err != nil && !errors.Is(err, os.ErrNotExist) {
			unlock()
			return fmt.Errorf("catalog: remove unreferenced blob %q: %w", name, err)
		}
		unlock()
	}
	dir, err := os.Open(c.blobDir)
	if err != nil {
		return fmt.Errorf("catalog: open compacted blob directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("catalog: sync compacted blob directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("catalog: close compacted blob directory: %w", err)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM content_stages WHERE owner_id <> ? AND mutation_id IS NULL", c.owner[:]); err != nil {
		return fmt.Errorf("catalog: retire abandoned content stages: %w", err)
	}
	return nil
}

func (c *Catalog) blobEntryReferenced(ctx context.Context, name string) (bool, error) {
	if strings.HasPrefix(name, ".stage-") {
		var live bool
		if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM content_stages
    WHERE temp_name = ? AND (owner_id = ? OR mutation_id IS NOT NULL)
)`, name, c.owner[:]).Scan(&live); err != nil {
			return false, fmt.Errorf("catalog: recheck staged file %q: %w", name, err)
		}
		return live, nil
	}
	raw, err := hex.DecodeString(name)
	if err != nil || len(raw) != len(ContentHash{}) {
		return false, fmt.Errorf("catalog: invalid blob name %q", name)
	}
	var live bool
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM object_versions WHERE kind = ? AND tombstone = 0 AND hash = ?
    UNION ALL
    SELECT 1 FROM content_stages
    WHERE published = 1 AND hash = ? AND (owner_id = ? OR mutation_id IS NOT NULL)
)`, uint8(KindFile), raw, raw, c.owner[:]).Scan(&live); err != nil {
		return false, fmt.Errorf("catalog: recheck blob %q: %w", name, err)
	}
	return live, nil
}

func (c *Catalog) blobReferences(ctx context.Context) (map[string]struct{}, int, error) {
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("catalog: begin blob reference snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	referenced := map[string]struct{}{}
	rows, err := tx.QueryContext(ctx,
		"SELECT DISTINCT hash FROM object_versions WHERE kind = ? AND tombstone = 0", uint8(KindFile))
	if err != nil {
		return nil, 0, fmt.Errorf("catalog: query referenced blobs: %w", err)
	}
	if err := collectBlobHashes(rows, referenced, "referenced blob"); err != nil {
		return nil, 0, err
	}
	rows, err = tx.QueryContext(ctx, `
SELECT hash FROM content_stages
WHERE published = 1 AND (owner_id = ? OR mutation_id IS NOT NULL)`, c.owner[:])
	if err != nil {
		return nil, 0, fmt.Errorf("catalog: query published content stages: %w", err)
	}
	if err := collectBlobHashes(rows, referenced, "published content stage"); err != nil {
		return nil, 0, err
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM content_stages WHERE owner_id = ? AND published = 0`, c.owner[:]).Scan(&pending); err != nil {
		return nil, 0, fmt.Errorf("catalog: count pending content stages: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("catalog: finish blob reference snapshot: %w", err)
	}
	return referenced, pending, nil
}

func collectBlobHashes(rows *sql.Rows, referenced map[string]struct{}, label string) error {
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("catalog: scan %s: %w", label, err)
		}
		if len(raw) != len(ContentHash{}) {
			_ = rows.Close()
			return fmt.Errorf("catalog: corrupt %s hash length %d", label, len(raw))
		}
		referenced[hex.EncodeToString(raw)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("catalog: read %ss: %w", label, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("catalog: close %ss: %w", label, err)
	}
	return nil
}
