package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	contentAfterWrite    = "content.after_write"
	contentAfterSync     = "content.after_sync"
	contentBeforePublish = "content.before_publish"
	contentAfterPublish  = "content.after_publish"
	contentBeforeDirSync = "content.before_dir_sync"
	contentAfterDirSync  = "content.after_dir_sync"
	contentBeforeVerify  = "content.before_verify"
	contentAfterOpen     = "content.after_open"
)

// StageContent streams content into the immutable blob store and returns its exact reference.
func (c *Catalog) StageContent(ctx context.Context, source io.Reader) (ref ContentRef, err error) {
	stage, err := NewStageID()
	if err != nil {
		return ContentRef{}, err
	}
	tempName := fmt.Sprintf(".stage-%x", stage[:])
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO content_stages(stage_id, owner_id, temp_name, published)
VALUES (?, ?, ?, 0)`, stage[:], c.owner[:], tempName); err != nil {
		return ContentRef{}, fmt.Errorf("catalog: claim staged content: %w", err)
	}
	tempPath := filepath.Join(c.blobDir, tempName)
	var temp *os.File
	defer func() {
		if err == nil {
			return
		}
		if temp != nil {
			err = errors.Join(err, temp.Close())
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("catalog: remove failed staged content: %w", removeErr))
		}
		if _, deleteErr := c.db.ExecContext(context.WithoutCancel(ctx),
			"DELETE FROM content_stages WHERE stage_id = ? AND owner_id = ?", stage[:], c.owner[:]); deleteErr != nil {
			err = errors.Join(err, fmt.Errorf("catalog: release failed content stage: %w", deleteErr))
		}
	}()

	temp, err = os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return ContentRef{}, fmt.Errorf("catalog: create staged content: %w", err)
	}

	digest := sha256.New()
	size, err := io.Copy(io.MultiWriter(temp, digest), contextReader{ctx: ctx, source: source})
	if err != nil {
		return ContentRef{}, fmt.Errorf("catalog: stream staged content: %w", err)
	}
	if err := c.trip(contentAfterWrite); err != nil {
		return ContentRef{}, err
	}
	if err := temp.Sync(); err != nil {
		return ContentRef{}, fmt.Errorf("catalog: sync staged content: %w", err)
	}
	if err := c.trip(contentAfterSync); err != nil {
		return ContentRef{}, err
	}
	if err := temp.Close(); err != nil {
		return ContentRef{}, fmt.Errorf("catalog: close staged content: %w", err)
	}
	temp = nil

	var contentHash ContentHash
	copy(contentHash[:], digest.Sum(nil))
	ref = ContentRef{Stage: stage, Hash: contentHash, Size: size}
	if err := c.publishContentStage(ctx, tempPath, ref); err != nil {
		return ContentRef{}, err
	}
	return ref, nil
}

func (c *Catalog) publishContentStage(ctx context.Context, tempPath string, ref ContentRef) error {
	unlock := c.blobGates.lock(blobName(ref.Hash))
	defer unlock()

	target := c.blobPath(ref.Hash)
	if err := c.trip(contentBeforePublish); err != nil {
		return err
	}
	if err := os.Link(tempPath, target); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("catalog: publish staged content: %w", err)
		}
		if err := verifyBlobPath(target, ref); err != nil {
			return err
		}
	}
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("catalog: unlink staged content: %w", err)
	}
	if err := c.trip(contentAfterPublish); err != nil {
		return err
	}
	dir, err := os.Open(c.blobDir)
	if err != nil {
		return fmt.Errorf("catalog: open blob directory: %w", err)
	}
	if err := c.trip(contentBeforeDirSync); err != nil {
		_ = dir.Close()
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("catalog: sync blob directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("catalog: close blob directory: %w", err)
	}
	if err := c.trip(contentAfterDirSync); err != nil {
		return err
	}
	result, err := c.db.ExecContext(ctx, `
UPDATE content_stages SET hash = ?, size = ?, published = 1
WHERE stage_id = ? AND owner_id = ? AND published = 0`,
		ref.Hash[:], ref.Size, ref.Stage[:], c.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: publish content stage: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect published content stage: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: content stage ownership lost", ErrInvalidTransition)
	}
	return nil
}

func (c *Catalog) verifyContentRef(ctx context.Context, query rowQuerier, kind Kind, ref ContentRef) error {
	if err := c.validateContentRef(ctx, query, kind, ref); err != nil {
		return err
	}
	return c.verifyContentBlob(ref)
}

func (c *Catalog) verifyMutationContentRef(ctx context.Context, query rowQuerier, mutation MutationID, kind Kind, ref ContentRef) error {
	if err := c.validateMutationContentRef(ctx, query, mutation, kind, ref); err != nil {
		return err
	}
	return c.verifyContentBlob(ref)
}

func (c *Catalog) verifyContentBlob(ref ContentRef) error {
	if ref == (ContentRef{}) {
		return nil
	}
	if err := c.trip(contentBeforeVerify); err != nil {
		return err
	}
	file, err := c.openBlob(ref.Hash)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err := c.trip(contentAfterOpen); err != nil {
		return err
	}
	return verifyOpenFile(file, ref)
}

func (c *Catalog) validateContentRef(ctx context.Context, query rowQuerier, kind Kind, ref ContentRef) error {
	return c.validateOwnedContentRef(ctx, query, kind, ref, nil)
}

func (c *Catalog) validateMutationContentRef(ctx context.Context, query rowQuerier, mutation MutationID, kind Kind, ref ContentRef) error {
	return c.validateOwnedContentRef(ctx, query, kind, ref, &mutation)
}

func (c *Catalog) validateOwnedContentRef(ctx context.Context, query rowQuerier, kind Kind, ref ContentRef, mutation *MutationID) error {
	if kind != KindFile {
		if ref != (ContentRef{}) {
			return fmt.Errorf("%w: non-file carries staged content", ErrInvalidObject)
		}
		return nil
	}
	if ref.Stage == (StageID{}) {
		return fmt.Errorf("%w: file content has no stage", ErrInvalidObject)
	}
	var hash []byte
	var size int64
	statement := `
SELECT hash, size FROM content_stages
WHERE stage_id = ? AND published = 1`
	args := []any{ref.Stage[:]}
	if mutation == nil {
		statement += " AND owner_id = ? AND mutation_id IS NULL"
		args = append(args, c.owner[:])
	} else {
		statement += " AND mutation_id = ?"
		args = append(args, mutation[:])
	}
	if err := query.QueryRowContext(ctx, statement, args...).Scan(&hash, &size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: staged content does not exist", ErrNotFound)
		}
		return fmt.Errorf("catalog: read content stage: %w", err)
	}
	if len(hash) != len(ContentHash{}) || !bytes.Equal(hash, ref.Hash[:]) || size != ref.Size {
		return fmt.Errorf("%w: staged content reference changed", ErrIntegrity)
	}
	return nil
}

func (c *Catalog) consumeContentStage(ctx context.Context, tx *sql.Tx, mutation MutationID, kind Kind, ref ContentRef) error {
	if err := c.validateMutationContentRef(ctx, tx, mutation, kind, ref); err != nil {
		return err
	}
	if kind != KindFile {
		return nil
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM content_stages
WHERE stage_id = ? AND mutation_id = ? AND published = 1`, ref.Stage[:], mutation[:])
	if err != nil {
		return fmt.Errorf("catalog: consume content stage: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect consumed content stage: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: content stage was already consumed", ErrInvalidTransition)
	}
	return nil
}

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func verifyBlobPath(path string, ref ContentRef) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("catalog: open content for verification: %w", err)
	}
	defer func() { _ = file.Close() }()
	return verifyOpenFile(file, ref)
}

func (c *Catalog) openBlob(hash ContentHash) (*os.File, error) {
	unlock := c.blobGates.lock(blobName(hash))
	file, err := os.Open(c.blobPath(hash))
	unlock()
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: content blob does not exist", ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: open content blob: %w", err)
	}
	return file, nil
}

func verifyOpenFile(file *os.File, ref ContentRef) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("catalog: stat content for verification: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() != ref.Size {
		return fmt.Errorf("%w: content size or type changed", ErrIntegrity)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("catalog: seek content for verification: %w", err)
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return fmt.Errorf("catalog: hash content for verification: %w", err)
	}
	var actual ContentHash
	copy(actual[:], digest.Sum(nil))
	if actual != ref.Hash {
		return fmt.Errorf("%w: content digest does not match address", ErrIntegrity)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("catalog: rewind verified content: %w", err)
	}
	return nil
}

func (c *Catalog) blobPath(digest ContentHash) string {
	return filepath.Join(c.blobDir, blobName(digest))
}

func blobName(digest ContentHash) string { return fmt.Sprintf("%x", digest[:]) }

type contextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(buffer)
}
