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
	"strings"

	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

// ReleaseUnclaimedContentLimit is the hard maximum exact stages per release.
const ReleaseUnclaimedContentLimit = 256

// StageOwnedContent consumes and joins one owned producer stream.
func (c *Catalog) StageOwnedContent(
	ctx context.Context, source contentstream.Source,
) (ContentRef, error) {
	if source == nil {
		return ContentRef{}, fmt.Errorf("%w: owned content source is required", ErrInvalidObject)
	}
	ref, stageErr := c.StageContent(ctx, source)
	settleErr := source.Settle(stageErr)
	waitErr := source.Wait(ctx)
	if stageErr == nil && (settleErr != nil || waitErr != nil) {
		releaseErr := c.ReleaseUnclaimedContent(context.WithoutCancel(ctx), []ContentRef{ref})
		return ContentRef{}, errors.Join(settleErr, waitErr, releaseErr)
	}
	return ref, errors.Join(stageErr, settleErr, waitErr)
}

const (
	contentAfterWrite    = "content.after_write"
	contentAfterSync     = "content.after_sync"
	contentBeforePublish = "content.before_publish"
	contentAfterPublish  = "content.after_publish"
	contentBeforeDirSync = "content.before_dir_sync"
	contentAfterDirSync  = "content.after_dir_sync"
	contentBeforeVerify  = "content.before_verify"
	contentAfterOpen     = "content.after_open"
	contentCopyBuffer    = 64 << 10
)

// StageContent streams content into the immutable blob store and returns its exact reference.
func (c *Catalog) StageContent(ctx context.Context, source io.Reader) (ref ContentRef, err error) {
	stage, err := NewStageID()
	if err != nil {
		return ContentRef{}, err
	}
	tempName := fmt.Sprintf(".stage-%x", stage[:])
	reservation, err := c.beginTemporaryContent(ctx, stage, tempName)
	if err != nil {
		return ContentRef{}, err
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
		if abortErr := c.abortTemporaryReservation(
			context.WithoutCancel(ctx), reservation,
		); abortErr != nil {
			err = errors.Join(err, abortErr)
		}
	}()

	temp, err = os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return ContentRef{}, fmt.Errorf("catalog: create staged content: %w", err)
	}

	digest := sha256.New()
	quota := reservation.writer(io.MultiWriter(temp, digest))
	size, err := io.CopyBuffer(quota, contextReader{ctx: ctx, source: source}, make([]byte, contentCopyBuffer))
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
	if err := reservation.finalizeTemporary(ctx); err != nil {
		return ContentRef{}, err
	}

	var contentHash ContentHash
	copy(contentHash[:], digest.Sum(nil))
	ref = ContentRef{Stage: stage, Hash: contentHash, Size: size}
	if err := c.publishContentStage(ctx, tempPath, ref, reservation); err != nil {
		return ContentRef{}, err
	}
	return ref, nil
}

func (c *Catalog) publishContentStage(ctx context.Context, tempPath string, ref ContentRef, reservation *temporaryReservation) error {
	return c.publishOwnedContentStage(ctx, tempPath, ref, reservation, nil)
}

func (c *Catalog) publishOwnedContentStage(
	ctx context.Context,
	tempPath string,
	ref ContentRef,
	reservation *temporaryReservation,
	mutation *MutationID,
) error {
	unlock := c.blobGates.lock(blobName(ref.Hash))
	defer unlock()

	target := c.blobPath(ref.Hash)
	if err := c.trip(contentBeforePublish); err != nil {
		return err
	}
	transition, newBlob, err := c.beginContentPublish(ctx, reservation, ref, mutation)
	if err != nil {
		return err
	}
	if newBlob {
		if err := os.Link(tempPath, target); err != nil {
			return c.quarantineStorageTransition(
				ctx, transition, fmt.Errorf("catalog: publish staged content: %w", err),
			)
		}
	} else if err := verifyBlobPath(target, ref); err != nil {
		return c.quarantineStorageTransition(ctx, transition, err)
	}
	if err := os.Remove(tempPath); err != nil {
		return c.quarantineStorageTransition(
			ctx, transition, fmt.Errorf("catalog: unlink staged content: %w", err),
		)
	}
	if err := c.trip(contentAfterPublish); err != nil {
		return err
	}
	if err := c.trip(contentBeforeDirSync); err != nil {
		return err
	}
	if err := syncStorageDirectory(c.blobDir); err != nil {
		return fmt.Errorf("catalog: sync blob directory: %w", err)
	}
	if err := c.trip(contentAfterDirSync); err != nil {
		return err
	}
	return c.finishContentPublish(ctx, reservation, transition, ref, mutation, newBlob)
}

func (c *Catalog) verifyContentRef(ctx context.Context, query rowQuerier, kind Kind, ref ContentRef) error {
	if err := c.validateContentRef(ctx, query, kind, ref); err != nil {
		return err
	}
	return c.verifyContentBlob(ctx, ref)
}

func (c *Catalog) verifyMutationContentRef(ctx context.Context, query rowQuerier, mutation MutationID, kind Kind, ref ContentRef) error {
	if err := c.validateMutationContentRef(ctx, query, mutation, kind, ref); err != nil {
		return err
	}
	return c.verifyContentBlob(ctx, ref)
}

func (c *Catalog) verifyContentBlob(ctx context.Context, ref ContentRef) error {
	if ref == (ContentRef{}) {
		return nil
	}
	if err := c.trip(contentBeforeVerify); err != nil {
		return err
	}
	file, err := c.openBlob(ctx, ref)
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

func (c *Catalog) validateSourceContentRef(ctx context.Context, query rowQuerier, operation causal.OperationID, kind Kind, ref ContentRef) error {
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
	if err := query.QueryRowContext(ctx, `
SELECT hash, size FROM content_stages
WHERE stage_id = ? AND published = 1 AND source_operation_id = ?`,
		ref.Stage[:], operation[:]).Scan(&hash, &size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: staged source content does not exist", ErrNotFound)
		}
		return fmt.Errorf("catalog: read source content stage: %w", err)
	}
	if len(hash) != len(ContentHash{}) || !bytes.Equal(hash, ref.Hash[:]) || size != ref.Size {
		return fmt.Errorf("%w: staged source content reference changed", ErrIntegrity)
	}
	return nil
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
		statement += " AND owner_id = ? AND mutation_id IS NULL AND source_operation_id IS NULL"
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

// ReleaseUnclaimedContent discards exact, catalog-owned stages that have not been claimed by a mutation.
func (c *Catalog) ReleaseUnclaimedContent(ctx context.Context, refs []ContentRef) error {
	if len(refs) > ReleaseUnclaimedContentLimit {
		return fmt.Errorf("%w: unclaimed content release exceeds %d stages", ErrInvalidObject, ReleaseUnclaimedContentLimit)
	}
	unique := make(map[ContentRef]struct{}, len(refs))
	for _, ref := range refs {
		if ref.Stage == (StageID{}) || ref.Size < 0 {
			return fmt.Errorf("%w: invalid unclaimed content reference", ErrInvalidObject)
		}
		unique[ref] = struct{}{}
	}
	if len(unique) == 0 {
		return nil
	}
	values := make([]string, 0, len(unique))
	args := make([]any, 0, len(unique)*3+1)
	for ref := range unique {
		values = append(values, "(?, ?, ?)")
		args = append(args, ref.Stage[:], ref.Hash[:], ref.Size)
	}
	requested := "WITH requested(stage_id, hash, size) AS (VALUES " + strings.Join(values, ",") + ") "
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin unclaimed content release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var unavailable, mismatched int
	inspectArgs := append(append([]any(nil), args...), c.owner[:])
	if err := tx.QueryRowContext(ctx, requested+`
SELECT
    EXISTS(
        SELECT 1 FROM requested request
        JOIN content_stages stage ON stage.stage_id = request.stage_id AND stage.owner_id = ?
        WHERE stage.mutation_id IS NOT NULL OR stage.source_operation_id IS NOT NULL OR stage.published <> 1
    ),
    EXISTS(
        SELECT 1 FROM requested request
        JOIN content_stages stage ON stage.stage_id = request.stage_id AND stage.owner_id = ?
        WHERE stage.hash <> request.hash OR stage.size <> request.size
    )`, append(inspectArgs, c.owner[:])...).Scan(&unavailable, &mismatched); err != nil {
		return fmt.Errorf("catalog: inspect unclaimed content stages: %w", err)
	}
	if unavailable != 0 {
		return fmt.Errorf("%w: content stage is not unclaimed and published", ErrInvalidTransition)
	}
	if mismatched != 0 {
		return fmt.Errorf("%w: unclaimed content reference changed", ErrIntegrity)
	}
	deleteArgs := append(append([]any(nil), args...), c.owner[:])
	if _, err := tx.ExecContext(ctx, requested+`
INSERT OR IGNORE INTO blob_gc_candidates(hash)
SELECT stage.hash
FROM content_stages stage
WHERE stage.owner_id = ? AND stage.mutation_id IS NULL AND stage.published = 1
  AND EXISTS (
      SELECT 1 FROM requested request
      WHERE request.stage_id = stage.stage_id
        AND request.hash = stage.hash
        AND request.size = stage.size
  )`, deleteArgs...); err != nil {
		return fmt.Errorf("catalog: enqueue unclaimed content blobs: %w", err)
	}
	if _, err := tx.ExecContext(ctx, requested+`
DELETE FROM content_stages
WHERE owner_id = ? AND mutation_id IS NULL AND published = 1
  AND EXISTS (
      SELECT 1 FROM requested request
      WHERE request.stage_id = content_stages.stage_id
        AND request.hash = content_stages.hash
        AND request.size = content_stages.size
  )`, deleteArgs...); err != nil {
		return fmt.Errorf("catalog: release unclaimed content stages: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit unclaimed content release: %w", err)
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

func (c *Catalog) openBlob(ctx context.Context, ref ContentRef) (*os.File, error) {
	unlock := c.blobGates.lock(blobName(ref.Hash))
	var state uint8
	var size int64
	err := c.readDB.QueryRowContext(ctx, `
SELECT state, size
FROM storage_entries
WHERE name = ? AND kind = ? AND hash = ?`,
		blobName(ref.Hash), storageEntryPublished, ref.Hash[:]).Scan(&state, &size)
	if errors.Is(err, sql.ErrNoRows) {
		unlock()
		return nil, c.quarantineStorageFailure(
			ctx, ref.Hash, ref.Size,
			fmt.Errorf("%w: content blob has no durable storage entry", ErrIntegrity),
		)
	}
	if err != nil {
		unlock()
		return nil, fmt.Errorf("catalog: read content storage entry: %w", err)
	}
	if state != storageEntryStable || size != ref.Size {
		unlock()
		return nil, c.quarantineStorageFailure(
			ctx, ref.Hash, ref.Size,
			fmt.Errorf("%w: content storage entry changed", ErrIntegrity),
		)
	}
	file, err := os.Open(c.blobPath(ref.Hash))
	unlock()
	if err != nil {
		return nil, c.quarantineStorageFailure(
			ctx, ref.Hash, ref.Size, fmt.Errorf("catalog: open content blob: %w", err),
		)
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
