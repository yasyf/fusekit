package sourceauthority

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

type limitedOwnedContent struct {
	contentstream.Source
	reader io.Reader
}

func (s limitedOwnedContent) Read(buffer []byte) (int, error) {
	return s.reader.Read(buffer)
}

func (r *Runtime) stageContent(ctx context.Context, source contentstream.Source) (catalog.ContentRef, error) {
	return r.stageContentWithin(ctx, source, SnapshotPlanOutputByteLimit)
}

func (r *Runtime) stageContentWithin(ctx context.Context, source contentstream.Source, limit int64) (catalog.ContentRef, error) {
	if source == nil || limit <= 0 {
		return catalog.ContentRef{}, fmt.Errorf("%w: invalid content stream limit", ErrInvalidPlan)
	}
	limited := limitedOwnedContent{Source: source, reader: io.LimitReader(source, limit+1)}
	ref, err := r.catalog.StageOwnedContent(ctx, limited)
	if err != nil {
		return catalog.ContentRef{}, err
	}
	if ref.Size <= limit {
		return ref, nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
	defer cancel()
	cleanupErr := r.catalog.ReleaseUnclaimedContent(cleanupCtx, []catalog.ContentRef{ref})
	return catalog.ContentRef{}, errors.Join(
		fmt.Errorf("%w: projected content exceeds %d bytes", ErrInvalidPlan, limit),
		cleanupErr,
	)
}
