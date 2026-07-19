package sourceauthority

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

type boundedStageStore struct {
	Store
	staged   []byte
	released []catalog.ContentRef
}

func (s *boundedStageStore) StageOwnedContent(_ context.Context, source contentstream.Source) (catalog.ContentRef, error) {
	content, err := io.ReadAll(source)
	if err != nil {
		return catalog.ContentRef{}, err
	}
	if err := source.Settle(nil); err != nil {
		return catalog.ContentRef{}, err
	}
	if err := source.Wait(context.Background()); err != nil {
		return catalog.ContentRef{}, err
	}
	s.staged = content
	return catalog.ContentRef{Size: int64(len(content))}, nil
}

func (s *boundedStageStore) ReleaseUnclaimedContent(_ context.Context, refs []catalog.ContentRef) error {
	s.released = append(s.released, refs...)
	return nil
}

func TestStageContentRejectsAndReleasesOverLimitStream(t *testing.T) {
	store := &boundedStageStore{}
	runtime := &Runtime{catalog: store}
	if _, err := runtime.stageContentWithin(t.Context(), ownedTestContent(io.NopCloser(bytes.NewReader([]byte("abcde")))), 3); err == nil {
		t.Fatal("oversized content unexpectedly staged")
	}
	if got := string(store.staged); got != "abcd" {
		t.Fatalf("bounded stage read = %q, want one-byte overflow proof", got)
	}
	if len(store.released) != 1 || store.released[0].Size != 4 {
		t.Fatalf("released stages = %+v, want exact oversized stage", store.released)
	}
}

func TestStageContentAcceptsExactLimitWithoutRelease(t *testing.T) {
	store := &boundedStageStore{}
	runtime := &Runtime{catalog: store}
	ref, err := runtime.stageContentWithin(t.Context(), ownedTestContent(io.NopCloser(bytes.NewReader([]byte("abc")))), 3)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Size != 3 || string(store.staged) != "abc" {
		t.Fatalf("stage = (%d, %q), want exact content", ref.Size, store.staged)
	}
	if len(store.released) != 0 {
		t.Fatalf("released stages = %+v, want none", store.released)
	}
}
