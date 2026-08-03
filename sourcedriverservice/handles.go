package sourcedriverservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

// sessionState is the per-session server state the withdrawn streams left
// behind: the pinned content reads a client pages through, and the begun
// mutations whose bodies it stages before the commit that consumes them.
// Session.Done() releases every one, so a peer that vanishes mid-transfer
// strands nothing.
type sessionState struct {
	mu       sync.Mutex
	closed   bool
	sequence uint64
	handles  map[sourcedriverproto.HandleID]*contentHandle
	applies  map[string]*pendingApply
}

func newSessionState() *sessionState {
	return &sessionState{
		handles: make(map[sourcedriverproto.HandleID]*contentHandle),
		applies: make(map[string]*pendingApply),
	}
}

func (s *sessionState) openContent(source contentstream.Source, ref sourcedriver.ContentRef) (sourcedriverproto.HandleID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", errors.New("source driver service: session has settled")
	}
	s.sequence++
	id := sourcedriverproto.HandleID(fmt.Sprintf("content-%d", s.sequence))
	s.handles[id] = &contentHandle{source: source, ref: ref, hasher: sha256.New()}
	return id, nil
}

func (s *sessionState) content(id sourcedriverproto.HandleID) (*contentHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, open := s.handles[id]
	if !open {
		return nil, fmt.Errorf("%w: content handle %q is not open", sourcedriver.ErrNotFound, id)
	}
	return handle, nil
}

func (s *sessionState) takeContent(id sourcedriverproto.HandleID) (*contentHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handle, open := s.handles[id]
	if !open {
		return nil, fmt.Errorf("%w: content handle %q is not open", sourcedriver.ErrNotFound, id)
	}
	delete(s.handles, id)
	return handle, nil
}

// begin declares one mutation apply, restarting from scratch when the id is
// already begun: a retried ApplyMutation must be able to re-stage after a lost
// response rather than wedge on its own stranded upload.
func (s *sessionState) begin(id string, request sourcedriver.MutationRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("source driver service: session has settled")
	}
	if stale, begun := s.applies[id]; begun {
		delete(s.applies, id)
		if err := stale.close(); err != nil {
			return err
		}
	}
	pending := &pendingApply{request: request}
	if request.HasContent {
		file, err := createScratch()
		if err != nil {
			return err
		}
		pending.upload = &mutationUpload{file: file, hasher: sha256.New()}
	}
	s.applies[id] = pending
	return nil
}

func (s *sessionState) chunk(id string, sequence uint32, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("source driver service: session has settled")
	}
	pending, begun := s.applies[id]
	if !begun {
		return fmt.Errorf("%w: mutation apply %q is not begun", sourcedriver.ErrNotFound, id)
	}
	if pending.upload == nil {
		return fmt.Errorf("%w: contentless mutation accepts no chunks", sourcedriver.ErrIntegrity)
	}
	if sequence != pending.upload.cursor+1 {
		return fmt.Errorf("%w: mutation chunk sequence differs", sourcedriver.ErrIntegrity)
	}
	return pending.upload.write(payload)
}

func (s *sessionState) takeApply(id string) (*pendingApply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, begun := s.applies[id]
	if !begun {
		return nil, fmt.Errorf("%w: mutation apply %q is not begun", sourcedriver.ErrNotFound, id)
	}
	delete(s.applies, id)
	return pending, nil
}

func (s *sessionState) release() error {
	s.mu.Lock()
	s.closed = true
	handles, applies := s.handles, s.applies
	s.handles = make(map[sourcedriverproto.HandleID]*contentHandle)
	s.applies = make(map[string]*pendingApply)
	s.mu.Unlock()
	released := make([]error, 0, len(handles)+len(applies))
	for _, handle := range handles {
		released = append(released, handle.settle(errSessionReleased))
	}
	for _, pending := range applies {
		released = append(released, pending.close())
	}
	return errors.Join(released...)
}

// pendingApply is one begun mutation awaiting its commit: the exact request the
// begin declared, and the body staged chunk by chunk when it carries content.
type pendingApply struct {
	request sourcedriver.MutationRequest
	upload  *mutationUpload
}

func (p *pendingApply) close() error {
	if p.upload == nil {
		return nil
	}
	return p.upload.close()
}

var errSessionReleased = errors.New("source driver service: session released its open content")

// contentHandle is one open source body, read forward by bounded unary pages.
// The source is a sequential reader, so a page names the offset it continues
// from and a page that skips or repeats bytes is refused rather than served.
type contentHandle struct {
	mu       sync.Mutex
	source   contentstream.Source
	ref      sourcedriver.ContentRef
	hasher   hash.Hash
	consumed int64
	ended    bool
	settled  bool
	err      error
}

func (h *contentHandle) read(ctx context.Context, offset int64, limit int) ([]byte, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.settled {
		return nil, false, errors.Join(fmt.Errorf("%w: content handle has settled", sourcedriver.ErrIntegrity), h.err)
	}
	if offset != h.consumed {
		return nil, false, fmt.Errorf("%w: content read offset %d does not continue from %d", sourcedriver.ErrIntegrity, offset, h.consumed)
	}
	if h.ended {
		return nil, true, nil
	}
	page := make([]byte, 0, limit)
	buffer := make([]byte, limit)
	for len(page) < limit {
		count, readErr := h.source.Read(buffer[:limit-len(page)])
		if count > 0 {
			h.consumed += int64(count)
			_, _ = h.hasher.Write(buffer[:count])
			if h.consumed > h.ref.Size || h.consumed > sourcedriver.MaxContentBytes {
				return nil, false, h.failLocked(ctx, fmt.Errorf("%w: content exceeds exact size", sourcedriver.ErrIntegrity))
			}
			page = append(page, buffer[:count]...)
		}
		if errors.Is(readErr, io.EOF) {
			var actual catalog.ContentHash
			copy(actual[:], h.hasher.Sum(nil))
			if h.consumed != h.ref.Size || actual != h.ref.Hash {
				return nil, false, h.failLocked(ctx, fmt.Errorf("%w: content size or digest differs", sourcedriver.ErrIntegrity))
			}
			h.ended = true
			break
		}
		if readErr != nil {
			return nil, false, h.failLocked(ctx, readErr)
		}
		if count == 0 {
			return nil, false, h.failLocked(ctx, errors.New("source driver service: content reader made no progress"))
		}
	}
	return page, h.ended, nil
}

func (h *contentHandle) failLocked(ctx context.Context, cause error) error {
	return errors.Join(cause, h.settleLocked(ctx, cause))
}

// settle releases the source, refusing a close that arrives before EOF exactly
// as the withdrawn stream's terminal did.
func (h *contentHandle) settle(cause error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cause == nil && !h.ended {
		cause = fmt.Errorf("%w: content settled before EOF", sourcedriver.ErrIntegrity)
	}
	return errors.Join(cause, h.settleLocked(context.Background(), cause))
}

func (h *contentHandle) settleLocked(ctx context.Context, cause error) error {
	if h.settled {
		return h.err
	}
	h.settled, h.err = true, cause
	settleErr := h.source.Settle(cause)
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()
	return errors.Join(settleErr, h.source.Wait(waitCtx))
}

// mutationUpload is one mutation body staged ahead of the commit that consumes
// it: unlinked scratch on disk, so a gigabyte body never sits in memory.
type mutationUpload struct {
	file   *os.File
	hasher hash.Hash
	size   int64
	cursor uint32
}

func (u *mutationUpload) write(payload []byte) error {
	if u.size+int64(len(payload)) > sourcedriver.MaxContentBytes {
		return fmt.Errorf("%w: mutation content exceeds exact size", sourcedriver.ErrIntegrity)
	}
	if _, err := u.file.WriteAt(payload, u.size); err != nil {
		return err
	}
	u.size += int64(len(payload))
	_, _ = u.hasher.Write(payload)
	u.cursor++
	return nil
}

// source seals the staged body against the exact size and digest the commit
// names, refusing when the staged bytes differ.
func (u *mutationUpload) source(size int64, digest catalog.ContentHash) (contentstream.Source, error) {
	var staged catalog.ContentHash
	copy(staged[:], u.hasher.Sum(nil))
	if u.size != size || staged != digest {
		return nil, fmt.Errorf("%w: staged mutation content size or digest differs", sourcedriver.ErrIntegrity)
	}
	if err := u.file.Sync(); err != nil {
		return nil, err
	}
	return &stagedSource{
		reader: io.NewSectionReader(u.file, 0, u.size), file: u.file, done: make(chan struct{}),
	}, nil
}

func (u *mutationUpload) close() error { return u.file.Close() }

// stagedSource replays one staged mutation body to the driver.
type stagedSource struct {
	reader *io.SectionReader
	file   *os.File

	mu     sync.Mutex
	settle sync.Once
	done   chan struct{}
	err    error
}

func (s *stagedSource) Read(buffer []byte) (int, error) { return s.reader.Read(buffer) }

func (s *stagedSource) Settle(cause error) error {
	s.settle.Do(func() {
		s.mu.Lock()
		s.err = errors.Join(cause, s.file.Close())
		s.mu.Unlock()
		close(s.done)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *stagedSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		_ = s.Settle(ctx.Err())
		<-s.done
		return ctx.Err()
	}
}

var _ contentstream.Source = (*stagedSource)(nil)

func createScratch() (*os.File, error) {
	file, err := os.CreateTemp("", "fusekit-source-mutation-")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	if err := os.Chmod(path, 0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
