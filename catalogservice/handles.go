package catalogservice

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/contentstream"
)

var errSessionReleased = errors.New("catalog service: session released its open content")

// sessionState is the per-session server state the withdrawn streams left
// behind: the pinned content handles a client pages through, the begun content
// mutations whose bodies it stages before the commit that consumes them, and
// the activation inbox parked polls drain. Session.Done() releases every one,
// so a peer that vanishes mid-transfer strands nothing.
type sessionState struct {
	mu      sync.Mutex
	closed  bool
	handles map[catalogproto.HandleID]*contentHandle
	uploads map[catalogproto.MutationRequestID]*pendingMutation
	inbox   map[activationKey][]catalogproto.ActivationNotification
	wake    chan struct{}
}

func newSessionState() *sessionState {
	return &sessionState{
		handles: make(map[catalogproto.HandleID]*contentHandle),
		uploads: make(map[catalogproto.MutationRequestID]*pendingMutation),
		inbox:   make(map[activationKey][]catalogproto.ActivationNotification),
		wake:    make(chan struct{}),
	}
}

// bindSession returns the state this session keys, watching its close signal
// exactly once so pinned content, staged uploads, and the activation inbox die
// with the peer that owns them.
func (s *Server) bindSession(session daemonkit.Session) *sessionState {
	id := session.ID()
	s.sessionMu.Lock()
	state, bound := s.sessions[id]
	if !bound {
		state = newSessionState()
		s.sessions[id] = state
	}
	s.sessionMu.Unlock()
	if bound {
		return state
	}
	if done := session.Done(); done != nil {
		go func() {
			<-done
			s.dropSession(id)
		}()
	}
	return state
}

func (s *Server) dropSession(id uint64) {
	s.sessionMu.Lock()
	state, bound := s.sessions[id]
	delete(s.sessions, id)
	s.sessionMu.Unlock()
	if bound {
		state.release()
	}
}

func (state *sessionState) release() {
	state.mu.Lock()
	state.closed = true
	handles, uploads := state.handles, state.uploads
	state.handles = make(map[catalogproto.HandleID]*contentHandle)
	state.uploads = make(map[catalogproto.MutationRequestID]*pendingMutation)
	state.inbox = make(map[activationKey][]catalogproto.ActivationNotification)
	wake := state.wake
	state.wake = make(chan struct{})
	state.mu.Unlock()
	close(wake)
	for _, handle := range handles {
		_ = handle.settle(context.Background(), errSessionReleased)
	}
	for _, pending := range uploads {
		_ = pending.close()
	}
}

func (state *sessionState) openHandle(handle *contentHandle) (catalogproto.HandleID, error) {
	id, err := mintHandleID()
	if err != nil {
		return "", err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return "", errSessionReleased
	}
	state.handles[id] = handle
	return id, nil
}

func (state *sessionState) handle(id catalogproto.HandleID) (*contentHandle, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	handle, open := state.handles[id]
	if !open {
		return nil, fmt.Errorf("%w: content handle %q is not open", catalog.ErrNotFound, id)
	}
	return handle, nil
}

func (state *sessionState) takeHandle(id catalogproto.HandleID) (*contentHandle, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	handle, open := state.handles[id]
	if !open {
		return nil, fmt.Errorf("%w: content handle %q is not open", catalog.ErrNotFound, id)
	}
	delete(state.handles, id)
	return handle, nil
}

// beginUpload declares one content mutation, restarting from scratch when the
// request id is already begun: a retried begin must be able to re-stage after
// a lost response rather than wedge on its own stranded upload.
func (state *sessionState) beginUpload(pending *pendingMutation) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errSessionReleased
	}
	if stale, begun := state.uploads[pending.input.RequestID]; begun {
		delete(state.uploads, pending.input.RequestID)
		if err := stale.close(); err != nil {
			return err
		}
	}
	state.uploads[pending.input.RequestID] = pending
	return nil
}

// stageChunk appends one sequenced chunk to a begun upload under the session
// lock, so racing chunks serialize exactly as the withdrawn stream did.
func (state *sessionState) stageChunk(id catalogproto.MutationRequestID, sequence uint32, payload []byte) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return errSessionReleased
	}
	pending, begun := state.uploads[id]
	if !begun {
		return fmt.Errorf("%w: mutation %q is not begun", catalog.ErrNotFound, id)
	}
	return pending.upload.write(sequence, payload)
}

func (state *sessionState) takeUpload(id catalogproto.MutationRequestID) (*pendingMutation, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	pending, begun := state.uploads[id]
	if !begun {
		return nil, fmt.Errorf("%w: mutation %q is not begun", catalog.ErrNotFound, id)
	}
	delete(state.uploads, id)
	return pending, nil
}

// contentHandle is one pinned open, read forward by bounded unary pages. The
// underlying stream is sequential, so a page names the offset it continues
// from and a page that skips or repeats bytes is refused rather than served.
// The pin releases only at EOF, catalog.close, or session close, preserving
// the blob-retention contract the withdrawn stream held.
type contentHandle struct {
	mu       sync.Mutex
	read     func([]byte) (int, error)
	close    func(context.Context, error) error
	busy     bool
	consumed uint64
	ended    bool
	settled  bool
	err      error
}

func namespaceContentHandle(content io.ReadCloser) *contentHandle {
	return &contentHandle{
		read:  content.Read,
		close: func(context.Context, error) error { return content.Close() },
	}
}

func privateContentHandle(source contentstream.Source) *contentHandle {
	return &contentHandle{
		read: source.Read,
		close: func(ctx context.Context, cause error) error {
			return settlePrivateOpenSource(ctx, source, cause)
		},
	}
}

// page reads outside the handle lock, so a close arriving while the
// underlying reader blocks can settle the content and unblock it — the unary
// analog of the withdrawn stream dying with its cancelled context.
func (h *contentHandle) page(ctx context.Context, offset uint64, limit uint32) ([]byte, bool, error) {
	h.mu.Lock()
	if h.busy {
		h.mu.Unlock()
		return nil, false, fmt.Errorf("%w: concurrent content read", catalog.ErrIntegrity)
	}
	if h.settled && !h.ended {
		err := h.err
		h.mu.Unlock()
		return nil, false, errors.Join(fmt.Errorf("%w: content handle has settled", catalog.ErrIntegrity), err)
	}
	if offset != h.consumed {
		consumed := h.consumed
		h.mu.Unlock()
		return nil, false, fmt.Errorf("%w: content read offset %d does not continue from %d", catalog.ErrIntegrity, offset, consumed)
	}
	if h.ended {
		h.mu.Unlock()
		return nil, true, nil
	}
	h.busy = true
	h.mu.Unlock()
	page := make([]byte, 0, limit)
	buffer := make([]byte, limit)
	var ended bool
	var pageErr error
	for len(page) < int(limit) {
		count, readErr := h.read(buffer[:int(limit)-len(page)])
		if count > 0 {
			page = append(page, buffer[:count]...)
		}
		if errors.Is(readErr, io.EOF) {
			ended = true
			break
		}
		if readErr != nil {
			pageErr = readErr
			break
		}
		if count == 0 {
			pageErr = errors.New("catalog service: content reader made no progress")
			break
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.busy = false
	if h.settled {
		return nil, false, errors.Join(fmt.Errorf("%w: content handle has settled", catalog.ErrIntegrity), h.err)
	}
	if pageErr != nil {
		return nil, false, errors.Join(pageErr, h.settleLocked(ctx, pageErr))
	}
	h.consumed += uint64(len(page))
	if ended {
		h.ended = true
		if err := h.settleLocked(ctx, nil); err != nil {
			return nil, false, err
		}
	}
	return page, ended, nil
}

// release settles a closed handle: cleanly after EOF, and with an explicit
// cancellation cause before it, so a private source never sees a false claim
// of complete consumption. A deliberate close reports no error either way.
func (h *contentHandle) release(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ended {
		_ = h.settleLocked(ctx, nil)
		return
	}
	_ = h.settleLocked(ctx, errors.New("catalog service: content handle closed before EOF"))
}

func (h *contentHandle) settle(ctx context.Context, cause error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.settleLocked(ctx, cause)
}

func (h *contentHandle) settleLocked(ctx context.Context, cause error) error {
	if h.settled {
		return nil
	}
	h.settled = true
	h.err = cause
	return h.close(ctx, cause)
}

// pendingMutation is one begun content mutation awaiting its commit: the exact
// authorized request the begin admitted, and the body staged chunk by chunk.
type pendingMutation struct {
	input         catalogproto.MutationRequest
	tenant        catalog.TenantID
	generation    catalog.Generation
	identity      Identity
	authorization Authorization
	upload        *mutationUpload
}

func (p *pendingMutation) close() error {
	return p.upload.close()
}

// mutationUpload is one mutation body staged ahead of the commit that consumes
// it: unlinked scratch on disk, so a large body never sits in memory.
type mutationUpload struct {
	file   *os.File
	hasher hash.Hash
	size   int64
	cursor uint32
}

func newMutationUpload() (*mutationUpload, error) {
	file, err := createScratch()
	if err != nil {
		return nil, err
	}
	return &mutationUpload{file: file, hasher: sha256.New()}, nil
}

func (u *mutationUpload) write(sequence uint32, payload []byte) error {
	if sequence != u.cursor+1 {
		return fmt.Errorf("%w: mutation chunk sequence %d does not continue from %d", catalog.ErrIntegrity, sequence, u.cursor)
	}
	if _, err := u.file.WriteAt(payload, u.size); err != nil {
		return err
	}
	u.size += int64(len(payload))
	_, _ = u.hasher.Write(payload)
	u.cursor = sequence
	return nil
}

// source seals the staged body against the exact total and digest the commit
// names, refusing when the staged bytes differ.
func (u *mutationUpload) source(total uint64, digest string) (contentstream.Source, error) {
	staged := hex.EncodeToString(u.hasher.Sum(nil))
	if u.size != int64(total) || staged != digest {
		return nil, fmt.Errorf("%w: staged mutation content total or digest differs", catalog.ErrIntegrity)
	}
	if err := u.file.Sync(); err != nil {
		return nil, err
	}
	return &stagedMutationSource{
		reader: io.NewSectionReader(u.file, 0, u.size), file: u.file, done: make(chan struct{}),
	}, nil
}

func (u *mutationUpload) close() error { return u.file.Close() }

// stagedMutationSource replays one staged mutation body into the catalog stage.
type stagedMutationSource struct {
	reader *io.SectionReader
	file   *os.File

	mu     sync.Mutex
	settle sync.Once
	done   chan struct{}
	err    error
}

func (s *stagedMutationSource) Read(buffer []byte) (int, error) { return s.reader.Read(buffer) }

// Settle releases the scratch file; the abort cause is the caller's own error
// already, so only teardown failures surface here and from Wait.
func (s *stagedMutationSource) Settle(cause error) error {
	s.settle.Do(func() {
		s.mu.Lock()
		s.err = s.file.Close()
		s.mu.Unlock()
		close(s.done)
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *stagedMutationSource) Wait(ctx context.Context) error {
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

var _ contentstream.Source = (*stagedMutationSource)(nil)

func createScratch() (*os.File, error) {
	file, err := os.CreateTemp("", "fusekit-catalog-mutation-")
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

func mintHandleID() (catalogproto.HandleID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return catalogproto.HandleID(hex.EncodeToString(raw[:])), nil
}

func mintBrokerInstanceID() (catalogproto.BrokerInstanceID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return catalogproto.BrokerInstanceID(hex.EncodeToString(raw[:])), nil
}
