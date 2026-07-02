package content

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// BridgeProtoVersion stamps every bridge request and response; frozen, additive-only.
const BridgeProtoVersion = 1

// BridgeOp is a bridge (data-socket) request operation.
type BridgeOp string

const (
	// BridgeOpManifest requests the domain's top-level Entry list.
	BridgeOpManifest BridgeOp = "manifest"
	// BridgeOpRead requests the computed bytes for a synth entry.
	BridgeOpRead BridgeOp = "read"
	// BridgeOpWrite persists a write to a synth entry.
	BridgeOpWrite BridgeOp = "write"
	// BridgeOpClassify reports a name's serving kind.
	BridgeOpClassify BridgeOp = "classify"

	// Tree ops serve a fully-remote consumer whose Source also implements Tree.
	BridgeOpStat     BridgeOp = "stat"
	BridgeOpList     BridgeOp = "list"
	BridgeOpReadAt   BridgeOp = "readat"
	BridgeOpReadlink BridgeOp = "readlink"

	// Write ops serve a consumer whose Source also implements WritableTree.
	BridgeOpCreate   BridgeOp = "create"
	BridgeOpWriteAt  BridgeOp = "writeat"
	BridgeOpTruncate BridgeOp = "truncate"
	BridgeOpUnlink   BridgeOp = "unlink"
	BridgeOpRename   BridgeOp = "rename"
	BridgeOpMkdir    BridgeOp = "mkdir"

	// Handle ops serve per-open snapshot tokens for a HandleTree consumer;
	// readat/writeat/truncate carry the token in BridgeRequest.Token.
	BridgeOpOpen       BridgeOp = "open"
	BridgeOpFlush      BridgeOp = "flush"
	BridgeOpRelease    BridgeOp = "release"
	BridgeOpReleaseAll BridgeOp = "releaseall"
)

// Bridge error classes: a transient class is retried, a deterministic one is
// permanent, and the errno classes map a failure to a fuse reply.
const (
	ClassNotFound      = "not-found" // ENOENT
	ClassInvalid       = "invalid"   // EINVAL
	ClassPerm          = "perm"      // EPERM
	ClassTransient     = "transient" // EIO
	ClassDeterministic = "deterministic"
	// ClassUnsupported means the server knows the op but its Source does not
	// implement the surface it needs (Tree, WritableTree, HandleTree) — e.g. a
	// write against a read-only tenant maps to EROFS. Distinct from the
	// class-less unknown-op reply, which signals version skew.
	ClassUnsupported = "unsupported"
)

// BridgeRequest is one data-socket request: one newline-delimited JSON object,
// one request and one response per connection. Data carries the BridgeOpWrite
// payload (base64 via Go's []byte JSON encoding).
type BridgeRequest struct {
	Proto  int      `json:"proto"`
	Op     BridgeOp `json:"op"`
	Domain string   `json:"domain,omitempty"`
	Name   string   `json:"name,omitempty"`
	Data   []byte   `json:"data,omitempty"`
	Ofst   int64    `json:"ofst,omitempty"`   // readat, writeat
	Size   int      `json:"size,omitempty"`   // readat
	To     string   `json:"to,omitempty"`     // rename target
	Length int64    `json:"length,omitempty"` // truncate size
	Token  string   `json:"token,omitempty"`  // handle token: flush/release, and readat/writeat/truncate when handle-scoped
}

// BridgeResponse is one data-socket reply: one JSON object per line. ErrClass is
// empty for a Source-only consumer, set by a Tree consumer.
type BridgeResponse struct {
	Proto    int     `json:"proto"`
	OK       bool    `json:"ok"`
	Error    string  `json:"error,omitempty"`
	ErrClass string  `json:"err_class,omitempty"`
	Version  string  `json:"version,omitempty"` // hello/version handshake (unused by ops; reserved)
	Entries  []Entry `json:"entries,omitempty"` // manifest, list
	Kind     string  `json:"kind,omitempty"`    // classify
	Data     []byte  `json:"data,omitempty"`    // read, readat
	Item     *Entry  `json:"item,omitempty"`    // stat; open (the snapshot's entry)
	Target   string  `json:"target,omitempty"`  // readlink
	Token    string  `json:"token,omitempty"`   // open
}

// BridgeServer binds the data socket and dispatches the content ops to a
// consumer-injected Source (and the Tree ops when it also implements Tree).
// fusekit owns this responder; the consumer owns the bytes. Its clients are the
// sandboxed File Provider extension and the fuse holder.
type BridgeServer struct {
	// Socket is the data socket path. Required.
	Socket string
	// Source supplies the content. Required.
	Source Source
	// Version is the consumer's version, logged at startup. It must never be
	// fusekit's module version — a consumer comparing the holder's wire version to
	// its own would loop forever.
	Version string
	// Log receives per-op outcomes. nil defaults to stderr.
	Log *log.Logger

	wg sync.WaitGroup
}

// Run binds the data socket and serves until ctx is cancelled, then stops
// accepting and drains in-flight handlers. It fails loudly when Source is nil.
func (s *BridgeServer) Run(ctx context.Context) error {
	if s.Source == nil {
		return errors.New("content: BridgeServer.Run requires a ContentSource")
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[fusekit-bridge] ", log.LstdFlags)
	}

	ln, lock, err := proc.SingleEntrant{
		Socket: s.Socket,
		// Refuse to bind over a live peer; rebind a stale socket. A single daemon
		// owns the bridge (no version-skew eviction), so Evict just dials the
		// socket to detect a live peer.
		Evict: func() (bool, error) {
			conn, derr := net.DialTimeout("unix", s.Socket, bridgeDialTimeout)
			if derr != nil {
				return false, nil
			}
			conn.Close()
			return false, fmt.Errorf("a bridge server already serves %s; refusing to start", s.Socket)
		},
	}.Listen()
	if err != nil {
		return fmt.Errorf("bind bridge socket: %w", err)
	}
	defer lock.Close()
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = ln.Close() }) }
	defer closeListener()

	go func() {
		<-ctx.Done()
		closeListener()
	}()

	s.Log.Printf("bridge %s started; socket=%s", s.Version, s.Socket)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			s.Log.Printf("accept: %v", err)
			continue
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.handle(conn) }()
	}
	s.wg.Wait()
	s.Log.Printf("bridge stopped")
	return nil
}

// bridgeHandleTimeout bounds one bridge exchange so a stuck or half-open peer
// can never park a handler goroutine past ctx cancel, hanging Run's wg.Wait (and
// the consumer's own shutdown Wait).
const bridgeHandleTimeout = 10 * time.Second

func (s *BridgeServer) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(bridgeHandleTimeout))
	var req BridgeRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		s.writeResp(conn, BridgeResponse{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	s.writeResp(conn, s.dispatch(req))
}

func (s *BridgeServer) writeResp(conn net.Conn, r BridgeResponse) {
	r.Proto = BridgeProtoVersion
	_ = json.NewEncoder(conn).Encode(r)
}

func (s *BridgeServer) dispatch(req BridgeRequest) BridgeResponse {
	switch req.Op {
	case BridgeOpManifest:
		return s.handleManifest(req)
	case BridgeOpRead:
		return s.handleRead(req)
	case BridgeOpWrite:
		return s.handleWrite(req)
	case BridgeOpClassify:
		return s.handleClassify(req)
	case BridgeOpStat:
		return s.handleStat(req)
	case BridgeOpList:
		return s.handleList(req)
	case BridgeOpReadAt:
		return s.handleReadAt(req)
	case BridgeOpReadlink:
		return s.handleReadlink(req)
	case BridgeOpCreate:
		return s.handleCreate(req)
	case BridgeOpWriteAt:
		return s.handleWriteAt(req)
	case BridgeOpTruncate:
		return s.handleTruncate(req)
	case BridgeOpUnlink:
		return s.handleUnlink(req)
	case BridgeOpRename:
		return s.handleRename(req)
	case BridgeOpMkdir:
		return s.handleMkdir(req)
	case BridgeOpOpen:
		return s.handleOpen(req)
	case BridgeOpFlush:
		return s.handleFlush(req)
	case BridgeOpRelease:
		return s.handleRelease(req)
	case BridgeOpReleaseAll:
		return s.handleReleaseAll(req)
	default:
		// No ErrClass: an unknown op signals version skew (an op minted after
		// this server shipped), never a capability verdict. IsUnsupported
		// matches this reply for old servers.
		return BridgeResponse{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

// unsupported is the capability-miss reply: the op is known, the Source just
// does not implement the surface it needs.
func unsupported(op BridgeOp, surface string) *BridgeResponse {
	return &BridgeResponse{
		OK:       false,
		Error:    fmt.Sprintf("%s: source does not implement %s", op, surface),
		ErrClass: ClassUnsupported,
	}
}

// tree returns the Source as a Tree, or a ClassUnsupported miss when the
// consumer is Source-only.
func (s *BridgeServer) tree(op BridgeOp) (Tree, *BridgeResponse) {
	if t, ok := s.Source.(Tree); ok {
		return t, nil
	}
	return nil, unsupported(op, "Tree")
}

// writableTree returns the Source as a WritableTree, or a ClassUnsupported miss
// — a read-only tenant answers every write op this way, never a panic.
func (s *BridgeServer) writableTree(op BridgeOp) (WritableTree, *BridgeResponse) {
	if t, ok := s.Source.(WritableTree); ok {
		return t, nil
	}
	return nil, unsupported(op, "WritableTree")
}

// handleTree returns the Source as a HandleTree, or a ClassUnsupported miss —
// the reply that tells the holder to stay stateless (no tokens).
func (s *BridgeServer) handleTree(op BridgeOp) (HandleTree, *BridgeResponse) {
	if t, ok := s.Source.(HandleTree); ok {
		return t, nil
	}
	return nil, unsupported(op, "HandleTree")
}

// IsUnsupported reports whether err is the bridge's verdict that the serving
// source cannot answer the op: a ClassUnsupported reply from a current server,
// or the class-less unknown-op reply an older BridgeServer gives for an op
// minted after it shipped. Both read as "capability absent", so a caller
// degrades the same way (read-only, tokenless) for either vintage.
func IsUnsupported(err error) bool {
	var ce ClassedError
	if errors.As(err, &ce) && ce.Class() == ClassUnsupported {
		return true
	}
	return err != nil && strings.Contains(err.Error(), "unknown op:")
}

func (s *BridgeServer) handleManifest(req BridgeRequest) BridgeResponse {
	if req.Domain == "" {
		return BridgeResponse{OK: false, Error: "manifest: domain is required"}
	}
	entries, err := s.Source.Manifest(req.Domain)
	if err != nil {
		s.Log.Printf("manifest %s: %v", req.Domain, err)
		return BridgeResponse{OK: false, Error: err.Error()}
	}
	return BridgeResponse{OK: true, Entries: entries}
}

func (s *BridgeServer) handleRead(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "read: domain and name are required"}
	}
	data, err := s.Source.ReadSynth(req.Domain, req.Name)
	if err != nil {
		s.Log.Printf("read %s/%s: %v", req.Domain, req.Name, err)
		return BridgeResponse{OK: false, Error: err.Error()}
	}
	return BridgeResponse{OK: true, Data: data}
}

func (s *BridgeServer) handleWrite(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "write: domain and name are required"}
	}
	if err := s.Source.WriteThrough(req.Domain, req.Name, req.Data); err != nil {
		s.Log.Printf("write %s/%s: %v", req.Domain, req.Name, err)
		return BridgeResponse{OK: false, Error: err.Error()}
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleClassify(req BridgeRequest) BridgeResponse {
	if req.Name == "" {
		return BridgeResponse{OK: false, Error: "classify: name is required"}
	}
	return BridgeResponse{OK: true, Kind: string(s.Source.Classify(req.Name))}
}

func (s *BridgeServer) handleStat(req BridgeRequest) BridgeResponse {
	t, bad := s.tree(req.Op)
	if bad != nil {
		return *bad
	}
	e, err := t.Stat(req.Domain, req.Name)
	if err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true, Item: &e}
}

func (s *BridgeServer) handleList(req BridgeRequest) BridgeResponse {
	t, bad := s.tree(req.Op)
	if bad != nil {
		return *bad
	}
	entries, err := t.List(req.Domain, req.Name)
	if err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true, Entries: entries}
}

func (s *BridgeServer) handleReadAt(req BridgeRequest) BridgeResponse {
	if req.Token != "" {
		h, bad := s.handleTree(req.Op)
		if bad != nil {
			return *bad
		}
		data, err := h.ReadAtHandle(req.Domain, req.Name, req.Token, req.Ofst, req.Size)
		if err != nil {
			return errResp(err)
		}
		return BridgeResponse{OK: true, Data: data}
	}
	t, bad := s.tree(req.Op)
	if bad != nil {
		return *bad
	}
	data, err := t.ReadAt(req.Domain, req.Name, req.Ofst, req.Size)
	if err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true, Data: data}
}

func (s *BridgeServer) handleReadlink(req BridgeRequest) BridgeResponse {
	t, bad := s.tree(req.Op)
	if bad != nil {
		return *bad
	}
	target, err := t.Readlink(req.Domain, req.Name)
	if err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true, Target: target}
}

func (s *BridgeServer) handleCreate(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "create: domain and name are required"}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.Create(req.Domain, req.Name); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleWriteAt(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "writeat: domain and name are required"}
	}
	if req.Token != "" {
		h, bad := s.handleTree(req.Op)
		if bad != nil {
			return *bad
		}
		if err := h.WriteAtHandle(req.Domain, req.Name, req.Token, req.Ofst, req.Data); err != nil {
			return errResp(err)
		}
		return BridgeResponse{OK: true}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.WriteAt(req.Domain, req.Name, req.Ofst, req.Data); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleTruncate(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "truncate: domain and name are required"}
	}
	if req.Token != "" {
		h, bad := s.handleTree(req.Op)
		if bad != nil {
			return *bad
		}
		if err := h.TruncateHandle(req.Domain, req.Name, req.Token, req.Length); err != nil {
			return errResp(err)
		}
		return BridgeResponse{OK: true}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.Truncate(req.Domain, req.Name, req.Length); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleUnlink(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "unlink: domain and name are required"}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.Unlink(req.Domain, req.Name); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleRename(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" || req.To == "" {
		return BridgeResponse{OK: false, Error: "rename: domain, name, and to are required"}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.Rename(req.Domain, req.Name, req.To); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleMkdir(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "mkdir: domain and name are required"}
	}
	t, bad := s.writableTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := t.Mkdir(req.Domain, req.Name); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleOpen(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" {
		return BridgeResponse{OK: false, Error: "open: domain and name are required"}
	}
	h, bad := s.handleTree(req.Op)
	if bad != nil {
		return *bad
	}
	token, e, err := h.OpenHandle(req.Domain, req.Name)
	if err != nil {
		return errResp(err)
	}
	if token == "" {
		// An empty token would alias every tokenless request; refuse the
		// contract violation here rather than corrupt routing later.
		return BridgeResponse{OK: false, Error: fmt.Sprintf("open %s/%s: consumer returned an empty token", req.Domain, req.Name)}
	}
	return BridgeResponse{OK: true, Token: token, Item: &e}
}

func (s *BridgeServer) handleFlush(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" || req.Token == "" {
		return BridgeResponse{OK: false, Error: "flush: domain, name, and token are required"}
	}
	h, bad := s.handleTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := h.FlushHandle(req.Domain, req.Name, req.Token); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleRelease(req BridgeRequest) BridgeResponse {
	if req.Domain == "" || req.Name == "" || req.Token == "" {
		return BridgeResponse{OK: false, Error: "release: domain, name, and token are required"}
	}
	h, bad := s.handleTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := h.ReleaseHandle(req.Domain, req.Name, req.Token); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

func (s *BridgeServer) handleReleaseAll(req BridgeRequest) BridgeResponse {
	if req.Domain == "" {
		return BridgeResponse{OK: false, Error: "releaseall: domain is required"}
	}
	h, bad := s.handleTree(req.Op)
	if bad != nil {
		return *bad
	}
	if err := h.ReleaseAllHandles(req.Domain); err != nil {
		return errResp(err)
	}
	return BridgeResponse{OK: true}
}

// ClassedError is an error carrying a bridge error class (a Class* constant) so a
// failure's class crosses the wire.
type ClassedError interface {
	error
	Class() string
}

func errResp(err error) BridgeResponse {
	r := BridgeResponse{OK: false, Error: err.Error()}
	var ce ClassedError
	if errors.As(err, &ce) {
		r.ErrClass = ce.Class()
	}
	return r
}
