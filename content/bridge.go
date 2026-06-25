package content

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/yasyf/fusekit/proc"
)

// BridgeProtoVersion stamps every bridge (data-socket) request and response.
// Frozen and additive-only.
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

	// The Tree ops serve a fully-remote consumer whose Source also implements
	// Tree; a Source-only server answers them with an unknown-op error.
	BridgeOpStat     BridgeOp = "stat"
	BridgeOpList     BridgeOp = "list"
	BridgeOpReadAt   BridgeOp = "readat"
	BridgeOpReadlink BridgeOp = "readlink"
)

// Bridge error classes mirror mountd's: a transient class is retried, a
// deterministic one is permanent, and the errno classes map a failure to a fuse
// reply. A Source-only consumer leaves ErrClass empty (a plain error).
const (
	ClassNotFound      = "not-found"     // ENOENT
	ClassInvalid       = "invalid"       // EINVAL
	ClassPerm          = "perm"          // EPERM
	ClassTransient     = "transient"     // retryable (consumer mid-flight); EIO + retry
	ClassDeterministic = "deterministic" // permanent failure; do not retry
)

// BridgeRequest is one data-socket request: one JSON object, newline-delimited,
// one request and one response per connection. Data carries the write payload
// for BridgeOpWrite (base64 via Go's []byte JSON encoding).
type BridgeRequest struct {
	Proto  int      `json:"proto"`
	Op     BridgeOp `json:"op"`
	Domain string   `json:"domain,omitempty"`
	Name   string   `json:"name,omitempty"`
	Data   []byte   `json:"data,omitempty"`
	Ofst   int64    `json:"ofst,omitempty"` // readat
	Size   int      `json:"size,omitempty"` // readat
}

// BridgeResponse is one data-socket reply: one JSON object per line. ErrClass is
// empty for a Source-only consumer (the File Provider wire carries no classes);
// a Tree consumer sets it so the holder maps a failure to a fuse reply.
type BridgeResponse struct {
	Proto    int     `json:"proto"`
	OK       bool    `json:"ok"`
	Error    string  `json:"error,omitempty"`
	ErrClass string  `json:"err_class,omitempty"`
	Version  string  `json:"version,omitempty"` // hello/version handshake (unused by ops; reserved)
	Entries  []Entry `json:"entries,omitempty"` // manifest, list
	Kind     string  `json:"kind,omitempty"`    // classify
	Data     []byte  `json:"data,omitempty"`    // read, readat
	Item     *Entry  `json:"item,omitempty"`    // stat
	Target   string  `json:"target,omitempty"`  // readlink
}

// BridgeServer binds the data socket and dispatches the content ops to a
// consumer-injected Source (and, when it also implements Tree, the Tree ops).
// fusekit owns this responder; the consumer owns the bytes. The clients are the
// sandboxed File Provider extension and the fuse holder. A stale socket is
// rebound, a live peer refused.
type BridgeServer struct {
	// Socket is the data socket path. Required.
	Socket string
	// Source supplies the content. Required; Run fails loudly without it.
	Source Source
	// Version is the consumer's version, logged at startup. It must never be
	// fusekit's module version — a consumer comparing the holder's wire version
	// to its own would loop forever if fusekit's leaked onto the wire.
	Version string
	// Log receives per-op outcomes. nil defaults to stderr.
	Log *log.Logger

	wg sync.WaitGroup
}

// Run binds the data socket and serves until ctx is cancelled. On the way out it
// stops accepting and drains in-flight handlers. It fails loudly and immediately
// when Source is nil — a bridge with no content source can serve nothing.
func (s *BridgeServer) Run(ctx context.Context) error {
	if s.Source == nil {
		return errors.New("content: BridgeServer.Run requires a ContentSource")
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[fusekit-bridge] ", log.LstdFlags)
	}

	ln, lock, err := proc.SingleEntrant{
		Socket: s.Socket,
		// Refuse to bind over a live peer (another daemon already serving the
		// bridge); a stale socket with no peer is rebound. The bridge has no
		// version-skew eviction — a single daemon owns it — so Evict always
		// reports "no live peer to evict" by dialing the socket itself.
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

// handle serves one connection: one request, one response.
func (s *BridgeServer) handle(conn net.Conn) {
	defer conn.Close()
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
	default:
		return BridgeResponse{OK: false, Error: "unknown op: " + string(req.Op)}
	}
}

// tree returns the Source as a Tree, or a not-OK response when the consumer is
// Source-only (the holder never sends Tree ops to such a consumer, so this is a
// wire-contract violation, reported as unknown op).
func (s *BridgeServer) tree(op BridgeOp) (Tree, *BridgeResponse) {
	if t, ok := s.Source.(Tree); ok {
		return t, nil
	}
	return nil, &BridgeResponse{OK: false, Error: "unknown op: " + string(op)}
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

// ClassedError is a Tree error that carries a bridge error class (one of the
// Class* constants), so a deterministic failure crosses the wire distinct from a
// transient one.
type ClassedError interface {
	error
	Class() string
}

// errResp maps a Tree error to a not-OK response, lifting its class when it has
// one.
func errResp(err error) BridgeResponse {
	r := BridgeResponse{OK: false, Error: err.Error()}
	var ce ClassedError
	if errors.As(err, &ce) {
		r.ErrClass = ce.Class()
	}
	return r
}
