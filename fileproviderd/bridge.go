package fileproviderd

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

// BridgeProtoVersion stamps every bridge (data-socket) request and response. It
// versions INDEPENDENTLY of ControlProtoVersion because the control socket is
// served by the signed companion app while the data socket is served by the
// consumer's Go process and called by the sandboxed extension — two binaries on
// two ship cadences. Frozen and additive-only, same discipline as the control
// wire.
const BridgeProtoVersion = 1

// EntryKind classifies a top-level domain entry, telling the extension how to
// serve it. The string values are FROZEN wire artifacts.
type EntryKind string

const (
	// EntrySymlink: a shared entry the extension serves by reading the backing
	// file/dir directly (clone-able fetchContents, near-zero cost). The bulk of
	// the tree.
	EntrySymlink EntryKind = "symlink"
	// EntrySynth: a computed entry whose bytes come from Go via ReadSynth (a
	// merged config, an injected settings file). Writes route back through
	// WriteThrough.
	EntrySynth EntryKind = "synth"
	// EntryPrivate: an account-private entry the extension serves from the
	// private store, never linked to the shared base.
	EntryPrivate EntryKind = "private"
)

// Entry is one top-level domain entry in a Manifest: its name, how to serve it
// (Kind), an opaque content-version stamp, and its size. Version is the
// freshness key the extension stamps onto NSFileProviderItemVersion.content so a
// backing change forces a re-fetch; the consumer derives it however it likes
// (an mtime/size hash for shared/private, a merge/settings key for synth) — this
// package treats it as opaque.
type Entry struct {
	Name    string    `json:"name"`
	Kind    EntryKind `json:"kind"`
	Version string    `json:"version"`
	Size    int64     `json:"size,omitempty"`
}

// ContentSource is the consumer-injected content seam: fusekit owns the bridge
// framework (the wire, the dispatch, the socket), the consumer supplies the
// bytes. It holds ALL the consumer specifics this package deliberately does not
// — the merge schema, the private/shared/excluded classification, the version
// strategy. Every method takes the domain identifier so one source serves every
// registered domain.
//
// Implementations must be safe for concurrent calls: the extension fans out
// enumerations and fetches, and BridgeServer dispatches each on its own
// goroutine.
type ContentSource interface {
	// Manifest returns the authoritative top-level classification for the
	// domain — every entry the extension surfaces at the domain root, each with
	// its serving Kind and content-version stamp.
	Manifest(domain string) ([]Entry, error)
	// ReadSynth returns the computed bytes for an EntrySynth entry (e.g. the
	// merged config). name is the entry's Name from the Manifest.
	ReadSynth(domain, name string) ([]byte, error)
	// WriteThrough persists a write to an EntrySynth entry back to the consumer's
	// source of truth (e.g. splitting a merged config into base + private store).
	// It must be idempotent and additive so out-of-order delivery converges.
	WriteThrough(domain, name string, data []byte) error
	// Classify reports how a (possibly new) top-level name should be served,
	// for createItem on a name not yet in the Manifest.
	Classify(name string) EntryKind
}

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
}

// BridgeResponse is one data-socket reply: one JSON object per line. The bridge
// wire carries no error classes — a ContentSource failure is a plain message the
// extension surfaces as an I/O error; there is no retreat decision on this
// socket (that lives on the control wire's ClassNoEntitlement).
type BridgeResponse struct {
	Proto   int     `json:"proto"`
	OK      bool    `json:"ok"`
	Error   string  `json:"error,omitempty"`
	Version string  `json:"version,omitempty"` // hello/version handshake (unused by ops; reserved)
	Entries []Entry `json:"entries,omitempty"` // manifest
	Kind    string  `json:"kind,omitempty"`    // classify
	Data    []byte  `json:"data,omitempty"`    // read
}

// BridgeServer binds the data socket and dispatches Manifest/ReadSynth/
// WriteThrough/Classify to a consumer-injected ContentSource. fusekit owns this
// responder; the consumer (and the sandboxed extension that calls it) owns
// nothing of the wire. It is the data-socket analog of mountd.Server, but its
// payload is content, not mounts, and the SERVER here is the consumer's Go
// process (the extension is the client), so there is no SingleEntrant eviction
// policy to choose — a stale socket is rebound, a live peer refused.
type BridgeServer struct {
	// Socket is the data socket path (typically inside the App-Group container
	// the sandboxed extension can reach). Required.
	Socket string
	// Source supplies the content. Required; Run fails loudly without it.
	Source ContentSource
	// Version is reported in the handshake field and is the consumer's version,
	// for parity with the extension's expectation — never fusekit's module
	// version (a consumer comparing it to its own would loop forever if
	// fusekit's leaked onto the wire).
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
		return errors.New("fileproviderd: BridgeServer.Run requires a ContentSource")
	}
	if s.Log == nil {
		s.Log = log.New(os.Stderr, "[fileproviderd-bridge] ", log.LstdFlags)
	}

	ln, lock, err := proc.SingleEntrant{
		Socket: s.Socket,
		// Refuse to bind over a live peer (another daemon already serving the
		// bridge); a stale socket with no peer is rebound. The bridge has no
		// version-skew eviction — a single daemon owns it — so Evict always
		// reports "no live peer to evict" by dialing the socket itself.
		Evict: func() (bool, error) {
			conn, derr := net.DialTimeout("unix", s.Socket, controlDialTimeout)
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
	default:
		return BridgeResponse{OK: false, Error: "unknown op: " + string(req.Op)}
	}
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
