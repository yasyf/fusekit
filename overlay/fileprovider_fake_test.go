package overlay

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/fileproviderd"
)

// fakeFPApp is a goroutine-backed stand-in for the signed File Provider companion
// app: it binds the control socket and answers each newline-JSON control Request
// with a scripted Response. Binding synchronously before any provider call makes
// fileproviderd's spawn short-circuit on the live socket, never launching a real
// bundle.
type fakeFPApp struct {
	socket string
	ln     net.Listener

	mu       sync.Mutex
	requests []fileproviderd.Request
	// respond maps an op to its canned response; a missing op replies unknown-op.
	respond map[fileproviderd.Op]fileproviderd.Response
	// register, when non-nil, overrides respond[OpRegister] with a per-domain root.
	register func(domain string) fileproviderd.Response
	// path, when non-nil, overrides respond[OpPath] per domain (Health/State).
	path func(domain string) fileproviderd.Response
}

// startFakeFPApp binds a fake companion app on a short socket and returns it. The
// socket lives under a short /tmp dir to dodge the macOS 104-byte sun_path limit a
// long t.TempDir path would blow.
func startFakeFPApp(t *testing.T) *fakeFPApp {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-ov-fp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	a := &fakeFPApp{socket: socket, ln: ln, respond: map[fileproviderd.Op]fileproviderd.Response{}}
	t.Cleanup(func() { ln.Close() })
	go a.serve()
	return a
}

func (a *fakeFPApp) serve() {
	for {
		conn, err := a.ln.Accept()
		if err != nil {
			return
		}
		go a.handle(conn)
	}
}

func (a *fakeFPApp) handle(conn net.Conn) {
	defer conn.Close()
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	var req fileproviderd.Request
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &req); err != nil {
		return
	}
	a.mu.Lock()
	a.requests = append(a.requests, req)
	reg, pth := a.register, a.path
	resp, ok := a.respond[req.Op]
	a.mu.Unlock()

	var out fileproviderd.Response
	switch {
	case req.Op == fileproviderd.OpRegister && reg != nil:
		out = reg(req.Domain)
	case req.Op == fileproviderd.OpPath && pth != nil:
		out = pth(req.Domain)
	case ok:
		out = resp
	default:
		out = fileproviderd.Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
	out.Proto = fileproviderd.ControlProtoVersion
	_ = json.NewEncoder(conn).Encode(out)
}

func (a *fakeFPApp) setResponse(op fileproviderd.Op, resp fileproviderd.Response) {
	a.mu.Lock()
	a.respond[op] = resp
	a.mu.Unlock()
}

func (a *fakeFPApp) setRegister(fn func(domain string) fileproviderd.Response) {
	a.mu.Lock()
	a.register = fn
	a.mu.Unlock()
}

func (a *fakeFPApp) setPath(fn func(domain string) fileproviderd.Response) {
	a.mu.Lock()
	a.path = fn
	a.mu.Unlock()
}

func (a *fakeFPApp) ops() []fileproviderd.Op {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]fileproviderd.Op, len(a.requests))
	for i, r := range a.requests {
		out[i] = r.Op
	}
	return out
}

// fpSpecFor builds a FileProviderSpec pointed at the fake's control socket. AppPath
// must be non-empty for the spawn arm (unused here — the live socket wins); the
// bundle id must be non-empty for FileProviderAvailable.
func fpSpecFor(a *fakeFPApp) *FileProviderSpec {
	return &FileProviderSpec{
		AppPath:           "/Apps/CCPoolStatus.app",
		ControlSocket:     a.socket,
		BridgeSocket:      filepath.Join(filepath.Dir(a.socket), "bridge.sock"),
		ExtensionBundleID: "com.example.fp.extension",
		AppGroup:          "group.com.example.status",
	}
}

// withFileProviderEnabled pins FileProviderAvailable's enabled-check to a fixed
// answer (restored on cleanup), so Select's FP arm runs without a real extension
// or a pluginkit shell-out.
func withFileProviderEnabled(t *testing.T, enabled bool) {
	t.Helper()
	prev := fileProviderEnabled
	fileProviderEnabled = func(string) bool { return enabled }
	t.Cleanup(func() { fileProviderEnabled = prev })
}
