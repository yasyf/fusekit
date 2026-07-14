package fileproviderd

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// shortSockDir returns a short /tmp socket dir; t.TempDir()'s long path would
// exceed the macOS 104-byte sun_path limit.
func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-fpd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// fakeApp is a goroutine-backed stand-in for the signed File Provider companion
// app, answering each newline-JSON control Request with a scripted Response.
type fakeApp struct {
	socket string
	ln     net.Listener

	mu       sync.Mutex
	requests []Request
	// respond maps an op to its canned response; a missing op replies unknown-op.
	respond map[Op]Response
	// register, when non-nil, overrides respond[OpRegister] with a per-domain path.
	register func(domain string) Response
	// path, when non-nil, overrides respond[OpPath] per domain (so a test can script
	// a per-call/counting Path verdict).
	path func(domain string) Response
	// probe, when non-nil, overrides respond[OpProbeDomain] per domain (so a test
	// can script a per-call/counting probe-domain verdict).
	probe func(domain string) Response
	// probeShallow, when non-nil, answers a SHALLOW probe-domain (req.Shallow) per
	// domain. Nil models an OLD app that ignores the flag and answers a deep probe:
	// a shallow request then falls through to probe/respond[OpProbeDomain].
	probeShallow func(domain string) Response
	// prepare, when non-nil, answers prepare-domain per domain. Nil models an old
	// app whose unknown-op default arm fires.
	prepare func(domain string) Response
}

// startFakeApp binds a fake companion app on a short socket; responses may be
// scripted before or concurrently with driving a client.
func startFakeApp(t *testing.T) *fakeApp {
	t.Helper()
	socket := filepath.Join(shortSockDir(t), "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	a := &fakeApp{socket: socket, ln: ln, respond: map[Op]Response{}}
	t.Cleanup(func() { ln.Close() })
	go a.serve()
	return a
}

func (a *fakeApp) serve() {
	for {
		conn, err := a.ln.Accept()
		if err != nil {
			return
		}
		go a.handle(conn)
	}
}

func (a *fakeApp) handle(conn net.Conn) {
	defer conn.Close()
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	var req Request
	if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &req); err != nil {
		return
	}
	a.mu.Lock()
	a.requests = append(a.requests, req)
	reg, pth, prb := a.register, a.path, a.probe
	psh, prep := a.probeShallow, a.prepare
	resp, ok := a.respond[req.Op]
	a.mu.Unlock()

	var out Response
	switch {
	case req.Op == OpRegister && reg != nil:
		out = reg(req.Domain)
	case req.Op == OpPath && pth != nil:
		out = pth(req.Domain)
	case req.Op == OpProbeDomain && req.Shallow && psh != nil:
		out = psh(req.Domain)
	case req.Op == OpProbeDomain && prb != nil:
		out = prb(req.Domain)
	case req.Op == OpPrepareDomain && prep != nil:
		out = prep(req.Domain)
	case ok:
		out = resp
	default:
		out = Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
	out.Proto = ControlProtoVersion
	_ = json.NewEncoder(conn).Encode(out)
}

func (a *fakeApp) setResponse(op Op, resp Response) {
	a.mu.Lock()
	a.respond[op] = resp
	a.mu.Unlock()
}

func (a *fakeApp) setRegister(fn func(domain string) Response) {
	a.mu.Lock()
	a.register = fn
	a.mu.Unlock()
}

func (a *fakeApp) setPath(fn func(domain string) Response) {
	a.mu.Lock()
	a.path = fn
	a.mu.Unlock()
}

func (a *fakeApp) setProbe(fn func(domain string) Response) {
	a.mu.Lock()
	a.probe = fn
	a.mu.Unlock()
}

func (a *fakeApp) setProbeShallow(fn func(domain string) Response) {
	a.mu.Lock()
	a.probeShallow = fn
	a.mu.Unlock()
}

func (a *fakeApp) setPrepare(fn func(domain string) Response) {
	a.mu.Lock()
	a.prepare = fn
	a.mu.Unlock()
}

func (a *fakeApp) seen() []Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Request(nil), a.requests...)
}

// int64ptr returns a pointer to n, for scripting Response.JSONBytes.
func int64ptr(n int64) *int64 { return &n }

// boolptr returns a pointer to b, for scripting Response.Listed.
func boolptr(b bool) *bool { return &b }

func readLine(conn net.Conn) (string, error) {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}
