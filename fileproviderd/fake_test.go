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

// shortSockDir returns a short /tmp dir for unix sockets, dodging the macOS
// 104-byte sun_path limit that t.TempDir()'s long path would blow.
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
// app: it binds the control socket and answers each newline-JSON Request with a
// scripted Response, exactly as the real Swift host app would. It is the
// File-Provider analog of mountd's startRawHolder, but typed — the wire is
// frozen, so a fake that marshals real Responses both exercises the client's
// decode AND keeps the fake honest about the proto. No Mac, no real domain.
type fakeApp struct {
	socket string
	ln     net.Listener

	mu       sync.Mutex
	requests []Request
	// respond maps an op to its canned response. A missing op replies with an
	// unknown-op error, like the real app would for a proto it predates.
	respond map[Op]Response
	// register, when non-nil, overrides the static respond[OpRegister] so a test
	// can return a per-domain path (e.g. a t.TempDir standing in for the
	// CloudStorage root).
	register func(domain string) Response
}

// startFakeApp binds a fake companion app on a short socket and returns it. The
// caller scripts responses via respond/register before (or concurrently with)
// driving a client; the listener is closed on test cleanup.
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
	reg := a.register
	resp, ok := a.respond[req.Op]
	a.mu.Unlock()

	var out Response
	switch {
	case req.Op == OpRegister && reg != nil:
		out = reg(req.Domain)
	case ok:
		out = resp
	default:
		out = Response{OK: false, Error: "unknown op: " + string(req.Op)}
	}
	out.Proto = ControlProtoVersion
	_ = json.NewEncoder(conn).Encode(out)
}

// setResponse scripts the canned response for one op.
func (a *fakeApp) setResponse(op Op, resp Response) {
	a.mu.Lock()
	a.respond[op] = resp
	a.mu.Unlock()
}

// setRegister scripts a per-domain register handler.
func (a *fakeApp) setRegister(fn func(domain string) Response) {
	a.mu.Lock()
	a.register = fn
	a.mu.Unlock()
}

// seen returns the requests the fake has received so far, in order.
func (a *fakeApp) seen() []Request {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Request(nil), a.requests...)
}

// readLine reads one newline-terminated line, for raw-wire assertions.
func readLine(conn net.Conn) (string, error) {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}
