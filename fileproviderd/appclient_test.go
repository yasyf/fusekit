package fileproviderd

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// TestAppClientRoundTrips drives each AppClient method against the fake app and
// asserts the decoded result and the exact request received (frozen proto-1 wire).
func TestAppClientRoundTrips(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(a *fakeApp)
		invoke  func(t *testing.T, c *AppClient)
		wantReq Request
	}{
		{
			name:    "health returns the app version",
			setup:   func(a *fakeApp) { a.setResponse(OpHealth, Response{OK: true, Version: "v9.8.7"}) },
			wantReq: Request{Proto: 1, Op: OpHealth},
			invoke: func(t *testing.T, c *AppClient) {
				v, err := c.Health(context.Background())
				if err != nil || v != "v9.8.7" {
					t.Fatalf("Health = %q, %v; want the app version", v, err)
				}
			},
		},
		{
			name: "register returns the domain root",
			setup: func(a *fakeApp) {
				a.setRegister(func(domain string) Response { return Response{OK: true, Path: "/cloud/" + domain} })
			},
			wantReq: Request{Proto: 1, Op: OpRegister, Domain: "acct-01"},
			invoke: func(t *testing.T, c *AppClient) {
				p, err := c.Register(context.Background(), "acct-01")
				if err != nil || p != "/cloud/acct-01" {
					t.Fatalf("Register = %q, %v; want /cloud/acct-01", p, err)
				}
			},
		},
		{
			name:    "path returns the domain root without re-registering",
			setup:   func(a *fakeApp) { a.setResponse(OpPath, Response{OK: true, Path: "/cloud/acct-02"}) },
			wantReq: Request{Proto: 1, Op: OpPath, Domain: "acct-02"},
			invoke: func(t *testing.T, c *AppClient) {
				p, err := c.Path(context.Background(), "acct-02")
				if err != nil || p != "/cloud/acct-02" {
					t.Fatalf("Path = %q, %v; want /cloud/acct-02", p, err)
				}
			},
		},
		{
			name:    "signal succeeds",
			setup:   func(a *fakeApp) { a.setResponse(OpSignal, Response{OK: true}) },
			wantReq: Request{Proto: 1, Op: OpSignal, Domain: "acct-03"},
			invoke: func(t *testing.T, c *AppClient) {
				if err := c.Signal(context.Background(), "acct-03"); err != nil {
					t.Fatalf("Signal = %v, want nil", err)
				}
			},
		},
		{
			name:    "remove succeeds",
			setup:   func(a *fakeApp) { a.setResponse(OpRemove, Response{OK: true}) },
			wantReq: Request{Proto: 1, Op: OpRemove, Domain: "acct-04"},
			invoke: func(t *testing.T, c *AppClient) {
				if err := c.Remove(context.Background(), "acct-04"); err != nil {
					t.Fatalf("Remove = %v, want nil", err)
				}
			},
		},
		{
			name:    "probe true",
			setup:   func(a *fakeApp) { a.setResponse(OpProbe, Response{OK: true, FPOK: true}) },
			wantReq: Request{Proto: 1, Op: OpProbe},
			invoke: func(t *testing.T, c *AppClient) {
				ok, err := c.Probe(context.Background())
				if err != nil || !ok {
					t.Fatalf("Probe = %v, %v; want true", ok, err)
				}
			},
		},
		{
			name:    "probe-domain served returns the .claude.json byte count",
			setup:   func(a *fakeApp) { a.setResponse(OpProbeDomain, Response{OK: true, JSONBytes: int64ptr(99)}) },
			wantReq: Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-05"},
			invoke: func(t *testing.T, c *AppClient) {
				v, err := c.ProbeDomain(context.Background(), "acct-05")
				if err != nil || v == nil || *v != 99 {
					t.Fatalf("ProbeDomain = %v, %v; want a pointer to 99", v, err)
				}
			},
		},
		{
			name:    "probe-domain empty .claude.json returns a pointer to 0, not nil",
			setup:   func(a *fakeApp) { a.setResponse(OpProbeDomain, Response{OK: true, JSONBytes: int64ptr(0)}) },
			wantReq: Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-06"},
			invoke: func(t *testing.T, c *AppClient) {
				v, err := c.ProbeDomain(context.Background(), "acct-06")
				if err != nil || v == nil || *v != 0 {
					t.Fatalf("ProbeDomain = %v, %v; want a non-nil pointer to 0 (empty, not absent)", v, err)
				}
			},
		},
		{
			name:    "probe-domain absent .claude.json returns a nil byte count",
			setup:   func(a *fakeApp) { a.setResponse(OpProbeDomain, Response{OK: true}) },
			wantReq: Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-07"},
			invoke: func(t *testing.T, c *AppClient) {
				v, err := c.ProbeDomain(context.Background(), "acct-07")
				if err != nil || v != nil {
					t.Fatalf("ProbeDomain = %v, %v; want a nil byte count (domain serves, .claude.json absent)", v, err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			tc.setup(a)
			tc.invoke(t, NewAppClient(a.socket))
			seen := a.seen()
			if len(seen) != 1 {
				t.Fatalf("fake app saw %d requests, want exactly 1: %+v", len(seen), seen)
			}
			if !reflect.DeepEqual(seen[0], tc.wantReq) {
				t.Fatalf("request = %+v, want %+v", seen[0], tc.wantReq)
			}
		})
	}
}

// TestAppClientErrorClasses pins that each wire class maps to its sentinel and,
// load-bearing, that ClassNoEntitlement is the ONLY retreat condition.
func TestAppClientErrorClasses(t *testing.T) {
	tests := []struct {
		name      string
		resp      Response
		wantIs    error
		retreatOK bool
	}{
		{name: "no-entitlement is the retreat", resp: Response{OK: false, ErrClass: ClassNoEntitlement, Error: "enable me"}, wantIs: ErrCannotControl, retreatOK: true},
		{name: "register-failed is transient", resp: Response{OK: false, ErrClass: ClassRegisterFailed, Error: "dup"}, wantIs: ErrRegisterFailed},
		{name: "busy is transient", resp: Response{OK: false, ErrClass: ClassBusy, Error: "inflight"}, wantIs: ErrBusy},
		{name: "unknown class is transient, never retreat", resp: Response{OK: false, ErrClass: "future", Error: "?"}, wantIs: ErrAppUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			a.setResponse(OpRegister, tc.resp)
			_, err := NewAppClient(a.socket).Register(context.Background(), "acct-01")
			if err == nil {
				t.Fatal("Register succeeded, want an error")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Errorf("err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if got := errors.Is(err, ErrCannotControl); got != tc.retreatOK {
				t.Errorf("errors.Is ErrCannotControl = %v, want %v (only no-entitlement retreats)", got, tc.retreatOK)
			}
		})
	}
}

// TestAppClientProbeDomainErrorClasses pins probe-domain's failure mapping: the
// served/missing/empty verdicts round-trip above; here the wire failure classes
// route to their sentinels, an app too old to know the op (its unknown-op default
// arm: ok:false, EMPTY err_class) becomes ErrOpUnsupported — distinct from BOTH the
// transient blip and the retreat — and an unknown class stays transient.
func TestAppClientProbeDomainErrorClasses(t *testing.T) {
	tests := []struct {
		name   string
		resp   Response
		script bool // false: leave probe-domain unscripted so the fake's unknown-op arm answers
		wantIs error
	}{
		{name: "unregistered domain is ErrNoDomain", resp: Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"}, script: true, wantIs: ErrNoDomain},
		{name: "not-serving is ErrDomainNotServing", resp: Response{OK: false, ErrClass: ClassDomainNotServing, Error: "materializing"}, script: true, wantIs: ErrDomainNotServing},
		{name: "busy is ErrBusy", resp: Response{OK: false, ErrClass: ClassBusy, Error: "inflight"}, script: true, wantIs: ErrBusy},
		{name: "old app unknown-op arm is ErrOpUnsupported", script: false, wantIs: ErrOpUnsupported},
		{name: "unknown class stays transient, never retreat", resp: Response{OK: false, ErrClass: "future", Error: "?"}, script: true, wantIs: ErrAppUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			if tc.script {
				a.setResponse(OpProbeDomain, tc.resp)
			}
			v, err := NewAppClient(a.socket).ProbeDomain(context.Background(), "acct-01")
			if v != nil {
				t.Errorf("ProbeDomain byte count = %v, want nil on a failure", v)
			}
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("ProbeDomain err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if errors.Is(err, ErrCannotControl) {
				t.Errorf("ProbeDomain err = %v, want NEVER the retreat condition", err)
			}
		})
	}
}

// TestErrOpUnsupportedDistinct pins that ErrOpUnsupported is confusable with
// NEITHER the transient blip nor the retreat: if it read as ErrAppUnavailable the
// Setup poll would treat an old app as "still materializing" and loop forever
// instead of failing loud; if it read as ErrCannotControl an old app would retreat
// the account to symlink.
func TestErrOpUnsupportedDistinct(t *testing.T) {
	if errors.Is(ErrOpUnsupported, ErrAppUnavailable) {
		t.Error("ErrOpUnsupported errors.Is ErrAppUnavailable; an old app must not read as a transient blip")
	}
	if errors.Is(ErrOpUnsupported, ErrCannotControl) {
		t.Error("ErrOpUnsupported errors.Is ErrCannotControl; an old app must not read as the retreat")
	}
	if errors.Is(ErrOpUnsupported, ErrDomainNotServing) {
		t.Error("ErrOpUnsupported errors.Is ErrDomainNotServing; the loud-upgrade path must not read as a readiness miss")
	}
}

// TestAppClientUnreachable pins that dialing a dead socket maps to the
// dial-refusal subset of ErrAppUnavailable, never the retreat condition.
func TestAppClientUnreachable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "absent.sock") // no listener
	_, err := NewAppClient(socket).Register(context.Background(), "acct-01")
	if err == nil {
		t.Fatal("Register against a dead socket succeeded, want an error")
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
	if !errors.Is(err, ErrAppDialRefused) {
		t.Errorf("err = %v, want errors.Is ErrAppDialRefused", err)
	}
	if errors.Is(err, ErrCannotControl) {
		t.Errorf("err = %v, want a dead socket NOT classified as the retreat condition", err)
	}
}

func TestAppClientMidRPCFailureIsNotDialRefused(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}()

	_, err = NewAppClient(socket).Register(context.Background(), "acct-01")
	<-done
	if err == nil {
		t.Fatal("Register against an app that closed mid-RPC succeeded, want an error")
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
	if errors.Is(err, ErrAppDialRefused) {
		t.Errorf("err = %v, want a successful dial NOT classified as ErrAppDialRefused", err)
	}
}

// TestAppClientBoundButUnresponsive pins the op-timeout arm: a listener that
// accepts and never replies is ErrAppUnavailable, never the dial-refusal
// subset.
func TestAppClientBoundButUnresponsive(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "hang.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		ln.Close()
	})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = NewAppClient(socket).Register(ctx, "acct-01")
	if err == nil {
		t.Fatal("Register against a hung app succeeded, want an error")
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
	if errors.Is(err, ErrAppDialRefused) {
		t.Errorf("err = %v, want a hung op NOT classified as ErrAppDialRefused", err)
	}
}

// TestAppClientRefusedStaleSocket pins ECONNREFUSED — a socket file with no
// listener behind it — to the dial-refusal subset (the absent-socket test only
// covers ENOENT).
func TestAppClientRefusedStaleSocket(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "stale.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = ln.Close()

	_, err = NewAppClient(socket).Register(context.Background(), "acct-01")
	if err == nil {
		t.Fatal("Register against a stale socket file succeeded, want an error")
	}
	if !errors.Is(err, ErrAppDialRefused) {
		t.Errorf("err = %v, want errors.Is ErrAppDialRefused for ECONNREFUSED", err)
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
}

func TestErrAppDialRefusedAliasesChildUnavailable(t *testing.T) {
	if !errors.Is(ErrAppDialRefused, proc.ErrChildUnavailable) {
		t.Errorf("ErrAppDialRefused = %v, want errors.Is proc.ErrChildUnavailable", ErrAppDialRefused)
	}
}

// TestAppClientProbeCarriesClass pins the probe special case: an OK RPC whose
// throwaway-domain check failed surfaces that failure's sentinel, not bare FPOK.
func TestAppClientProbeCarriesClass(t *testing.T) {
	a := startFakeApp(t)
	a.setResponse(OpProbe, Response{OK: true, FPOK: false, ErrClass: ClassNoEntitlement, Error: "disabled"})
	ok, err := NewAppClient(a.socket).Probe(context.Background())
	if ok {
		t.Fatal("Probe = true, want false on a no-entitlement probe")
	}
	if !errors.Is(err, ErrCannotControl) {
		t.Errorf("Probe err = %v, want errors.Is ErrCannotControl", err)
	}
}

// TestAppClientRawRequestBytes pins the EXACT bytes Register puts on the wire,
// independent of the typed fake, so a field-name drift is caught here too.
func TestAppClientRawRequestBytes(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		line, _ := readLine(conn)
		got <- line
		_, _ = io.WriteString(conn, `{"proto":1,"ok":true,"path":"/cloud/acct-01"}`+"\n")
	}()
	if _, err := NewAppClient(socket).Register(context.Background(), "acct-01"); err != nil {
		t.Fatalf("Register = %v, want nil", err)
	}
	want := `{"proto":1,"op":"register","domain":"acct-01"}`
	if line := <-got; line != want {
		t.Fatalf("raw request = %s\nwant         %s", line, want)
	}
}

// TestAppClientUnknownOpReply pins that an app predating an op (bare unknown-op
// error) surfaces the message verbatim, not a sentinel.
func TestAppClientUnknownOpReply(t *testing.T) {
	a := startFakeApp(t) // no op scripted -> fake replies unknown-op
	_, err := NewAppClient(a.socket).Path(context.Background(), "acct-01")
	if err == nil || !contains(err.Error(), "unknown op") {
		t.Fatalf("Path err = %v, want the bare unknown-op message", err)
	}
	if errors.Is(err, ErrCannotControl) {
		t.Errorf("err = %v, want unknown-op NOT classified as the retreat condition", err)
	}
}
