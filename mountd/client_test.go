package mountd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

// startRawHolder serves scripted raw lines over a short /tmp unix socket,
// speaking the wire by hand (never the real Server) so the client's encode/
// decode is pinned independently of the server. respond nil stalls (read, hold
// open, never reply); respond "" hangs up (close without replying, like a
// holder crash mid-request).
func startRawHolder(t *testing.T, respond func(reqLine string) string) (socket string, requests func() []string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-mountd-cl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket = filepath.Join(dir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	stall := make(chan struct{})
	t.Cleanup(func() {
		close(stall)
		ln.Close()
	})
	var mu sync.Mutex
	var lines []string
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					return
				}
				mu.Lock()
				lines = append(lines, strings.TrimSuffix(line, "\n"))
				mu.Unlock()
				if respond == nil {
					<-stall
					return
				}
				reply := respond(line)
				if reply == "" {
					return
				}
				_, _ = io.WriteString(conn, reply+"\n")
			}(conn)
		}
	}()
	requests = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
	return socket, requests
}

// TestClientRoundTrips pins each method's exact request bytes and its decoding
// of a canned response. The wantReq literals are proto-2 wire artifacts (see
// protocol_test.go).
func TestClientRoundTrips(t *testing.T) {
	tests := []struct {
		name    string
		resp    string
		wantReq string
		invoke  func(t *testing.T, c *Client)
	}{
		{
			name:    "health returns the holder version",
			resp:    `{"proto":2,"ok":true,"version":"v9.8.7 (abc1234)"}`,
			wantReq: `{"proto":2,"op":"health"}`,
			invoke: func(t *testing.T, c *Client) {
				v, err := c.Health()
				if err != nil || v != "v9.8.7 (abc1234)" {
					t.Fatalf("Health = %q, %v; want the holder's version", v, err)
				}
			},
		},
		{
			name:    "health surfaces a classless error verbatim",
			resp:    `{"proto":2,"ok":false,"error":"kaboom"}`,
			wantReq: `{"proto":2,"op":"health"}`,
			invoke: func(t *testing.T, c *Client) {
				if _, err := c.Health(); err == nil || err.Error() != "kaboom" {
					t.Fatalf("Health err = %v, want the holder's message verbatim", err)
				}
			},
		},
		{
			name:    "probe true",
			resp:    `{"proto":2,"ok":true,"fuse_ok":true}`,
			wantReq: `{"proto":2,"op":"probe"}`,
			invoke: func(t *testing.T, c *Client) {
				ok, err := c.Probe()
				if err != nil || !ok {
					t.Fatalf("Probe = %v, %v; want true", ok, err)
				}
			},
		},
		{
			name:    "probe false arrives as an omitted field",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"probe"}`,
			invoke: func(t *testing.T, c *Client) {
				ok, err := c.Probe()
				if err != nil || ok {
					t.Fatalf("Probe = %v, %v; want false", ok, err)
				}
			},
		},
		{
			name:    "mount sends base and dir",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"mount","base":"/pool/base","dir":"/pool/acct-01"}`,
			invoke: func(t *testing.T, c *Client) {
				if err := c.Mount("/pool/base", "/pool/acct-01"); err != nil {
					t.Fatalf("Mount: %v", err)
				}
			},
		},
		{
			name:    "unmount sends base and dir",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"unmount","base":"/pool/base","dir":"/pool/acct-01"}`,
			invoke: func(t *testing.T, c *Client) {
				warning, err := c.Unmount("/pool/base", "/pool/acct-01")
				if err != nil || warning != "" {
					t.Fatalf("Unmount = (%q, %v), want a clean OK", warning, err)
				}
			},
		},
		{
			name:    "unmount surfaces the persist-warning first-class",
			resp:    `{"proto":2,"ok":true,"warning":"journal: write journal: ENOSPC"}`,
			wantReq: `{"proto":2,"op":"unmount","base":"/pool/base","dir":"/pool/acct-01"}`,
			invoke: func(t *testing.T, c *Client) {
				warning, err := c.Unmount("/pool/base", "/pool/acct-01")
				if err != nil {
					t.Fatalf("Unmount: %v (an OK-with-warning must not read as failure)", err)
				}
				if !strings.Contains(warning, "ENOSPC") {
					t.Fatalf("Unmount warning = %q, want the holder's persist-warning", warning)
				}
			},
		},
		{
			name:    "reclaim surfaces the aggregated persist-warning",
			resp:    `{"proto":2,"ok":true,"warning":"/m/a: journal: write journal: ENOSPC"}`,
			wantReq: `{"proto":2,"op":"reclaim","owner":"cc-pool"}`,
			invoke: func(t *testing.T, c *Client) {
				c.Owner = "cc-pool"
				failed, warning, err := c.Reclaim()
				if err != nil || len(failed) != 0 {
					t.Fatalf("Reclaim = (%v, %v), want a clean sweep", failed, err)
				}
				if !strings.Contains(warning, "ENOSPC") {
					t.Fatalf("Reclaim warning = %q, want the aggregated persist-warning", warning)
				}
			},
		},
		{
			name:    "list decodes mounts",
			resp:    `{"proto":2,"ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":true},{"dir":"/pool/acct-02","base":"/pool/base","live":false}]}`,
			wantReq: `{"proto":2,"op":"list"}`,
			invoke: func(t *testing.T, c *Client) {
				mounts, err := c.List()
				if err != nil {
					t.Fatalf("List: %v", err)
				}
				want := []MountInfo{
					{Dir: "/pool/acct-01", Base: "/pool/base", Live: true},
					{Dir: "/pool/acct-02", Base: "/pool/base", Live: false},
				}
				if !reflect.DeepEqual(mounts, want) {
					t.Fatalf("List = %+v, want %+v", mounts, want)
				}
			},
		},
		{
			name:    "list with no mounts is empty",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"list"}`,
			invoke: func(t *testing.T, c *Client) {
				mounts, err := c.List()
				if err != nil || len(mounts) != 0 {
					t.Fatalf("List = %+v, %v; want empty", mounts, err)
				}
			},
		},
		{
			name:    "hello decodes version and features",
			resp:    `{"proto":2,"ok":true,"version":"v1.0.0","features":["mux","bridge","tree","lease-gate"]}`,
			wantReq: `{"proto":2,"op":"hello"}`,
			invoke: func(t *testing.T, c *Client) {
				h, err := c.Hello()
				if err != nil {
					t.Fatalf("Hello: %v", err)
				}
				if h.Version != "v1.0.0" || !reflect.DeepEqual(h.Features, []string{"mux", "bridge", "tree", "lease-gate"}) {
					t.Fatalf("Hello = %+v", h)
				}
				if err := h.Require(FeatureMux, FeatureLeaseGate); err != nil {
					t.Fatalf("Require(present features) = %v", err)
				}
				if err := h.Require("time-travel"); err == nil || !strings.Contains(err.Error(), "time-travel") {
					t.Fatalf("Require(missing feature) = %v, want an error naming it", err)
				}
			},
		},
		{
			name:    "leases decodes the diagnostic",
			resp:    `{"proto":2,"ok":true,"leases":[{"file":"/l/ab.lease","held":true,"dir":"/pool/acct-01","owner":"cc-pool","pid":42,"argv0":"claude","started":1700000000}]}`,
			wantReq: `{"proto":2,"op":"leases","owner":"cc-pool"}`,
			invoke: func(t *testing.T, c *Client) {
				c.Owner = "cc-pool"
				leases, err := c.Leases()
				if err != nil {
					t.Fatalf("Leases: %v", err)
				}
				want := []LeaseInfo{{File: "/l/ab.lease", Held: true, Dir: "/pool/acct-01", Owner: "cc-pool", PID: 42, Argv0: "claude", Started: 1700000000}}
				if !reflect.DeepEqual(leases, want) {
					t.Fatalf("Leases = %+v, want %+v", leases, want)
				}
			},
		},
		{
			name:    "list-all widens the view",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"list","owner":"cc-pool","all":true}`,
			invoke: func(t *testing.T, c *Client) {
				c.Owner = "cc-pool"
				if _, err := c.ListAll(); err != nil {
					t.Fatalf("ListAll: %v", err)
				}
			},
		},
		{
			name:    "bridges-all widens the view",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"bridges","owner":"cc-pool","all":true}`,
			invoke: func(t *testing.T, c *Client) {
				c.Owner = "cc-pool"
				if _, err := c.BridgesAll(); err != nil {
					t.Fatalf("BridgesAll: %v", err)
				}
			},
		},
		{
			name:    "leases-all widens the view",
			resp:    `{"proto":2,"ok":true}`,
			wantReq: `{"proto":2,"op":"leases","owner":"cc-pool","all":true}`,
			invoke: func(t *testing.T, c *Client) {
				c.Owner = "cc-pool"
				if _, err := c.LeasesAll(); err != nil {
					t.Fatalf("LeasesAll: %v", err)
				}
			},
		},
		{
			name:    "health carries the lease summary",
			resp:    `{"proto":2,"ok":true,"version":"v1.0.0","leases_total":3,"leases_held":1}`,
			wantReq: `{"proto":2,"op":"health"}`,
			invoke: func(t *testing.T, c *Client) {
				st, err := c.Status()
				if err != nil {
					t.Fatalf("Status: %v", err)
				}
				if st.LeasesTotal != 3 || st.LeasesHeld != 1 {
					t.Fatalf("Status lease summary = %d/%d, want 3/1", st.LeasesTotal, st.LeasesHeld)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			socket, requests := startRawHolder(t, func(string) string { return tc.resp })
			tc.invoke(t, NewClient(socket))
			got := requests()
			if len(got) != 1 {
				t.Fatalf("holder saw %d requests, want exactly 1: %q", len(got), got)
			}
			if got[0] != tc.wantReq {
				t.Fatalf("request line = %s\nwant         %s", got[0], tc.wantReq)
			}
		})
	}
}

func TestClientErrClassMapping(t *testing.T) {
	// guidance mirrors fusekit.mountWaitErr's missing-grant copy: a realistic
	// ClassTCC payload a holder round-trips over the wire.
	const guidance = "fuse mount did not come up: /pool/acct-01 never became live; on macOS a process's first fuse mount is blocked pending a one-time OS volume-access grant that this failed attempt surfaces — mounts retry automatically once it is granted"
	sentinels := []error{ErrTCCDenied, ErrMountTimeout, ErrMountFailed, ErrUnmountWedged, ErrForeignMount, ErrBusy, ErrBaseMismatch, ErrContentUnavailable, ErrOwnerMismatch, ErrUnknownClass}
	tests := []struct {
		name  string
		class string
		want  error // nil: no sentinel may match
	}{
		{"tcc", ClassTCC, ErrTCCDenied},
		{"mount-timeout", ClassMountTimeout, ErrMountTimeout},
		{"mount-failed", ClassMountFailed, ErrMountFailed},
		{"wedged", ClassWedged, ErrUnmountWedged},
		{"foreign-mount", ClassForeignMount, ErrForeignMount},
		{"busy", ClassBusy, ErrBusy},
		{"base-mismatch", ClassBaseMismatch, ErrBaseMismatch},
		// A transient content-bridge outage is retryable, never a convertible
		// failure: it must map to ErrContentUnavailable, NOT ErrMountFailed.
		{"content-unavailable", ClassContentUnavailable, ErrContentUnavailable},
		// A cross-owner refusal is a non-retryable misfire verdict, never a
		// mount failure or a retry class.
		{"owner-mismatch", ClassOwnerMismatch, ErrOwnerMismatch},
		// Forward skew: a class this client predates must map to ErrUnknownClass —
		// which drivers route to retry, never conversion — not to a wrong
		// sentinel or a plain (convertible) error.
		{"unknown class from a newer holder", "quota-exceeded", ErrUnknownClass},
		{"no class at all", "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := fmt.Sprintf(`{"proto":2,"ok":false,"error":%q,"err_class":%q}`, guidance, tc.class)
			socket, _ := startRawHolder(t, func(string) string { return resp })

			err := NewClient(socket).Mount("/pool/base", "/pool/acct-01")
			if err == nil {
				t.Fatal("ok=false must surface an error")
			}
			for _, s := range sentinels {
				if got, want := errors.Is(err, s), s == tc.want; got != want {
					t.Errorf("errors.Is(err, %v) = %v, want %v (err: %v)", s, got, want, err)
				}
			}
			if errors.Is(err, ErrHolderUnavailable) {
				t.Errorf("a holder reply must never read as holder-unavailable: %v", err)
			}
			if !strings.Contains(err.Error(), guidance) {
				t.Errorf("holder guidance lost from the error message: %q", err)
			}
			if tc.want != nil && errors.Is(tc.want, ErrUnknownClass) && !strings.Contains(err.Error(), tc.class) {
				t.Errorf("unrecognized class %q lost from the error message: %q", tc.class, err)
			}
		})
	}
}

func TestClientHolderUnavailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "never-bound.sock")
	c := NewClient(socket)
	if c.Available() {
		t.Fatal("Available = true for a socket that was never bound")
	}
	methods := []struct {
		name string
		call func() error
	}{
		{"health", func() error { _, err := c.Health(); return err }},
		{"probe", func() error { _, err := c.Probe(); return err }},
		{"mount", func() error { return c.Mount("/pool/base", "/pool/acct-01") }},
		{"unmount", func() error { _, err := c.Unmount("/pool/base", "/pool/acct-01"); return err }},
		{"list", func() error { _, err := c.List(); return err }},
	}
	for _, tc := range methods {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if !errors.Is(err, ErrHolderUnavailable) {
				t.Fatalf("err = %v, want errors.Is ErrHolderUnavailable", err)
			}
		})
	}
}

// TestClientAddMountOwnerMismatch pins the spec-vs-client owner footgun: a
// non-empty MountSpec.Owner disagreeing with Client.Owner is refused crisply
// before any wire I/O — never silently overridden by the wire owner; a
// matching or empty spec owner proceeds to the wire (holder-unavailable here,
// since nothing is bound).
func TestClientAddMountOwnerMismatch(t *testing.T) {
	c := NewClient(filepath.Join(shortSockDir(t), "never-bound.sock"))
	c.Owner = "cc-pool"

	err := c.AddMount(fusekit.MountSpec{Base: "/b", Dir: "/m", Owner: "cc-notes"})
	if err == nil || errors.Is(err, ErrHolderUnavailable) {
		t.Fatalf("AddMount(mismatched owner) = %v, want a pre-wire refusal", err)
	}
	if !strings.Contains(err.Error(), "cc-notes") || !strings.Contains(err.Error(), "cc-pool") {
		t.Fatalf("refusal %q does not name both owners", err)
	}
	for name, spec := range map[string]fusekit.MountSpec{
		"matching owner":   {Base: "/b", Dir: "/m", Owner: "cc-pool"},
		"empty spec owner": {Base: "/b", Dir: "/m"},
	} {
		if err := c.AddMount(spec); !errors.Is(err, ErrHolderUnavailable) {
			t.Fatalf("AddMount(%s) = %v, want to reach the wire (holder unavailable)", name, err)
		}
	}
}

// TestClientHolderDiedMidRequest: a holder that reads the request then hangs
// up without replying (a crash inside Setup, as a fuse-t fault would cause)
// leaves the outcome unknown and must read as ErrHolderUnavailable, never a
// classless op-level failure a driver could mistake for mount-failed.
func TestClientHolderDiedMidRequest(t *testing.T) {
	socket, requests := startRawHolder(t, func(string) string { return "" })
	err := NewClient(socket).Mount("/pool/base", "/pool/acct-01")
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Fatalf("err = %v, want errors.Is ErrHolderUnavailable", err)
	}
	for _, s := range []error{ErrTCCDenied, ErrMountTimeout, ErrMountFailed, ErrUnmountWedged, ErrForeignMount, ErrBusy, ErrBaseMismatch} {
		if errors.Is(err, s) {
			t.Fatalf("outcome-unknown must not carry an op-level class: errors.Is(err, %v) on %v", s, err)
		}
	}
	if got := requests(); len(got) != 1 {
		t.Fatalf("holder saw %d requests, want 1", len(got))
	}
}

// TestClientStalledHolderIsUnavailable: a holder that accepts but never
// replies must surface as ErrHolderUnavailable once the deadline blows — the
// caller cannot tell a wedged holder from a dead one and must not treat the
// timeout as an op-level failure. Exercised through do() (the mapping lives
// there; every method routes through it).
func TestClientStalledHolderIsUnavailable(t *testing.T) {
	socket, requests := startRawHolder(t, nil)
	c := NewClient(socket)
	if !c.Available() {
		t.Fatal("a stalled holder still accepts connections; Available must be true")
	}
	_, err := c.do(Request{Op: OpHealth}, 200*time.Millisecond)
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Fatalf("stalled response err = %v, want errors.Is ErrHolderUnavailable", err)
	}
	if got := requests(); len(got) != 1 {
		t.Fatalf("holder saw %d requests, want 1", len(got))
	}
}

func TestClientWaitGone(t *testing.T) {
	t.Run("true once the socket is dead", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "gone.sock")
		start := time.Now()
		if !NewClient(socket).WaitGone(2 * time.Second) {
			t.Fatal("WaitGone = false for a dead socket")
		}
		if time.Since(start) > time.Second {
			t.Fatal("WaitGone on a dead socket should return on the first failed dial")
		}
	})
	t.Run("false while the holder lives", func(t *testing.T) {
		socket, _ := startRawHolder(t, func(string) string { return `{"proto":2,"ok":true}` })
		if NewClient(socket).WaitGone(400 * time.Millisecond) {
			t.Fatal("WaitGone = true while the socket still accepts")
		}
	})
}

// TestClientWaitGoneContext pins the ctx arm: a done ctx ends the wait on a
// live socket long before the timeout (a daemon shutdown must not stall ~70s
// behind a wedged holder), while kernel truth still wins — a dead socket
// reports gone even under a cancelled ctx.
func TestClientWaitGoneContext(t *testing.T) {
	t.Run("cancel aborts the wait on a live socket", func(t *testing.T) {
		socket, _ := startRawHolder(t, func(string) string { return `{"proto":2,"ok":true}` })
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		if NewClient(socket).WaitGoneContext(ctx, 30*time.Second) {
			t.Fatal("WaitGoneContext = true while the socket still accepts")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("cancelled WaitGoneContext took %v, want a prompt abort", elapsed)
		}
	})
	t.Run("dead socket reads gone even under a done ctx", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "gone.sock")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if !NewClient(socket).WaitGoneContext(ctx, 2*time.Second) {
			t.Fatal("WaitGoneContext = false for a dead socket; kernel truth must win over cancellation")
		}
	})
}

// TestClientProtoSkewNamesTheRightSide pins the remediation direction: the
// OLDER side upgrades — a proto-1 holder means upgrade the cask, a proto-3
// holder means THIS consumer is behind.
func TestClientProtoSkewNamesTheRightSide(t *testing.T) {
	cases := []struct {
		name     string
		resp     string
		want     string
		mustSkip string
	}{
		{
			name: "backward skew names the cask", resp: `{"proto":1,"ok":true}`,
			want: "brew upgrade --cask fusekit-holder", mustSkip: "upgrade this consumer",
		},
		{
			name: "forward skew names this consumer", resp: `{"proto":3,"ok":true}`,
			want: "upgrade this consumer", mustSkip: "brew upgrade",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			socket, _ := startRawHolder(t, func(string) string { return tc.resp })
			_, err := NewClient(socket).Health()
			if !errors.Is(err, ErrProtoMismatch) {
				t.Fatalf("Health = %v, want ErrProtoMismatch", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("skew error %q does not name the fix %q", err, tc.want)
			}
			if strings.Contains(err.Error(), tc.mustSkip) {
				t.Fatalf("skew error %q tells the WRONG side to upgrade (%q)", err, tc.mustSkip)
			}
		})
	}
}
