package mountd

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/lease"
)

// TestWireIngressCanonicalizesAliases pins P-9: /x/mnt and /x/./mnt are ONE
// dir everywhere downstream — registry row, journal key, and above all the
// lease fence — because dispatch Cleans absolute paths exactly once at the
// wire ingress.
func TestWireIngressCanonicalizesAliases(t *testing.T) {
	const base, dir, alias = "/pool/base", "/pool/acct-01", "/pool/./acct-01"
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)

	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	setState(fake, func(string) bool { return true }, func(string, string) bool { return true })

	// The alias resolves to the SAME registry row: idempotent OK, no second Setup.
	if resp := s.dispatch(Request{Op: OpMount, Base: "/pool/./base", Dir: alias, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("alias mount = %s, want the canonical row's idempotent OK", resp.Error)
	}
	if setups, _ := fake.calls(); !reflect.DeepEqual(setups, []hostCall{{base, dir}}) {
		t.Fatalf("setups = %v, want exactly one canonical setup", setups)
	}
	if _, ok := s.registered(dir); !ok {
		t.Fatalf("registry lost the canonical row %s", dir)
	}

	// The alias hits the CANONICAL lease fence: a session lease on /x/mnt
	// blocks an unmount spelled /x/./mnt.
	h, err := lease.Acquire(s.LeaseDir, dir, "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: alias, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("alias unmount under the canonical lease = (ok=%v class=%q), want busy — the alias bypassed the fence", resp.OK, resp.ErrClass)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: alias, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("alias unmount after release = %s", resp.Error)
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("teardowns = %v, want the canonical dir", tears)
	}
}

// TestWireIngressRefusesRelativePaths pins the other canonicalization leg:
// non-absolute Base/Dir/MuxRoot never reach a handler.
func TestWireIngressRefusesRelativePaths(t *testing.T) {
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	cases := []struct {
		name string
		req  Request
	}{
		{name: "relative dir", req: Request{Op: OpMount, Base: "/b", Dir: "pool/acct", Owner: "cc-pool"}},
		{name: "relative base", req: Request{Op: OpMount, Base: "b", Dir: "/pool/acct", Owner: "cc-pool"}},
		{name: "relative mux root", req: Request{Op: OpMount, Base: "/b", Dir: "/mux/a", MuxRoot: "mux", Owner: "cc-pool"}},
		{name: "relative unmount dir", req: Request{Op: OpUnmount, Base: "/b", Dir: "pool/acct", Owner: "cc-pool"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.dispatch(tc.req)
			if resp.OK || !strings.Contains(resp.Error, "must be absolute") {
				t.Fatalf("dispatch = (ok=%v %q), want an absolute-path refusal", resp.OK, resp.Error)
			}
		})
	}
	if setups, tears := fake.calls(); len(setups) != 0 || len(tears) != 0 {
		t.Fatalf("relative paths reached the host: setups=%v tears=%v", setups, tears)
	}
}

// TestPathForCleanContract pins the lease-key contract: filepath.Clean is the
// ONE normalization — aliases collide, distinct dirs never do, and no
// realpath resolution happens (a pure string transform).
func TestPathForCleanContract(t *testing.T) {
	const root = "/leases"
	canon := lease.PathFor(root, "/x/mnt")
	for _, alias := range []string{"/x/./mnt", "/x//mnt", "/x/../x/mnt", "/x/mnt/"} {
		if got := lease.PathFor(root, alias); got != canon {
			t.Errorf("PathFor(%q) = %q, want the canonical %q", alias, got, canon)
		}
	}
	if lease.PathFor(root, "/x/mnt2") == canon {
		t.Error("distinct dirs collided")
	}
}

// TestSeizeRefusesRelativeDir pins the fence-side absolute requirement.
func TestSeizeRefusesRelativeDir(t *testing.T) {
	if _, err := lease.Seize(t.TempDir(), "x/mnt"); err == nil || errors.Is(err, lease.ErrBusy) {
		t.Fatalf("Seize(relative) = %v, want a refusal", err)
	}
}
