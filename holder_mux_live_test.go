//go:build fuse && cgo && darwin

// FUSEKIT_LIVE-gated round trip of the mux wire path against a REAL fuse-t
// mount: two source-mode tenants attached as subtrees of ONE native mount, over
// a real content bridge. It proves the whole stack end to end — per-tenant synth
// isolation (distinct bytes AND client-observed fileids distinct across tenants),
// carve-outs that resolve through the shared mount, and exactly one native kernel
// mount for both tenants. External test package (mountd imports fusekit);
// darwin-tagged because holderfs is.
//
// SAFETY: the holder shuts down gracefully (no kill -9) and a t.Cleanup
// force-unmounts + clears the carcass on every exit path, so a failed run cannot
// strand a wedged fuse-t mount. Scratch root is under /tmp (short sun_path, never
// ~/.claude).

package fusekit_test

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/holderfs"
	"github.com/yasyf/fusekit/internal/carcass"
	"github.com/yasyf/fusekit/mountd"
)

// muxLiveSource is a two-domain content.Source: each account gets a private
// synth entry (its own bytes) and a shared symlink carve-out into the base.
type muxLiveSource struct {
	synth        map[string][]byte // domain (accountDir) -> synth bytes
	sharedTarget string            // absolute backing path of the shared carve-out
}

func (s *muxLiveSource) Manifest(string) ([]content.Entry, error) {
	return []content.Entry{
		{Name: "shared.txt", Kind: content.EntrySymlink, Target: s.sharedTarget, Version: "1"},
		{Name: "synth.json", Kind: content.EntrySynth, Version: "1", Private: true},
	}, nil
}

func (s *muxLiveSource) ReadSynth(domain, _ string) ([]byte, error) { return s.synth[domain], nil }
func (s *muxLiveSource) WriteThrough(string, string, []byte) error  { return nil }
func (s *muxLiveSource) Classify(string) content.EntryKind          { return content.EntrySynth }

func serveMuxBridge(t *testing.T, src content.Source) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fk-mux-bridge")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "b.sock")
	srv := &content.BridgeServer{Socket: socket, Source: src, Version: "vLIVE", Log: log.New(io.Discard, "", 0)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("bridge did not exit")
		}
	})
	cl := content.NewBridgeClient(socket)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return socket
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bridge never came up")
	return ""
}

func statIno(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Ino
}

func TestLiveMuxTwoTenants(t *testing.T) {
	if os.Getenv("FUSEKIT_LIVE") != "1" {
		t.Skip("set FUSEKIT_LIVE=1 for live fuse-t mount tests")
	}

	root, err := os.MkdirTemp("/tmp", "fk-mux-")
	if err != nil {
		t.Fatalf("scratch root: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })

	base := filepath.Join(root, "base")
	muxRoot := filepath.Join(root, "mnt")
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	// The native mount lands at muxRoot; its parent must exist, muxRoot itself is
	// the mountpoint fuse-t creates over.
	if err := os.MkdirAll(muxRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	// Registered before RemoveAll (LIFO) so it runs first: clear the one
	// native root's carcass before its dir is removed (proof-gated;
	// healthy/absent is a no-op). A stranded wedged fuse-t mount can freeze
	// the machine.
	t.Cleanup(func() {
		_ = carcass.Clear(muxRoot)
	})

	const sharedBody = "shared-carve-out"
	sharedTarget := filepath.Join(base, "shared.txt")
	if err := os.WriteFile(sharedTarget, []byte(sharedBody), 0o600); err != nil {
		t.Fatalf("seed shared carve-out: %v", err)
	}

	type tenant struct {
		name       string
		accountDir string
		privateDir string
		subtree    string
		synth      []byte
	}
	tenants := []*tenant{
		{name: "acct-a", synth: []byte(`{"who":"aaaa"}`)},
		{name: "acct-b", synth: []byte(`{"who":"bbbbbb"}`)},
	}
	src := &muxLiveSource{synth: map[string][]byte{}, sharedTarget: sharedTarget}
	for _, tn := range tenants {
		tn.accountDir = filepath.Join(root, "accounts", tn.name)
		tn.privateDir = tn.accountDir + ".private"
		tn.subtree = filepath.Join(muxRoot, tn.name)
		if err := os.MkdirAll(tn.privateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		// The private synth writePath must exist for Getattr's backing lstat; the
		// served bytes are the source's (bridge) truth, distinct per tenant.
		if err := os.WriteFile(filepath.Join(tn.privateDir, "synth.json"), tn.synth, 0o600); err != nil {
			t.Fatal(err)
		}
		src.synth[tn.accountDir] = tn.synth
	}

	bridge := serveMuxBridge(t, src)

	socket := filepath.Join(root, "m.sock")
	srv := &mountd.Server{
		Socket:   socket,
		Host:     holderfs.Host(),
		Version:  "vLIVE",
		Log:      log.New(io.Discard, "", 0),
		LeaseDir: filepath.Join(root, "leases"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // belt to the force-unmount cleanup: ctx cancel sweeps mounts.
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	cl := mountd.NewClient(socket)
	cl.Owner = "live-test"
	waitUp(t, cl)

	for _, tn := range tenants {
		if err := cl.AddMount(fusekit.MountSpec{
			Base:          base,
			Dir:           tn.subtree,
			MuxRoot:       muxRoot,
			ContentSocket: bridge,
			Domain:        tn.accountDir,
			PrivateRoot:   tn.privateDir,
			ContentMode:   fusekit.ContentModeSource,
			ProbePath:     "/.ccp-probe",
		}); err != nil {
			t.Fatalf("AddMount %s: %v", tn.name, err)
		}
	}

	// Exactly one native kernel mount: the root is a mountpoint, the subtrees are
	// logical (never their own mountpoints).
	if !fusekit.Mounted(muxRoot) {
		t.Fatalf("mux root %s is not a mountpoint after attaching tenants", muxRoot)
	}
	for _, tn := range tenants {
		if fusekit.Mounted(tn.subtree) {
			t.Errorf("subtree %s is its own mountpoint; want a logical subtree of the ONE native mount", tn.subtree)
		}
	}

	// The holder lists both tenants, each a subtree of the same MuxRoot.
	mounts, err := cl.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(mounts) != 2 {
		t.Fatalf("list = %+v, want two subtree rows", mounts)
	}
	for _, m := range mounts {
		if m.MuxRoot != muxRoot || !m.Live {
			t.Errorf("list row = %+v, want a live subtree of %s", m, muxRoot)
		}
	}

	// Per-tenant synth isolation: each tenant's synth entry reads its OWN bytes
	// and the client observes a distinct fileid for it (never aliased across
	// tenants). go-nfsv4 mints its own client fileids, so the handler-side
	// SynthInoFloor slot discipline is defense-in-depth the client never sees;
	// pairwise-distinctness is the client-observable invariant.
	synthInos := map[string]uint64{}
	for _, tn := range tenants {
		got, err := os.ReadFile(filepath.Join(tn.subtree, "synth.json"))
		if err != nil {
			t.Fatalf("read %s synth: %v", tn.name, err)
		}
		if string(got) != string(tn.synth) {
			t.Errorf("%s synth = %q, want its own %q (cross-tenant leak)", tn.name, got, tn.synth)
		}
		ino := statIno(t, filepath.Join(tn.subtree, "synth.json"))
		synthInos[tn.name] = ino

		// The shared carve-out resolves through the mount to the base file.
		shared, err := os.ReadFile(filepath.Join(tn.subtree, "shared.txt"))
		if err != nil {
			t.Fatalf("read %s carve-out: %v", tn.name, err)
		}
		if string(shared) != sharedBody {
			t.Errorf("%s carve-out = %q, want %q", tn.name, shared, sharedBody)
		}
	}
	if synthInos["acct-a"] == synthInos["acct-b"] {
		t.Errorf("both tenants' synth entries serve fileid %d: slot remapping did not isolate them", synthInos["acct-a"])
	}

	// Detaching one tenant leaves the other, and the shared mount, intact.
	first := tenants[0]
	if err := cl.Unmount(base, first.subtree); err != nil {
		t.Fatalf("unmount %s: %v", first.name, err)
	}
	if !fusekit.Mounted(muxRoot) {
		t.Fatalf("mux root came down after a non-last detach")
	}
	if got, err := os.ReadFile(filepath.Join(tenants[1].subtree, "synth.json")); err != nil || string(got) != string(tenants[1].synth) {
		t.Fatalf("surviving tenant read after sibling detach = %q (err %v), want %q", got, err, tenants[1].synth)
	}

	cancel()
	if !cl.WaitGone(10 * time.Second) {
		t.Fatal("holder socket still live after shutdown")
	}
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
	if fusekit.Mounted(muxRoot) {
		t.Fatalf("mux root still mounted after the exit sweep took the last tenant")
	}
}
