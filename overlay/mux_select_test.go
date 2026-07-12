package overlay

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
	"github.com/yasyf/fusekit/mountd"
)

// recordingHost is a cgo-free mountd.Host stand-in: it records the specs and
// teardowns it is driven with and tracks per-dir liveness so an already-attached
// subtree re-mounts idempotently (State reports it live).
type recordingHost struct {
	mu        sync.Mutex
	specs     []fusekit.MountSpec
	teardowns [][2]string
	live      map[string]bool
}

var (
	_ mountd.Host           = (*recordingHost)(nil)
	_ mountd.TeardownPender = (*recordingHost)(nil)
)

func (h *recordingHost) Setup(spec fusekit.MountSpec) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.specs = append(h.specs, spec)
	if h.live == nil {
		h.live = map[string]bool{}
	}
	h.live[spec.Dir] = true
	return nil
}

func (h *recordingHost) Teardown(base, dir string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.teardowns = append(h.teardowns, [2]string{base, dir})
	delete(h.live, dir)
	return nil
}

// TeardownDone satisfies the required TeardownPender capability; the
// recording host never pends.
func (h *recordingHost) TeardownDone(string) <-chan struct{} { return nil }

func (h *recordingHost) State(_, dir string) (mounted, alive bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	l := h.live[dir]
	return l, l
}

func (h *recordingHost) capturedSpecs() []fusekit.MountSpec {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]fusekit.MountSpec(nil), h.specs...)
}

func (h *recordingHost) capturedTeardowns() [][2]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([][2]string(nil), h.teardowns...)
}

// startFakeHolder runs a real mountd.Server backed by host over a short /tmp
// socket, returning the socket path and the server's lease dir (so tests can
// hold a session lease against it). The holder is already Available, so a
// provider's AddMount reaches it without ever spawning a binary.
func startFakeHolder(t *testing.T, host mountd.Host) (socket, leaseDir string) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ov-mux-sock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket = filepath.Join(dir, "m.sock")
	leaseDir = t.TempDir()
	s := &mountd.Server{Socket: socket, Host: host, Version: "test", Log: log.New(io.Discard, "", 0), LeaseDir: leaseDir}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = s.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fake holder did not stop on ctx cancel")
		}
	})
	cl := mountd.NewClient(socket)
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("fake holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return socket, leaseDir
}

// muxTestDirs returns short /tmp base, muxRoot, and accountDir paths whose
// parents exist. The subtree (muxRoot/<name>) is never created on disk — the
// fake holder mounts nothing — which is exactly what a real per-account subtree
// looks like before its native mount: os.Lstat/Mounted read it as absent, so
// AddMount's local short-circuit never fires.
func muxTestDirs(t *testing.T) (base, muxRoot, accountDir string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "ov-mux")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	base = filepath.Join(root, "base")
	muxRoot = filepath.Join(root, "mnt")
	accountDir = filepath.Join(root, "accounts", "acct-01")
	for _, d := range []string{base, muxRoot, filepath.Dir(accountDir)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return base, muxRoot, accountDir
}

func muxProviderFor(socket, muxRoot string) *RemoteFuseProvider {
	return newRemoteFuse(BackendNFS, &HolderSpec{
		Socket:       socket,
		LogPath:      filepath.Join(os.TempDir(), "ov-mux-holder.log"),
		Owner:        "test",
		MuxRoot:      muxRoot,
		ContentMode:  "source",
		BridgeSocket: filepath.Join(os.TempDir(), "ov-mux-bridge.sock"),
		ProbePath:    "/.ccp-probe",
		Args:         []string{"holder", "--socket", socket},
	})
}

// TestClearAccountDirForBridge pins the empty-dir clear and the fail-closed
// occupied-dir refusal that setupMux relies on before laying the bridge symlink.
func TestClearAccountDirForBridge(t *testing.T) {
	t.Run("absent path is a no-op", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "missing")
		if err := clearAccountDirForBridge(p); err != nil {
			t.Fatalf("clear(absent) = %v, want nil", err)
		}
	})
	t.Run("empty real dir is removed", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "empty")
		if err := os.Mkdir(p, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := clearAccountDirForBridge(p); err != nil {
			t.Fatalf("clear(empty) = %v, want nil", err)
		}
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Errorf("empty dir survived clear (lstat err=%v)", err)
		}
	})
	t.Run("non-empty real dir is refused fail-closed", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "full")
		if err := os.Mkdir(p, 0o700); err != nil {
			t.Fatal(err)
		}
		keep := filepath.Join(p, "state.json")
		if err := os.WriteFile(keep, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := clearAccountDirForBridge(p)
		if !errors.Is(err, ErrAccountDirOccupied) {
			t.Fatalf("clear(non-empty) = %v, want ErrAccountDirOccupied", err)
		}
		if b, rerr := os.ReadFile(keep); rerr != nil || string(b) != "data" {
			t.Errorf("clear clobbered real state: %q, %v", b, rerr)
		}
	})
	t.Run("non-directory file is refused", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := clearAccountDirForBridge(p); !errors.Is(err, ErrAccountDirOccupied) {
			t.Fatalf("clear(file) = %v, want ErrAccountDirOccupied", err)
		}
	})
	t.Run("existing symlink is left for AtomicSymlink", func(t *testing.T) {
		p := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink("/anywhere", p); err != nil {
			t.Fatal(err)
		}
		if err := clearAccountDirForBridge(p); err != nil {
			t.Fatalf("clear(symlink) = %v, want nil", err)
		}
		if _, err := os.Lstat(p); err != nil {
			t.Errorf("clear removed the symlink (lstat err=%v)", err)
		}
	})
}

// TestMuxSetupLaysBridgeSymlink pins mux Setup: the account attaches as a subtree
// of the shared native mount (spec carries MuxRoot, Domain, PrivateRoot), and the
// account dir becomes a fail-closed symlink into its subtree. Idempotent.
func TestMuxSetupLaysBridgeSymlink(t *testing.T) {
	base, muxRoot, accountDir := muxTestDirs(t)
	host := &recordingHost{}
	socket, _ := startFakeHolder(t, host)
	p := muxProviderFor(socket, muxRoot)
	subtree := filepath.Join(muxRoot, "acct-01")

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("mux Setup = %v, want nil", err)
	}
	got, err := os.Readlink(accountDir)
	if err != nil {
		t.Fatalf("account dir is not the bridge symlink: %v", err)
	}
	if got != subtree {
		t.Errorf("bridge symlink target = %q, want the subtree %q", got, subtree)
	}
	specs := host.capturedSpecs()
	if len(specs) != 1 {
		t.Fatalf("holder Setup specs = %d, want exactly 1", len(specs))
	}
	s := specs[0]
	switch {
	case s.Dir != subtree:
		t.Errorf("spec.Dir = %q, want the subtree %q", s.Dir, subtree)
	case s.MuxRoot != muxRoot:
		t.Errorf("spec.MuxRoot = %q, want %q", s.MuxRoot, muxRoot)
	case s.Base != base:
		t.Errorf("spec.Base = %q, want %q", s.Base, base)
	case s.Domain != accountDir:
		t.Errorf("spec.Domain = %q, want the canonical account dir %q", s.Domain, accountDir)
	case s.PrivateRoot != FusePrivateRoot(accountDir):
		t.Errorf("spec.PrivateRoot = %q, want %q", s.PrivateRoot, FusePrivateRoot(accountDir))
	case s.ContentMode != "source":
		t.Errorf("spec.ContentMode = %q, want source", s.ContentMode)
	}

	// Idempotent: the second Setup neither re-attaches (holder reports the
	// subtree live) nor disturbs the already-correct symlink.
	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("second mux Setup = %v, want nil (idempotent)", err)
	}
	if specs := host.capturedSpecs(); len(specs) != 1 {
		t.Fatalf("holder Setup specs after idempotent re-Setup = %d, want still 1", len(specs))
	}
	if got, _ := os.Readlink(accountDir); got != subtree {
		t.Errorf("bridge symlink drifted after idempotent Setup: %q", got)
	}
}

// TestMuxSetupRefusesOccupiedDir pins the fail-closed guard: a non-empty real
// account dir is never clobbered by the bridge symlink — Setup returns
// ErrAccountDirOccupied with the real state intact and no symlink laid.
func TestMuxSetupRefusesOccupiedDir(t *testing.T) {
	base, muxRoot, accountDir := muxTestDirs(t)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(accountDir, ".credentials.json")
	if err := os.WriteFile(keep, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	host := &recordingHost{}
	socket, _ := startFakeHolder(t, host)
	p := muxProviderFor(socket, muxRoot)

	err := p.Setup(base, accountDir)
	if !errors.Is(err, ErrAccountDirOccupied) {
		t.Fatalf("mux Setup over a non-empty account dir = %v, want ErrAccountDirOccupied", err)
	}
	if fi, lerr := os.Lstat(accountDir); lerr != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Errorf("account dir was clobbered into a symlink (lstat err=%v)", lerr)
	}
	if b, rerr := os.ReadFile(keep); rerr != nil || string(b) != "secret" {
		t.Errorf("real account state lost or changed: %q, %v", b, rerr)
	}
}

// TestMuxTeardownFailClosed pins mux Teardown: it retracts the bridge symlink
// and, with no reachable holder, treats the detach as a no-op success (the native
// root is gone with the holder); a regular file at the account path is refused
// before any holder contact, so unexplained real state is never removed.
func TestMuxTeardownFailClosed(t *testing.T) {
	t.Run("bridge symlink is retracted", func(t *testing.T) {
		base, muxRoot, accountDir := muxTestDirs(t)
		subtree := filepath.Join(muxRoot, "acct-01")
		if err := os.Symlink(subtree, accountDir); err != nil {
			t.Fatal(err)
		}
		// A dead-end socket: the detach-first RemoveMount finds the holder
		// unreachable and no-ops, so Teardown's success rests on the symlink
		// retraction alone.
		p := muxProviderFor(filepath.Join(t.TempDir(), "dead.sock"), muxRoot)
		if _, err := p.Teardown(base, accountDir); err != nil {
			t.Fatalf("mux Teardown = %v, want nil", err)
		}
		if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
			t.Errorf("bridge symlink survived Teardown (lstat err=%v)", err)
		}
	})
	t.Run("regular file is refused, never removed", func(t *testing.T) {
		base, muxRoot, accountDir := muxTestDirs(t)
		if err := os.WriteFile(accountDir, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		p := muxProviderFor(filepath.Join(t.TempDir(), "dead.sock"), muxRoot)
		if _, err := p.Teardown(base, accountDir); err == nil {
			t.Fatal("mux Teardown over a regular file = nil, want a fail-closed refusal")
		}
		if b, rerr := os.ReadFile(accountDir); rerr != nil || string(b) != "data" {
			t.Errorf("Teardown destroyed the file occupying the account path: %q, %v", b, rerr)
		}
	})
}

// TestMuxTeardownLegacyRealDir pins the legacy (pre-mux) teardown arm: a REAL
// account dir — the pre-cutover per-dir mount shape — never trips the bridge
// symlink guard. Unmounted it is a no-op success with the dir and its state
// left in place, whether the holder is unreachable or reachable-but-ignorant
// (the detach RPC answers not-mounted as an OK no-op); a subtree attach left by
// a setup that failed before laying the bridge is released, not leaked.
func TestMuxTeardownLegacyRealDir(t *testing.T) {
	mkLegacyDir := func(t *testing.T, accountDir string) string {
		t.Helper()
		if err := os.MkdirAll(accountDir, 0o700); err != nil {
			t.Fatal(err)
		}
		keep := filepath.Join(accountDir, "real.json")
		if err := os.WriteFile(keep, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		return keep
	}
	assertDirIntact := func(t *testing.T, accountDir, keep string) {
		t.Helper()
		fi, err := os.Lstat(accountDir)
		if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
			t.Errorf("legacy account dir disturbed: fi=%v err=%v", fi, err)
		}
		if b, rerr := os.ReadFile(keep); rerr != nil || string(b) != "data" {
			t.Errorf("Teardown destroyed real account data: %q, %v", b, rerr)
		}
	}

	t.Run("unmounted dir with unreachable holder no-ops", func(t *testing.T) {
		base, muxRoot, accountDir := muxTestDirs(t)
		keep := mkLegacyDir(t, accountDir)
		p := muxProviderFor(filepath.Join(t.TempDir(), "dead.sock"), muxRoot)
		if _, err := p.Teardown(base, accountDir); err != nil {
			t.Fatalf("legacy Teardown = %v, want nil (the symlink guard must not fire)", err)
		}
		assertDirIntact(t, accountDir, keep)
	})
	t.Run("holder that never attached the account answers detach as a no-op", func(t *testing.T) {
		base, muxRoot, accountDir := muxTestDirs(t)
		keep := mkLegacyDir(t, accountDir)
		host := &recordingHost{}
		socket, _ := startFakeHolder(t, host)
		p := muxProviderFor(socket, muxRoot)
		if _, err := p.Teardown(base, accountDir); err != nil {
			t.Fatalf("legacy Teardown with an ignorant holder = %v, want nil", err)
		}
		if got := host.capturedTeardowns(); len(got) != 0 {
			t.Errorf("holder Teardown calls = %d (%v), want 0 — nothing was attached", len(got), got)
		}
		assertDirIntact(t, accountDir, keep)
	})
	t.Run("half-established subtree attach is released, not leaked", func(t *testing.T) {
		base, muxRoot, accountDir := muxTestDirs(t)
		keep := mkLegacyDir(t, accountDir)
		host := &recordingHost{}
		socket, _ := startFakeHolder(t, host)
		p := muxProviderFor(socket, muxRoot)
		// A setup over an occupied dir attaches the subtree, then refuses the
		// bridge — exactly the half-established shape Teardown must release.
		if err := p.Setup(base, accountDir); !errors.Is(err, ErrAccountDirOccupied) {
			t.Fatalf("Setup over an occupied dir = %v, want ErrAccountDirOccupied", err)
		}
		if _, err := p.Teardown(base, accountDir); err != nil {
			t.Fatalf("legacy Teardown = %v, want nil", err)
		}
		want := [2]string{base, filepath.Join(muxRoot, "acct-01")}
		if got := host.capturedTeardowns(); len(got) != 1 || got[0] != want {
			t.Errorf("holder teardowns = %v, want exactly [%v]", got, want)
		}
		assertDirIntact(t, accountDir, keep)
	})
}

// TestMuxTeardownBusyLeavesBridgeSymlink pins the ask-before-destroy order: a
// detach the holder bounces (ErrBusy — a live session's held lease) leaves the
// bridge symlink in place, so the session's canonical account path keeps
// resolving; only a holder-confirmed detach retracts it.
func TestMuxTeardownBusyLeavesBridgeSymlink(t *testing.T) {
	base, muxRoot, accountDir := muxTestDirs(t)
	host := &recordingHost{}
	socket, leaseDir := startFakeHolder(t, host)
	p := muxProviderFor(socket, muxRoot)
	subtree := filepath.Join(muxRoot, "acct-01")

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("Setup = %v, want nil", err)
	}
	l, err := lease.Acquire(leaseDir, subtree, "session")
	if err != nil {
		t.Fatal(err)
	}
	if _, terr := p.Teardown(base, accountDir); !errors.Is(terr, mountd.ErrBusy) {
		t.Fatalf("Teardown under a held session lease = %v, want errors.Is mountd.ErrBusy", terr)
	}
	if got, rerr := os.Readlink(accountDir); rerr != nil || got != subtree {
		t.Fatalf("bounced detach disturbed the bridge symlink: %q, %v", got, rerr)
	}
	if got := host.capturedTeardowns(); len(got) != 0 {
		t.Fatalf("holder teardowns during the bounce = %v, want none", got)
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	warn, terr := p.Teardown(base, accountDir)
	if terr != nil {
		t.Fatalf("Teardown after the lease released = %v, want nil", terr)
	}
	if warn != "" {
		t.Errorf("warning = %q, want empty from a journal-less holder", warn)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("bridge symlink survived the confirmed detach (lstat err=%v)", err)
	}
	if want := [2]string{base, subtree}; len(host.capturedTeardowns()) != 1 || host.capturedTeardowns()[0] != want {
		t.Errorf("holder teardowns = %v, want exactly [%v]", host.capturedTeardowns(), want)
	}
}

// TestMuxHealthDetectsBridgeDrift pins the non-mount Health checks: a missing or
// drifted bridge symlink is a failure the caller heals with Sync. The
// live-through-the-mount check needs a real mount and lives in the integration test.
func TestMuxHealthDetectsBridgeDrift(t *testing.T) {
	base, muxRoot, accountDir := muxTestDirs(t)
	p := muxProviderFor(filepath.Join(t.TempDir(), "dead.sock"), muxRoot)

	if err := p.Health(base, accountDir); err == nil {
		t.Fatal("Health with no bridge symlink = nil, want a failure")
	}
	if err := os.Symlink(filepath.Join(muxRoot, "acct-99"), accountDir); err != nil {
		t.Fatal(err)
	}
	err := p.Health(base, accountDir)
	if err == nil {
		t.Fatal("Health with a drifted bridge symlink = nil, want a failure")
	}
	// Sync re-lays it at the correct subtree (then reports the root not mounted —
	// expected without a real native mount).
	if serr := p.Sync(base, accountDir); serr == nil {
		t.Fatal("Sync with a drifted symlink and no live mount = nil, want the not-mounted failure")
	}
	if got, _ := os.Readlink(accountDir); got != filepath.Join(muxRoot, "acct-01") {
		t.Errorf("Sync did not re-lay the bridge symlink at the correct subtree: %q", got)
	}
}
