package overlay

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

func testHolderSpec(t *testing.T) *HolderSpec {
	t.Helper()
	// Per-test temp dir, never a shared path: a fuse-build spawn lands its
	// holder-binary copy under the test's own dir and cannot collide with another
	// run. See the holder-spawn-storm incident.
	dir := t.TempDir()
	sock := filepath.Join(dir, "mounts.sock")
	return &HolderSpec{
		Socket:         sock,
		LogPath:        filepath.Join(dir, "holder.log"),
		StableExecDir:  filepath.Join(dir, "bin"),
		CannotHostHint: "install fuse-t and switch to the live-mirror build",
		Version:        "test-1",
		Args:           []string{"mount-holder", "--socket", sock},
		SpawnTimeout:   time.Second,
	}
}

func TestProviderForSymlink(t *testing.T) {
	p, err := ProviderFor(BackendSymlink, testSpec())
	if err != nil {
		t.Fatalf("ProviderFor(symlink): %v", err)
	}
	sp, ok := p.(*SymlinkProvider)
	if !ok {
		t.Fatalf("ProviderFor(symlink) = %T, want *SymlinkProvider", p)
	}
	if sp.Backend() != BackendSymlink {
		t.Errorf("Backend() = %q, want symlink", sp.Backend())
	}
	if !sp.Spec.IsPrivate(".claude.json") {
		t.Error("ProviderFor did not thread the spec into the SymlinkProvider")
	}
}

func TestProviderForFuseBackends(t *testing.T) {
	spec := testSpec()
	spec.Holder = testHolderSpec(t)
	for _, b := range []Backend{BackendNFS, BackendFSKit} {
		p, err := ProviderFor(b, spec)
		if err != nil {
			t.Fatalf("ProviderFor(%q): %v", b, err)
		}
		rp, ok := p.(*RemoteFuseProvider)
		if !ok {
			t.Fatalf("ProviderFor(%q) = %T, want *RemoteFuseProvider", b, p)
		}
		if rp.Backend() != b {
			t.Errorf("ProviderFor(%q).Backend() = %q, want %q", b, rp.Backend(), b)
		}
		if rp.Socket != spec.Holder.Socket || rp.Version != spec.Holder.Version {
			t.Errorf("ProviderFor(%q) did not carry the HolderSpec onto RemoteHost: %+v", b, rp.RemoteHost)
		}
		if got := rp.PrivateRoot("/x/acct-01"); got != FusePrivateRoot("/x/acct-01") {
			t.Errorf("PrivateRoot = %q, want %q", got, FusePrivateRoot("/x/acct-01"))
		}
	}
}

// TestProviderForFuseWithoutHolderFails pins that a fuse backend with no Holder
// wiring fails loudly, never silently downgrades.
func TestProviderForFuseWithoutHolderFails(t *testing.T) {
	spec := testSpec() // Holder is nil
	for _, b := range []Backend{BackendNFS, BackendFSKit} {
		if _, err := ProviderFor(b, spec); err == nil {
			t.Errorf("ProviderFor(%q) with nil Holder = nil error, want a loud failure", b)
		}
	}
}

func TestProviderForUnknownBackendFails(t *testing.T) {
	_, err := ProviderFor(Backend("fuse"), testSpec())
	if err == nil || !errors.Is(err, ErrUnknownBackend) {
		t.Errorf("ProviderFor(legacy fuse) error = %v, want ErrUnknownBackend", err)
	}
}

// TestProviderForCarriesContentWiring pins that a HolderSpec's content wiring
// lands on the RemoteFuseProvider, so Setup registers a content mount, not a
// plain passthrough.
func TestProviderForCarriesContentWiring(t *testing.T) {
	spec := testSpec()
	h := testHolderSpec(t)
	h.BridgeSocket = "/x/bridge.sock"
	h.ContentMode = "source"
	h.ProbePath = "/.ccp-probe"
	h.PrivatePrefixes = []string{".claude.json", ".credentials.json"}
	h.AttrCache = true
	h.AttrCacheTimeout = 30 * time.Second
	spec.Holder = h
	p, err := ProviderFor(BackendNFS, spec)
	if err != nil {
		t.Fatalf("ProviderFor(nfs): %v", err)
	}
	rp := p.(*RemoteFuseProvider)
	switch {
	case rp.contentSocket != h.BridgeSocket:
		t.Errorf("contentSocket = %q, want %q", rp.contentSocket, h.BridgeSocket)
	case rp.contentMode != h.ContentMode:
		t.Errorf("contentMode = %q, want %q", rp.contentMode, h.ContentMode)
	case rp.probePath != h.ProbePath:
		t.Errorf("probePath = %q, want %q", rp.probePath, h.ProbePath)
	case len(rp.privatePrefixes) != 2:
		t.Errorf("privatePrefixes = %v, want 2 entries", rp.privatePrefixes)
	case rp.attrCache != h.AttrCache:
		t.Errorf("attrCache = %v, want %v", rp.attrCache, h.AttrCache)
	case rp.attrCacheTimeout != h.AttrCacheTimeout:
		t.Errorf("attrCacheTimeout = %v, want %v", rp.attrCacheTimeout, h.AttrCacheTimeout)
	}
}

// TestSelectExecPathPureBuildProbes pins that a pure build whose HolderSpec sets
// ExecPath is host-capable: Select attempts the spawn instead of short-circuiting
// to symlink at the early gate. The ExecPath-missing reason ("not installed at
// <path>") proves Select threaded ExecPath into the probe Spawn; a dropped ExecPath
// would take the pure-build branch, give the generic "cannot host" reason, and
// wrongly refuse a real cask install.
func TestSelectExecPathPureBuildProbes(t *testing.T) {
	if fusekit.Built() {
		t.Skip("the early-gate bypass is only observable on a pure build (a fuse build passes the gate anyway)")
	}
	spec := testSpec()
	h := testHolderSpec(t)
	h.ExecPath = filepath.Join(t.TempDir(), "does-not-exist-holder")
	spec.Holder = h
	_, b, reason, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b != BackendSymlink {
		t.Errorf("backend = %q, want symlink (cask binary absent)", b)
	}
	if !strings.Contains(reason, "did not start") {
		t.Errorf("reason = %q, want a spawn-failure reason (proving the early gate was passed), not the early-gate message", reason)
	}
	if !strings.Contains(reason, "not installed at") {
		t.Errorf("reason = %q, want the ExecPath-missing failure (proving Select threaded ExecPath into the probe Spawn); the generic %q reason means ExecPath was dropped", reason, "cannot host fuse mounts")
	}
}

// TestSelectNoHolderSpec pins that a spec with no Holder wiring selects symlink
// without probing, carrying the cannot-host reason. Independent of build tag.
func TestSelectNoHolderSpec(t *testing.T) {
	p, b, reason, err := Select(context.Background(), testSpec())
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b != BackendSymlink {
		t.Errorf("backend = %q, want symlink", b)
	}
	if _, ok := p.(*SymlinkProvider); !ok {
		t.Errorf("provider = %T, want *SymlinkProvider", p)
	}
	if reason == "" {
		t.Error("symlink verdict carried an empty reason, want a human-readable one")
	}
}

// TestSelectPureBuildSelectsSymlink pins the pure-build verdict: even with full
// Holder wiring, a binary that cannot host fuse mounts selects symlink without
// probing.
func TestSelectPureBuildSelectsSymlink(t *testing.T) {
	if fusekit.Built() {
		t.Skip("fuse build can probe; the no-probe pure-build verdict is pure-build only")
	}
	spec := testSpec()
	spec.Holder = testHolderSpec(t)
	p, b, reason, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b != BackendSymlink {
		t.Errorf("backend = %q, want symlink (pure build cannot host)", b)
	}
	if _, ok := p.(*SymlinkProvider); !ok {
		t.Errorf("provider = %T, want *SymlinkProvider", p)
	}
	if reason == "" {
		t.Error("pure-build symlink verdict carried an empty reason")
	}
}
