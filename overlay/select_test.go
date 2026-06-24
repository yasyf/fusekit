package overlay

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

func testHolderSpec(t *testing.T) *HolderSpec {
	t.Helper()
	// A per-test temp dir, never a fixed shared path: a fuse-build spawn that
	// reaches this lands its holder-binary copy under the test's own dir and
	// cannot collide with another run. See the holder-spawn-storm incident.
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
	// The spec is threaded through, not dropped: a private name the test spec
	// declares must be honored by the returned provider.
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
		// The HolderSpec maps onto the embedded RemoteHost verbatim.
		if rp.Socket != spec.Holder.Socket || rp.Version != spec.Holder.Version {
			t.Errorf("ProviderFor(%q) did not carry the HolderSpec onto RemoteHost: %+v", b, rp.RemoteHost)
		}
		// PrivateRoot is the fuse backing dir, not the account dir itself.
		if got := rp.PrivateRoot("/x/acct-01"); got != FusePrivateRoot("/x/acct-01") {
			t.Errorf("PrivateRoot = %q, want %q", got, FusePrivateRoot("/x/acct-01"))
		}
	}
}

// TestProviderForFuseWithoutHolderFails pins the configuration error: a fuse
// backend with no Holder wiring must fail loudly, never silently downgrade.
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

// TestSelectPureBuildSelectsSymlink pins the pure-build (no fuse tag) verdict:
// even with full Holder wiring, a binary that cannot host fuse mounts selects
// symlink without probing. The untagged test binary is exactly that build.
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
