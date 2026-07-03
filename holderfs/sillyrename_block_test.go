//go:build fuse && cgo && darwin

package holderfs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
)

// newSillyFS builds a holderFS over real temp dirs (so silly-rename routing
// exercises genuine syscalls), with cc-pool's private classification.
func newSillyFS(t *testing.T) (fs *holderFS, base, priv string) {
	t.Helper()
	base, priv = t.TempDir(), t.TempDir()
	fs = &holderFS{
		base:            base,
		privateRoot:     priv,
		privateExact:    map[string]bool{"daemon": true},
		privatePrefixes: []string{".claude.json"},
		shared:          map[string]sharedEntry{},
		synth:           map[string]*synthView{},
		synthFhs:        map[uint64]*synthHandle{},
		nextSynthFh:     synthFhBase,
	}
	return fs, base, priv
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSillyRenameRealRouting pins that only a TOP-LEVEL silly-class name routes
// to PrivateRoot; nested silly names and near-miss prefixes stay in Base.
func TestSillyRenameRealRouting(t *testing.T) {
	fs := newRoutingFS() // base=/base, priv=/priv
	cases := []struct{ path, want string }{
		{"/.fuse_hidden0001", "/priv/.fuse_hidden0001"}, // top-level silly → private root
		{"/.nfs.abcdef", "/priv/.nfs.abcdef"},           // top-level silly → private root
		{"/nested/.nfs.abc", "/base/nested/.nfs.abc"},   // nested silly is not in-class → base
		{"/.nfsx", "/base/.nfsx"},                       // near-miss (no trailing dot) → base
		{"/.fuse_hidde", "/base/.fuse_hidde"},           // near-miss (truncated) → base
	}
	for _, c := range cases {
		if got := fs.real(c.path); got != c.want {
			t.Errorf("real(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

// TestSillyRenamePlainFileDiverts pins the leak fix for a plain Base file: a
// silly-rename lands the placeholder in PrivateRoot (shared Base stays clean),
// and Getattr/Open/Read/Unlink on the hidden name resolve there.
func TestSillyRenamePlainFileDiverts(t *testing.T) {
	fs, base, priv := newSillyFS(t)
	mustWrite(t, filepath.Join(base, "open.txt"), "PRIVATE-BYTES")
	const hiddenName = ".fuse_hidden000000000000abc"
	hidden := "/" + hiddenName

	if rc := fs.Rename("/open.txt", hidden); rc != 0 {
		t.Fatalf("Rename(open.txt -> %s) = %d, want 0", hidden, rc)
	}
	if _, err := os.Stat(filepath.Join(base, hiddenName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("silly placeholder leaked into shared Base (stat err = %v)", err)
	}
	if _, err := os.Stat(filepath.Join(base, "open.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("source still in Base after rename (stat err = %v)", err)
	}
	divert := filepath.Join(priv, hiddenName)
	if data, err := os.ReadFile(divert); err != nil || string(data) != "PRIVATE-BYTES" {
		t.Fatalf("diverted file = %q, err = %v; want PRIVATE-BYTES under PrivateRoot", data, err)
	}

	var st fuse.Stat_t
	if rc := fs.Getattr(hidden, &st, ^uint64(0)); rc != 0 {
		t.Fatalf("Getattr(%s) = %d, want 0 (resolves in PrivateRoot)", hidden, rc)
	}
	rc, fh := fs.Open(hidden, syscall.O_RDONLY)
	if rc != 0 {
		t.Fatalf("Open(%s) = %d, want 0", hidden, rc)
	}
	buf := make([]byte, 64)
	n := fs.Read(hidden, buf, 0, fh)
	if n < 0 {
		fs.Release(hidden, fh)
		t.Fatalf("Read(%s) = %d, want bytes", hidden, n)
	}
	if string(buf[:n]) != "PRIVATE-BYTES" {
		t.Errorf("Read(%s) = %q, want PRIVATE-BYTES", hidden, buf[:n])
	}
	fs.Release(hidden, fh)

	if rc := fs.Unlink(hidden); rc != 0 {
		t.Fatalf("Unlink(%s) = %d, want 0", hidden, rc)
	}
	if _, err := os.Stat(divert); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Unlink left the diverted file (stat err = %v)", err)
	}
}

// TestSillyRenameSynthSourceDiverts pins the privacy-critical case: silly-
// renaming a synth entry moves its private writePath into PrivateRoot (never
// shared Base), and the synth view keeps serving its cached snapshot through an
// open handle despite the absent writePath — while path Getattr falls back to a
// clean ENOENT and recovers once the follow-on atomic rename recreates
// writePath.
func TestSillyRenameSynthSourceDiverts(t *testing.T) {
	fs, base, priv := newSillyFS(t)
	writePath := filepath.Join(priv, ".claude.json")
	mustWrite(t, writePath, "MERGED-PRIVATE")
	v := newSynthView(".claude.json", "d", deadClient(t), writePath, nil)
	v.ino = sharedLinkInoBase
	v.seedFromWritePath() // warm the cache from the durable bytes
	fs.synth["/.claude.json"] = v

	// A reader holds the synth entry open: read-only opens serve the cached
	// snapshot off a synth handle, independent of writePath.
	orc, fh := fs.Open("/.claude.json", syscall.O_RDONLY)
	if orc != 0 {
		t.Fatalf("Open(/.claude.json) = %d, want 0", orc)
	}
	defer fs.Release("/.claude.json", fh)

	const hiddenName = ".fuse_hidden000000000000cafe"
	hidden := "/" + hiddenName
	if rc := fs.Rename("/.claude.json", hidden); rc != 0 {
		t.Fatalf("Rename(/.claude.json -> %s) = %d, want 0", hidden, rc)
	}
	if _, err := os.Stat(filepath.Join(base, hiddenName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("synth writePath leaked into shared Base (stat err = %v)", err)
	}
	if data, err := os.ReadFile(filepath.Join(priv, hiddenName)); err != nil || string(data) != "MERGED-PRIVATE" {
		t.Fatalf("diverted synth doc = %q, err = %v; want MERGED-PRIVATE under PrivateRoot", data, err)
	}
	if _, err := os.Stat(writePath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("synth writePath still present after silly-rename (stat err = %v)", err)
	}

	// The synth entry still SERVES via the open handle despite the absent writePath.
	buf := make([]byte, 64)
	n := fs.Read("/.claude.json", buf, 0, fh)
	if n < 0 {
		t.Fatalf("Read via open handle after diversion = %d, want cache-served bytes", n)
	}
	if string(buf[:n]) != "MERGED-PRIVATE" {
		t.Errorf("Read via open handle after diversion = %q, want MERGED-PRIVATE (cache-served)", buf[:n])
	}
	// Path Getattr falls back gracefully to a clean ENOENT while writePath is gone.
	var st fuse.Stat_t
	if rc := fs.getattrSynthPath(v, &st); rc != -int(syscall.ENOENT) {
		t.Errorf("getattrSynthPath with absent writePath = %d, want ENOENT (graceful fallback)", rc)
	}
	// The follow-on atomic rename recreates writePath; the entry resolves again
	// on its minted ino.
	mustWrite(t, writePath, "MERGED-PRIVATE-NEXT")
	if rc := fs.getattrSynthPath(v, &st); rc != 0 {
		t.Fatalf("getattrSynthPath after writePath recreated = %d, want 0", rc)
	}
	if st.Ino != v.ino {
		t.Errorf("synth Getattr ino = %d, want minted %d", st.Ino, v.ino)
	}
}

// TestSillyRenameReaddirSuppressed pins that neither a PrivateRoot-diverted
// placeholder nor a pre-fix Base placeholder is ever listed, while ordinary
// dotfiles and real private entries still list.
func TestSillyRenameReaddirSuppressed(t *testing.T) {
	fs, base, priv := newSillyFS(t)
	mustWrite(t, filepath.Join(base, ".fuse_hidden00000000legacy"), "LEGACY") // pre-fix Base litter
	mustWrite(t, filepath.Join(base, "real.json"), "{}")
	mustWrite(t, filepath.Join(base, ".foo"), "dot")
	mustWrite(t, filepath.Join(priv, ".fuse_hidden00000000divert"), "DIVERT") // freshly diverted
	mustWrite(t, filepath.Join(priv, ".claude.json"), "PRIV")                 // a real private entry lists

	names := map[string]bool{}
	rc := fs.Readdir("/", func(name string, _ *fuse.Stat_t, _ int64) bool {
		names[name] = true
		return true
	}, 0, 0)
	if rc != 0 {
		t.Fatalf("Readdir(/) = %d, want 0", rc)
	}
	for _, want := range []string{"real.json", ".foo", ".claude.json"} {
		if !names[want] {
			t.Errorf("Readdir(/) missing %q; got %v", want, names)
		}
	}
	for _, wantNot := range []string{".fuse_hidden00000000legacy", ".fuse_hidden00000000divert"} {
		if names[wantNot] {
			t.Errorf("Readdir(/) lists silly placeholder %q, want it suppressed", wantNot)
		}
	}
	for n := range names {
		if sillyRenamed(n) {
			t.Errorf("Readdir(/) leaked silly-rename name %q", n)
		}
	}
}

// TestSillyRenameBuildSweep pins that Build clears stale silly litter from
// PrivateRoot (crash-orphaned placeholders) while leaving Base litter and
// non-litter alone.
func TestSillyRenameBuildSweep(t *testing.T) {
	base, priv := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(priv, ".fuse_hidden00000000stale"), "STALE")
	mustWrite(t, filepath.Join(priv, ".nfs.deadbeef"), "STALE")
	mustWrite(t, filepath.Join(priv, ".claude.json"), "KEEP")     // real private file, kept
	mustWrite(t, filepath.Join(base, ".fuse_hidden00000000base"), "BASE") // Base litter, out of scope

	if _, err := Build(fusekit.MountSpec{Base: base, Dir: t.TempDir(), PrivateRoot: priv}); err != nil {
		t.Fatal(err)
	}
	for _, gone := range []string{".fuse_hidden00000000stale", ".nfs.deadbeef"} {
		if _, err := os.Stat(filepath.Join(priv, gone)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Build left stale PrivateRoot litter %q (stat err = %v)", gone, err)
		}
	}
	if _, err := os.Stat(filepath.Join(priv, ".claude.json")); err != nil {
		t.Errorf("Build swept a real private file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, ".fuse_hidden00000000base")); err != nil {
		t.Errorf("Build swept Base litter (must be out-of-band only): %v", err)
	}
}

// TestSillyRenameNegativeOrdinaryRename pins that ordinary and near-miss names
// route to Base exactly as before — the divert must fire ONLY on the silly class.
func TestSillyRenameNegativeOrdinaryRename(t *testing.T) {
	fs, base, priv := newSillyFS(t)
	mustWrite(t, filepath.Join(base, "a.txt"), "A")
	if rc := fs.Rename("/a.txt", "/b.txt"); rc != 0 {
		t.Fatalf("Rename(a.txt -> b.txt) = %d, want 0", rc)
	}
	if _, err := os.Stat(filepath.Join(base, "b.txt")); err != nil {
		t.Errorf("ordinary rename target not Base-resident: %v", err)
	}
	if _, err := os.Stat(filepath.Join(priv, "b.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ordinary rename target wrongly diverted to PrivateRoot (stat err = %v)", err)
	}

	mustWrite(t, filepath.Join(base, "c.txt"), "C")
	if rc := fs.Rename("/c.txt", "/.nfsx"); rc != 0 { // near-miss: not silly-class
		t.Fatalf("Rename(c.txt -> .nfsx) = %d, want 0", rc)
	}
	if _, err := os.Stat(filepath.Join(base, ".nfsx")); err != nil {
		t.Errorf("near-miss .nfsx not Base-resident: %v", err)
	}
	if _, err := os.Stat(filepath.Join(priv, ".nfsx")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".nfsx wrongly diverted to PrivateRoot (stat err = %v)", err)
	}
}
