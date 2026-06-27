//go:build fuse && cgo && darwin

package holderfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit"
)

// TestReadyFnProbePath pins the come-up liveness fix. The bug: come-up used the
// generic MountAlive, which lstats Base's lexicographically-first entry through
// the mount — for the holder that is a dotfile like ".credentials.json", a
// PrivatePrefixes redirect onto a PrivateRoot copy a Keychain-auth account never
// has, so the lstat returned a clean -ENOENT and come-up stalled until the
// timeout. The fix lstats the virtual PROBE path instead (always resolvable once
// the mount serves, never a redirect). readyFn does os.Stat(dir) + os.Lstat(
// dir/probe) — pure filesystem ops — so a plain file standing in for the live
// probe exercises the exact decision without a real fuse mount.
func TestReadyFnProbePath(t *testing.T) {
	dir := t.TempDir()
	ready := readyFn(fusekit.MountSpec{Dir: dir, Base: dir, ProbePath: "/.ccp-probe"})

	if ready() {
		t.Fatal("ready with no probe present = true, want false (mount not yet serving)")
	}
	if err := os.WriteFile(filepath.Join(dir, ".ccp-probe"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("ready with the probe present = false, want true (mount serving)")
	}
}

// TestReadyFnNoProbeFallsBackToMountAlive pins that a probe-less mount (a pure
// Base passthrough with no redirects, where MountAlive is correct) keeps using
// it: with Base holding one entry, MountAlive reports live.
func TestReadyFnNoProbeFallsBackToMountAlive(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ready := readyFn(fusekit.MountSpec{Dir: dir, Base: dir, ProbePath: ""})
	if !ready() {
		t.Fatal("probe-less ready over a dir with one entry = false, want true (MountAlive)")
	}
}
