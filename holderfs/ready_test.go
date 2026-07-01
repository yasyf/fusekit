//go:build fuse && cgo && darwin

package holderfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit"
)

// TestReadyFnProbePath pins that come-up liveness lstats the virtual PROBE path
// (always resolvable once the mount serves), not MountAlive's lstat of Base's
// first entry — for the holder a PrivatePrefixes dotfile (".credentials.json")
// redirect that -ENOENTs and stalls come-up until timeout. readyFn is pure
// filesystem ops (os.Stat(dir) + os.Lstat(dir/probe)), so a plain file standing in
// for the live probe exercises the exact decision without a real fuse mount.
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
