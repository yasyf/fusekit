package overlay

import (
	"context"
	"testing"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/fileproviderd"
)

// TestSelectPrefersFileProvider pins the head of the preference order: with FP
// wired, available, and a throwaway probe confirming capability, Select returns
// the FP provider (empty reason) before ever spawning the fuse holder.
func TestSelectPrefersFileProvider(t *testing.T) {
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpProbe, fileproviderd.Response{OK: true, FPOK: true})
	withFileProviderEnabled(t, true)

	spec := testSpec()
	spec.FileProvider = fpSpecFor(a)
	spec.Holder = testHolderSpec(t) // even with full fuse wiring, FP wins

	p, b, reason, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b != BackendFileProvider {
		t.Fatalf("backend = %q, want fileprovider (FP preferred over fuse)", b)
	}
	if _, ok := p.(*FileProviderProvider); !ok {
		t.Fatalf("provider = %T, want *FileProviderProvider", p)
	}
	if reason != "" {
		t.Errorf("FP verdict carried reason %q, want empty", reason)
	}
}

// TestSelectFallsThroughWhenFileProviderUnavailable pins that an unavailable
// extension skips the FP arm entirely (the probe never runs) and falls through to
// the fuse→symlink ladder.
func TestSelectFallsThroughWhenFileProviderUnavailable(t *testing.T) {
	if fusekit.Built() {
		t.Skip("a fuse build drives a REAL holder spawn on the fuse fall-through; the FP arm is build-independent and is covered by the pure build")
	}
	a := startFakeFPApp(t)
	// A capable probe, so the ONLY reason FP is skipped is unavailability.
	a.setResponse(fileproviderd.OpProbe, fileproviderd.Response{OK: true, FPOK: true})
	withFileProviderEnabled(t, false)

	spec := testSpec()
	spec.FileProvider = fpSpecFor(a)
	spec.Holder = testHolderSpec(t)

	_, b, reason, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b == BackendFileProvider {
		t.Fatal("backend = fileprovider, want a fall-through (extension unavailable)")
	}
	for _, op := range a.ops() {
		if op == fileproviderd.OpProbe {
			t.Errorf("Select probed FP despite unavailability; ops = %v", a.ops())
		}
	}
	if !fusekit.Built() {
		if b != BackendSymlink {
			t.Errorf("backend = %q, want symlink in the pure build", b)
		}
		if reason == "" {
			t.Error("symlink fall-through carried an empty reason")
		}
	}
}

// TestSelectFallsThroughWhenFileProviderProbeRefuses pins that an available
// extension whose throwaway probe refuses capability falls through to the ladder,
// rather than returning a half-working FP verdict.
func TestSelectFallsThroughWhenFileProviderProbeRefuses(t *testing.T) {
	if fusekit.Built() {
		t.Skip("a fuse build drives a REAL holder spawn on the fuse fall-through; the FP arm is build-independent and is covered by the pure build")
	}
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpProbe, fileproviderd.Response{
		OK: true, FPOK: false, ErrClass: fileproviderd.ClassNoEntitlement, Error: "extension off",
	})
	withFileProviderEnabled(t, true)

	spec := testSpec()
	spec.FileProvider = fpSpecFor(a)
	spec.Holder = testHolderSpec(t)

	_, b, _, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b == BackendFileProvider {
		t.Fatal("backend = fileprovider, want a fall-through (probe did not confirm capability)")
	}
	var probed bool
	for _, op := range a.ops() {
		if op == fileproviderd.OpProbe {
			probed = true
		}
	}
	if !probed {
		t.Errorf("Select did not probe FP; ops = %v", a.ops())
	}
}

// TestSelectNoFileProviderSpecKeepsLadder pins that a nil FileProvider never
// reaches the FP arm — Select keeps the fuse→symlink ladder.
func TestSelectNoFileProviderSpecKeepsLadder(t *testing.T) {
	spec := testSpec() // FileProvider is nil
	_, b, reason, err := Select(context.Background(), spec)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if b != BackendSymlink {
		t.Errorf("backend = %q, want symlink (no FP, no holder)", b)
	}
	if reason == "" {
		t.Error("symlink verdict carried an empty reason")
	}
}
