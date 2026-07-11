//go:build fuse && cgo && darwin

package holderfs

import (
	"testing"

	"github.com/yasyf/fusekit"
)

// TestBuildAlwaysPreClearsGracefulOnly pins the Build dispatch's teardown
// posture: every mode opts into the pre-mount carcass clear (ClearCarcass —
// which itself forces only on carcass proof v2, under the server's lease
// fence) and ForceOnWedge stays false everywhere — the holder is
// graceful-only outside that one proven-dead path.
func TestBuildAlwaysPreClearsGracefulOnly(t *testing.T) {
	private := t.TempDir()
	modes := []struct {
		name string
		spec func() fusekit.MountSpec
	}{
		{name: "source passthrough", spec: func() fusekit.MountSpec {
			return fusekit.MountSpec{Base: t.TempDir(), Dir: "/m/a", Owner: "cc-pool", PrivateRoot: private}
		}},
		{name: "mux root", spec: func() fusekit.MountSpec {
			return fusekit.MountSpec{Base: "/", Dir: "/mux", Owner: "cc-pool", ContentMode: fusekit.ContentModeMux}
		}},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			cfg, err := Build(m.spec())
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if !cfg.ClearCarcass {
				t.Fatal("ClearCarcass = false; the pre-mount clear is one of the two force-capable sites and must always be armed")
			}
			if cfg.ForceOnWedge {
				t.Fatal("ForceOnWedge = true; the holder must stay graceful-only")
			}
		})
	}
}
