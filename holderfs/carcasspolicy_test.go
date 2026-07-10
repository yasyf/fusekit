//go:build fuse && cgo && darwin

package holderfs

import (
	"testing"

	"github.com/yasyf/fusekit"
)

// TestBuildClearCarcassObeysCarcassPolicy pins the pre-mount carcass clear to
// the spec's CarcassPolicy across the Build dispatch: defer never opts into
// ClearCarcass, force and absent do, and ForceOnWedge stays false everywhere
// — the holder is graceful-only.
func TestBuildClearCarcassObeysCarcassPolicy(t *testing.T) {
	private := t.TempDir()
	modes := []struct {
		name string
		spec func(policy string) fusekit.MountSpec
	}{
		{name: "source passthrough", spec: func(p string) fusekit.MountSpec {
			return fusekit.MountSpec{Base: t.TempDir(), Dir: "/m/a", Owner: "cc-pool", PrivateRoot: private, CarcassPolicy: p}
		}},
		{name: "mux root", spec: func(p string) fusekit.MountSpec {
			return fusekit.MountSpec{Base: "/", Dir: "/mux", Owner: "cc-pool", ContentMode: fusekit.ContentModeMux, CarcassPolicy: p}
		}},
	}
	policies := []struct {
		name   string
		policy string
		want   bool
	}{
		{name: "absent means force", policy: "", want: true},
		{name: "force", policy: fusekit.CarcassPolicyForce, want: true},
		{name: "defer never force-clears", policy: fusekit.CarcassPolicyDefer, want: false},
	}
	for _, m := range modes {
		for _, p := range policies {
			t.Run(m.name+"/"+p.name, func(t *testing.T) {
				cfg, err := Build(m.spec(p.policy))
				if err != nil {
					t.Fatalf("Build: %v", err)
				}
				if cfg.ClearCarcass != p.want {
					t.Fatalf("ClearCarcass = %v, want %v for policy %q", cfg.ClearCarcass, p.want, p.policy)
				}
				if cfg.ForceOnWedge {
					t.Fatal("ForceOnWedge = true; the holder must stay graceful-only")
				}
			})
		}
	}
}
