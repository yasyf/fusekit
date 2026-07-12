package fusekit

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCgofusePinnedSignalRegistration pins the SHAPE of the pinned cgofuse
// source that Mount's post-ready signal.Reset ordering argument depends on:
// (i) FileSystemHost.Mount creates host.sigc, and (ii) hostInit — which the
// FUSE protocol serves before any other operation, so a successful ready()
// probe proves it ran — calls signal.Notify on it. A future pin whose
// registration timing moves must fail HERE, loudly, not regress into silent
// MNT_FORCE-on-SIGTERM (see defuseCgofuseSignals).
func TestCgofusePinnedSignalRegistration(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/winfsp/cgofuse").Output()
	if err != nil {
		t.Skipf("SKIPPING THE CGOFUSE SOURCE PIN: go list could not locate the pinned module (offline / no module cache?): %v — the defuse ordering argument is UNVERIFIED for this run", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		t.Skip("SKIPPING THE CGOFUSE SOURCE PIN: go list returned an empty module dir — the defuse ordering argument is UNVERIFIED for this run")
	}
	src, err := os.ReadFile(filepath.Join(dir, "fuse", "host.go"))
	if err != nil {
		t.Skipf("SKIPPING THE CGOFUSE SOURCE PIN: cannot read the pinned fuse/host.go: %v — the defuse ordering argument is UNVERIFIED for this run", err)
	}
	text := string(src)

	mountBody := funcBody(t, text, "func (host *FileSystemHost) Mount(")
	if !strings.Contains(mountBody, "host.sigc = make(chan os.Signal") {
		t.Fatal("pinned cgofuse Mount no longer creates host.sigc — re-verify the signal registration path and the defuseCgofuseSignals ordering argument before repinning")
	}
	initBody := funcBody(t, text, "func hostInit(")
	if !strings.Contains(initBody, "signal.Notify(host.sigc") {
		t.Fatal("pinned cgofuse hostInit no longer calls signal.Notify(host.sigc, ...) — the Reset-after-ready ordering argument is void; re-verify before repinning")
	}
}

// funcBody returns the source from decl to the next top-level func.
func funcBody(t *testing.T, src, decl string) string {
	t.Helper()
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("pinned cgofuse host.go lacks %q — the registration moved; re-verify the defuse ordering argument before repinning", decl)
	}
	rest := src[i:]
	if j := strings.Index(rest[1:], "\nfunc "); j >= 0 {
		rest = rest[:j+1]
	}
	return rest
}
