package overlay

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/fileproviderd"
)

// fpTestDirs returns a short-path base, account dir, and a domain root standing
// in for ~/Library/CloudStorage/<App>-<Name>/. The account dir's parent exists but
// the dir itself does not — Setup creates it as a symlink. Short /tmp paths keep
// socket and symlink ops off the long t.TempDir path.
func fpTestDirs(t *testing.T) (base, accountDir, domainRoot string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "ccp-ov-fpp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	base = filepath.Join(root, "base")
	domainRoot = filepath.Join(root, "cloud", "acct-01")
	accountDir = filepath.Join(root, "accounts", "acct-01")
	for _, d := range []string{base, domainRoot, filepath.Dir(accountDir)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return base, accountDir, domainRoot
}

// TestFileProviderSetupCreatesBridgeAndPrivateStore pins Setup's three effects:
// the domain is registered, the account dir becomes a symlink into the returned
// domain root, and the private store is seeded.
func TestFileProviderSetupCreatesBridgeAndPrivateStore(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(domain string) fileproviderd.Response {
		if domain != "acct-01" {
			t.Errorf("register domain = %q, want acct-01", domain)
		}
		return fileproviderd.Response{OK: true, Path: domainRoot}
	})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("Setup = %v, want nil", err)
	}

	got, err := os.Readlink(accountDir)
	if err != nil {
		t.Fatalf("account dir is not a symlink: %v", err)
	}
	if got != domainRoot {
		t.Errorf("bridge symlink target = %q, want the domain root %q", got, domainRoot)
	}
	priv := p.PrivateRoot(accountDir)
	if priv != FusePrivateRoot(accountDir) {
		t.Errorf("PrivateRoot = %q, want %q", priv, FusePrivateRoot(accountDir))
	}
	if fi, err := os.Stat(priv); err != nil || !fi.IsDir() {
		t.Errorf("private store %q not seeded as a dir (err=%v)", priv, err)
	}
	if p.Backend() != BackendFileProvider {
		t.Errorf("Backend() = %q, want %q", p.Backend(), BackendFileProvider)
	}
	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("second Setup = %v, want nil (idempotent)", err)
	}
}

// TestFileProviderSetupRefusesToClobberRealDir pins the fail-closed guard: a real
// (non-symlink) account dir holding account state must never be replaced by the
// bridge symlink.
func TestFileProviderSetupRefusesToClobberRealDir(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatal(err)
	}
	realFile := filepath.Join(accountDir, ".credentials.json")
	if err := os.WriteFile(realFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: true, Path: domainRoot}
	})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err == nil {
		t.Fatal("Setup over a real account dir = nil, want a loud clobber-guard failure")
	}
	if fi, err := os.Lstat(accountDir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Errorf("account dir was clobbered into a symlink (err=%v)", err)
	}
	if b, err := os.ReadFile(realFile); err != nil || string(b) != "secret" {
		t.Errorf("real account file lost or changed: %q, %v", b, err)
	}
}

// TestFileProviderSetupRetreatsOnNoEntitlement pins that a missing-entitlement
// register surfaces ErrCannotControl (the retreat condition), never the transient
// ErrAppUnavailable.
func TestFileProviderSetupRetreatsOnNoEntitlement(t *testing.T) {
	base, accountDir, _ := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoEntitlement, Error: "enable the extension"}
	})
	p := newFileProvider(fpSpecFor(a))

	err := p.Setup(base, accountDir)
	if !errors.Is(err, fileproviderd.ErrCannotControl) {
		t.Fatalf("Setup err = %v, want errors.Is ErrCannotControl (the retreat condition)", err)
	}
	if errors.Is(err, fileproviderd.ErrAppUnavailable) {
		t.Errorf("Setup err = %v, want the retreat NOT confused with the transient blip", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("account dir exists after a failed Setup, want none (lstat err=%v)", err)
	}
}

// TestFileProviderHealth pins Health's verdict for intact, drifted-symlink, and
// removed-registration (ErrNoDomain) states.
func TestFileProviderHealth(t *testing.T) {
	t.Run("intact domain and symlink", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		p := newFileProvider(fpSpecFor(a))
		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup = %v", err)
		}
		if err := p.Health(base, accountDir); err != nil {
			t.Fatalf("Health = %v, want nil (intact)", err)
		}
	})
	t.Run("drifted symlink target fails", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPath(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: true, Path: domainRoot + "-moved"}
		})
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		if err := fileproviderd.AtomicSymlink(accountDir, domainRoot); err != nil {
			t.Fatal(err)
		}
		p := newFileProvider(fpSpecFor(a))
		if err := p.Health(base, accountDir); err == nil {
			t.Fatal("Health with a drifted symlink target = nil, want a failure")
		}
	})
	t.Run("removed registration is ErrNoDomain", func(t *testing.T) {
		base, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPath(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
		})
		p := newFileProvider(fpSpecFor(a))
		err := p.Health(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("Health err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestFileProviderSync pins that Sync re-registers, re-asserts the bridge
// symlink, and signals the enumerator.
func TestFileProviderSync(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync = %v, want nil", err)
	}
	got, err := os.Readlink(accountDir)
	if err != nil || got != domainRoot {
		t.Fatalf("Sync did not assert the bridge symlink: %q, %v", got, err)
	}
	var sawRegister, sawSignal bool
	for _, op := range a.ops() {
		switch op {
		case fileproviderd.OpRegister:
			sawRegister = true
		case fileproviderd.OpSignal:
			sawSignal = true
		}
	}
	if !sawRegister || !sawSignal {
		t.Errorf("Sync ops = %v, want both a register and a signal", a.ops())
	}
}

// TestFileProviderTeardown pins that Teardown retracts the bridge symlink and
// deregisters the domain, leaving the private store in place.
func TestFileProviderTeardown(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("Setup = %v", err)
	}
	priv := p.PrivateRoot(accountDir)
	if err := p.Teardown(base, accountDir); err != nil {
		t.Fatalf("Teardown = %v, want nil", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("account dir still present after Teardown (lstat err=%v)", err)
	}
	var sawRemove bool
	for _, op := range a.ops() {
		if op == fileproviderd.OpRemove {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Errorf("Teardown ops = %v, want a remove", a.ops())
	}
	if fi, err := os.Stat(priv); err != nil || !fi.IsDir() {
		t.Errorf("Teardown removed the private store %q (err=%v)", priv, err)
	}
}

// TestFileProviderTeardownRefusesToRemoveRealDir pins the fail-closed guard:
// Teardown must never RemoveAll a real (non-symlink) account dir.
func TestFileProviderTeardownRefusesToRemoveRealDir(t *testing.T) {
	base, accountDir, _ := fpTestDirs(t)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(accountDir, "real.txt")
	if err := os.WriteFile(keep, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Teardown(base, accountDir); err == nil {
		t.Fatal("Teardown over a real account dir = nil, want a fail-closed refusal")
	}
	if b, err := os.ReadFile(keep); err != nil || string(b) != "data" {
		t.Errorf("Teardown destroyed real account data: %q, %v", b, err)
	}
}

// TestProviderForFileProvider pins that ProviderFor returns the FP adapter for a
// wired spec.
func TestProviderForFileProvider(t *testing.T) {
	a := startFakeFPApp(t)
	spec := testSpec()
	spec.FileProvider = fpSpecFor(a)
	p, err := ProviderFor(BackendFileProvider, spec)
	if err != nil {
		t.Fatalf("ProviderFor(fileprovider) = %v", err)
	}
	fp, ok := p.(*FileProviderProvider)
	if !ok {
		t.Fatalf("ProviderFor(fileprovider) = %T, want *FileProviderProvider", p)
	}
	if fp.Backend() != BackendFileProvider {
		t.Errorf("Backend() = %q, want fileprovider", fp.Backend())
	}
	if got := fp.PrivateRoot("/x/acct-01"); got != FusePrivateRoot("/x/acct-01") {
		t.Errorf("PrivateRoot = %q, want %q", got, FusePrivateRoot("/x/acct-01"))
	}
}

// TestProviderForFileProviderWithoutSpecFails pins that the FP backend with no
// FileProvider wiring fails loudly, never downgrades.
func TestProviderForFileProviderWithoutSpecFails(t *testing.T) {
	spec := testSpec() // FileProvider is nil
	if _, err := ProviderFor(BackendFileProvider, spec); err == nil {
		t.Error("ProviderFor(fileprovider) with nil FileProvider = nil error, want a loud failure")
	}
}
