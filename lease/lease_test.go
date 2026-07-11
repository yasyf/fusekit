package lease

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func shrinkAcquireWait(t *testing.T, d time.Duration) {
	t.Helper()
	prev := acquireWait
	acquireWait = d
	t.Cleanup(func() { acquireWait = prev })
}

func TestPathFor(t *testing.T) {
	root := "/leases"
	cases := []struct {
		name     string
		a, b     string
		wantSame bool
	}{
		{name: "identical dirs share a lease file", a: "/pool/acct-01", b: "/pool/acct-01", wantSame: true},
		{name: "distinct dirs get distinct files", a: "/pool/acct-01", b: "/pool/acct-02", wantSame: false},
		// The canonical-path contract (P-9): Clean is applied exactly once, so
		// lexical aliases are ONE lease; Clean is the ONLY normalization, so a
		// distinct spelling that Clean cannot fold stays distinct.
		{name: "Clean folds // aliases into one lease", a: "/pool/acct-01", b: "/pool//acct-01", wantSame: true},
		{name: "Clean folds ./ aliases into one lease", a: "/pool/acct-01", b: "/pool/./acct-01", wantSame: true},
		{name: "Clean folds parent hops into one lease", a: "/pool/acct-01", b: "/pool/../pool/acct-01", wantSame: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pa, pb := PathFor(root, tc.a), PathFor(root, tc.b)
			if (pa == pb) != tc.wantSame {
				t.Fatalf("PathFor(%q)=%s PathFor(%q)=%s, wantSame=%v", tc.a, pa, tc.b, pb, tc.wantSame)
			}
			if filepath.Dir(pa) != root || filepath.Ext(pa) != ".lease" {
				t.Fatalf("PathFor shape = %s, want %s/<hash>.lease", pa, root)
			}
			base := strings.TrimSuffix(filepath.Base(pa), ".lease")
			if len(base) != 16 {
				t.Fatalf("hash prefix length = %d, want 16", len(base))
			}
		})
	}
}

func TestAcquireWritesHeaderAndProbeReadsIt(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-01"
	h, err := Acquire(root, dir, "cc-pool")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer h.Close()

	held, hdr, err := Probe(root, dir)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !held {
		t.Fatal("Probe = free with a live Handle, want held")
	}
	if hdr.Dir != dir || hdr.Owner != "cc-pool" || hdr.PID != os.Getpid() || hdr.Argv0 != os.Args[0] {
		t.Fatalf("header = %+v, want dir=%s owner=cc-pool pid=%d argv0=%s", hdr, dir, os.Getpid(), os.Args[0])
	}
	if hdr.Started.IsZero() {
		t.Fatal("header Started is zero")
	}
}

func TestSharedAcquire(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-01"
	h1, err := Acquire(root, dir, "cc-pool")
	if err != nil {
		t.Fatalf("Acquire h1: %v", err)
	}
	h2, err := Acquire(root, dir, "cc-pool")
	if err != nil {
		t.Fatalf("second shared Acquire: %v", err)
	}
	if _, err := Seize(root, dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Seize under two SH holders = %v, want ErrBusy", err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("close h1: %v", err)
	}
	if _, err := Seize(root, dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Seize with one SH holder left = %v, want ErrBusy", err)
	}
	if err := h2.Close(); err != nil {
		t.Fatalf("close h2: %v", err)
	}
	f, err := Seize(root, dir)
	if err != nil {
		t.Fatalf("Seize after last close = %v, want success", err)
	}
	if err := f.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestSeizeBusyCarriesProvenance(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-02"
	h, err := Acquire(root, dir, "cc-notes")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer h.Close()

	_, err = Seize(root, dir)
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("Seize = %v, want ErrBusy", err)
	}
	var held *HeldError
	if !errors.As(err, &held) {
		t.Fatalf("Seize error %T does not carry HeldError", err)
	}
	if held.Header.Owner != "cc-notes" || held.Header.Dir != dir || held.Header.PID != os.Getpid() {
		t.Fatalf("provenance = %+v, want owner=cc-notes dir=%s pid=%d", held.Header, dir, os.Getpid())
	}
	for _, want := range []string{"cc-notes", dir} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("busy error %q does not surface %q", err.Error(), want)
		}
	}
}

func TestFenceExcludesFreshAcquire(t *testing.T) {
	shrinkAcquireWait(t, 150*time.Millisecond)
	root := t.TempDir()
	const dir = "/pool/acct-03"
	f, err := Seize(root, dir)
	if err != nil {
		t.Fatalf("Seize on a free lease: %v", err)
	}
	if !f.Held() {
		t.Fatal("fresh fence reports not held")
	}
	if _, err := Acquire(root, dir, "cc-pool"); !errors.Is(err, ErrBusy) {
		t.Fatalf("Acquire under EX fence = %v, want ErrBusy after the deadline", err)
	}
	if err := f.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if f.Held() {
		t.Fatal("released fence reports held")
	}
	h, err := Acquire(root, dir, "cc-pool")
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	h.Close()
}

func TestReleaseUnlinksUnderEX(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-04"
	f, err := Seize(root, dir)
	if err != nil {
		t.Fatalf("Seize: %v", err)
	}
	p := f.Path()
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("lease file missing while fence held: %v", err)
	}
	if err := f.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("lease file survives Release: %v", err)
	}
	if err := f.Release(); err != nil {
		t.Fatalf("second Release: %v, want idempotent nil", err)
	}
}

// TestAcquireSurvivesFenceGC pins the unlink-race guard: an Acquire that
// waited out a fence must land on the FRESH lease file, visible to the next
// Seize — never a lock on the GC'd inode.
func TestAcquireSurvivesFenceGC(t *testing.T) {
	shrinkAcquireWait(t, 5*time.Second)
	root := t.TempDir()
	const dir = "/pool/acct-05"
	f, err := Seize(root, dir)
	if err != nil {
		t.Fatalf("Seize: %v", err)
	}

	var (
		wg   sync.WaitGroup
		h    *Handle
		aerr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		h, aerr = Acquire(root, dir, "cc-pool")
	}()
	time.Sleep(100 * time.Millisecond)
	if err := f.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}
	wg.Wait()
	if aerr != nil {
		t.Fatalf("Acquire across a fence GC: %v", aerr)
	}
	defer h.Close()

	held, hdr, err := Probe(root, dir)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !held || hdr.Owner != "cc-pool" {
		t.Fatalf("post-GC acquire invisible: held=%v hdr=%+v", held, hdr)
	}
	if _, err := Seize(root, dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Seize after post-GC Acquire = %v, want ErrBusy", err)
	}
}

func TestHandleFdIsNotCloexec(t *testing.T) {
	root := t.TempDir()
	h, err := Acquire(root, "/pool/acct-06", "cc-pool")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer h.Close()
	flags, err := unix.FcntlInt(h.f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("F_GETFD: %v", err)
	}
	if flags&unix.FD_CLOEXEC != 0 {
		t.Fatal("Handle fd is O_CLOEXEC; children could never inherit the lease")
	}
}

func TestFenceFdIsCloexec(t *testing.T) {
	root := t.TempDir()
	f, err := Seize(root, "/pool/acct-07")
	if err != nil {
		t.Fatalf("Seize: %v", err)
	}
	defer f.Release()
	flags, err := unix.FcntlInt(f.f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatalf("F_GETFD: %v", err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("Fence fd is inheritable; a spawned server could pin the fence forever")
	}
}

// TestChildInheritsLease proves the OFD semantics end-to-end: a fork+exec
// child inherits the non-CLOEXEC descriptor, so the lease stays held after
// the acquirer closes, and releases when the child dies.
func TestChildInheritsLease(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-08"
	h, err := Acquire(root, dir, "cc-pool")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	cmd := exec.Command("/bin/sleep", "30")
	if err := cmd.Start(); err != nil {
		h.Close()
		t.Fatalf("start child: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	if err := h.Close(); err != nil {
		t.Fatalf("close acquirer handle: %v", err)
	}
	if _, err := Seize(root, dir); !errors.Is(err, ErrBusy) {
		t.Fatalf("Seize with only the child holding = %v, want ErrBusy (fd inheritance broken)", err)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	if _, err := cmd.Process.Wait(); err != nil {
		t.Fatalf("wait child: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		f, err := Seize(root, dir)
		if err == nil {
			f.Release()
			return
		}
		if !errors.Is(err, ErrBusy) {
			t.Fatalf("Seize after child death: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("lease still held after the last holder died")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestProbeDoesNotDisturbState(t *testing.T) {
	root := t.TempDir()
	const dir = "/pool/acct-09"
	held, _, err := Probe(root, dir)
	if err != nil || held {
		t.Fatalf("Probe of a never-leased dir = held=%v err=%v, want free", held, err)
	}
	f, err := Seize(root, dir)
	if err != nil {
		t.Fatalf("Seize: %v", err)
	}
	defer f.Release()
	p := f.Path()

	held, _, err = Probe(root, dir)
	if err != nil {
		t.Fatalf("Probe under fence: %v", err)
	}
	if !held {
		t.Fatal("Probe under EX fence reads free")
	}
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("probe disturbed the lease file: %v", err)
	}
}

func TestList(t *testing.T) {
	root := t.TempDir()
	h, err := Acquire(root, "/pool/held", "cc-pool")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer h.Close()
	free, err := Acquire(root, "/pool/free", "cc-notes")
	if err != nil {
		t.Fatalf("Acquire free: %v", err)
	}
	free.Close()

	infos, err := List(root)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("List = %d entries, want 2: %+v", len(infos), infos)
	}
	byDir := map[string]Info{}
	for _, in := range infos {
		byDir[in.Header.Dir] = in
	}
	if got := byDir["/pool/held"]; !got.Held || got.Header.Owner != "cc-pool" {
		t.Fatalf("held lease listed as %+v", got)
	}
	if got := byDir["/pool/free"]; got.Held {
		t.Fatalf("free lease listed as held: %+v", got)
	}

	if infos, err := List(filepath.Join(root, "missing")); err != nil || infos != nil {
		t.Fatalf("List of a missing root = %v, %v; want nil, nil", infos, err)
	}
}

func TestAcquireRejectsRelativeDir(t *testing.T) {
	if _, err := Acquire(t.TempDir(), "pool/acct", "cc-pool"); err == nil {
		t.Fatal("Acquire with a relative dir succeeded, want error")
	}
}
