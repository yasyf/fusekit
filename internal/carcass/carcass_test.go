package carcass

import (
	"errors"
	"io/fs"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDeadErrno(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil (healthy stat)", err: nil, want: false},
		{name: "ENOENT is healthy — absent is not wedged", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.ENOENT}, want: false},
		{name: "ENOTCONN", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.ENOTCONN}, want: true},
		{name: "EIO", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EIO}, want: true},
		{name: "EPERM — orphaned-server signature", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EPERM}, want: true},
		{name: "EACCES — orphaned-server signature", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EACCES}, want: true},
		{name: "EBADF is not a carcass", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EBADF}, want: false},
		{name: "plain error is not a carcass", err: errors.New("boom"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeadErrno(tc.err); got != tc.want {
				t.Errorf("DeadErrno(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// clearSeams fakes the whole proof ladder for one test; callers must not run
// in parallel.
type clearSeams struct {
	stat        func(p string) error
	mount       func(dir string) (mountID, bool)
	serversDead func(dir string) error
	force       func(dir string) error
}

func swapClearSeams(t *testing.T, s clearSeams) *[]string {
	t.Helper()
	prevStat, prevMount, prevDead, prevForce := statFn, lookupMountFn, serversDeadFn, forceFn
	forced := &[]string{}
	statFn = s.stat
	lookupMountFn = s.mount
	if s.serversDead == nil {
		s.serversDead = func(string) error { return nil }
	}
	serversDeadFn = s.serversDead
	forceFn = func(dir string) error {
		*forced = append(*forced, dir)
		if s.force != nil {
			return s.force(dir)
		}
		return nil
	}
	t.Cleanup(func() { statFn, lookupMountFn, serversDeadFn, forceFn = prevStat, prevMount, prevDead, prevForce })
	return forced
}

func swapProbeDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	prev := ProbeDeadline
	ProbeDeadline = d
	t.Cleanup(func() { ProbeDeadline = prev })
}

func deadErr(p string) error {
	return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOTCONN}
}

// TestClearNeverForcesOnHang is the pinned regression for the shipped
// hang-as-carcass hole: a stat that does not answer within the probe deadline
// is NEVER proof of death — Clear defers with ErrUndetermined and the force
// path is untouched.
func TestClearNeverForcesOnHang(t *testing.T) {
	swapProbeDeadline(t, 30*time.Millisecond)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	forced := swapClearSeams(t, clearSeams{
		stat:  func(string) error { <-release; return nil },
		mount: func(string) (mountID, bool) { return mountID{fsidA: 1}, true },
	})

	err := Clear("/carcass/hung")
	if !errors.Is(err, ErrUndetermined) {
		t.Fatalf("Clear(hanging stat) = %v, want ErrUndetermined", err)
	}
	if len(*forced) != 0 {
		t.Fatalf("a hanging stat forced %v — force-on-hang is the kernel-panic hole", *forced)
	}
}

// TestReaperGateNeverReadsHangAsDead extends the no-force-on-hang regression
// to the reaper path: the orphan reaper's carcass gate is Probe == Dead, and
// a hanging stat must read NOT dead — never a kill verdict.
func TestReaperGateNeverReadsHangAsDead(t *testing.T) {
	swapProbeDeadline(t, 30*time.Millisecond)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	prev := statFn
	statFn = func(string) error { <-release; return nil }
	t.Cleanup(func() { statFn = prev })

	if provenDead("/carcass/hung") {
		t.Fatal("provenDead(hanging stat) = true — a hang is never a carcass; the reaper must defer")
	}
}

func TestClearProofV2(t *testing.T) {
	pinned := mountID{fsidA: 7, fsidB: 9, fstype: "nfs", source: "go-nfsv4"}
	cases := []struct {
		name        string
		stat        func(calls *int, p string) error
		mount       func(lookups *int) (mountID, bool)
		serversDead func(dir string) error
		force       func(dir string) error
		wantErr     error // nil means success
		wantForce   int
	}{
		{
			name:  "healthy path is a no-op",
			stat:  func(*int, string) error { return nil },
			mount: func(*int) (mountID, bool) { return pinned, true },
		},
		{
			name:  "ENOENT is healthy — absent is not wedged",
			stat:  func(_ *int, p string) error { return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOENT} },
			mount: func(*int) (mountID, bool) { return mountID{}, false },
		},
		{
			name:  "dead errno off a non-mountpoint is local state, never forced",
			stat:  func(_ *int, p string) error { return deadErr(p) },
			mount: func(*int) (mountID, bool) { return mountID{}, false },
		},
		{
			name: "proven-dead mountpoint is forced and clears",
			stat: func(calls *int, p string) error {
				// Dead on the probe and the pre-force revalidation; healthy
				// (ENOENT) once the force unmounted it.
				if *calls <= 2 {
					return deadErr(p)
				}
				return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOENT}
			},
			mount: func(lookups *int) (mountID, bool) {
				// Pinned, re-verified pre-force, gone post-force.
				if *lookups <= 2 {
					return pinned, true
				}
				return mountID{}, false
			},
			wantForce: 1,
		},
		{
			name: "death not revalidated immediately before the force is never forced",
			stat: func(calls *int, p string) error {
				if *calls == 1 {
					return deadErr(p) // probe read dead …
				}
				return nil // … but the revalidation reads healthy (resurrection window)
			},
			mount:   func(*int) (mountID, bool) { return pinned, true },
			wantErr: ErrUndetermined,
		},
		{
			name: "mount identity drift between proof and force aborts (assertion #5)",
			stat: func(_ *int, p string) error { return deadErr(p) },
			mount: func(lookups *int) (mountID, bool) {
				if *lookups == 1 {
					return pinned, true
				}
				return mountID{fsidA: 99, fstype: "nfs"}, true // replaced/covering mount
			},
			wantErr: ErrUndetermined,
		},
		{
			name: "mount gone between proof and force aborts",
			stat: func(_ *int, p string) error { return deadErr(p) },
			mount: func(lookups *int) (mountID, bool) {
				if *lookups == 1 {
					return pinned, true
				}
				return mountID{}, false
			},
			wantErr: ErrUndetermined,
		},
		{
			name:        "live server means denial, not death — never forced (assertions #6/#9)",
			stat:        func(_ *int, p string) error { return deadErr(p) },
			mount:       func(*int) (mountID, bool) { return pinned, true },
			serversDead: errUndeterminedWrap,
			wantErr:     ErrUndetermined,
		},
		{
			name:      "a carcass the force cannot clear surfaces wedged",
			stat:      func(_ *int, p string) error { return deadErr(p) },
			mount:     func(*int) (mountID, bool) { return pinned, true },
			wantErr:   ErrWedged,
			wantForce: 1,
		},
		{
			name:      "bounded force timeout surfaces wedged and keeps the carcass fenced",
			stat:      func(_ *int, p string) error { return deadErr(p) },
			mount:     func(*int) (mountID, bool) { return pinned, true },
			force:     func(dir string) error { return errWedgedf(dir) },
			wantErr:   ErrWedged,
			wantForce: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls, lookups := 0, 0
			forced := swapClearSeams(t, clearSeams{
				stat:        func(p string) error { calls++; return tc.stat(&calls, p) },
				mount:       func(string) (mountID, bool) { lookups++; return tc.mount(&lookups) },
				serversDead: tc.serversDead,
				force:       tc.force,
			})
			err := Clear("/carcass/dir")
			if tc.wantErr == nil && err != nil {
				t.Fatalf("Clear = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Clear = %v, want %v", err, tc.wantErr)
			}
			if len(*forced) != tc.wantForce {
				t.Fatalf("force calls = %v, want %d", *forced, tc.wantForce)
			}
		})
	}
}

func errUndeterminedWrap(dir string) error {
	return errors.Join(ErrUndetermined, errors.New("live go-nfsv4 serving "+dir))
}

func errWedgedf(dir string) error {
	return errors.Join(ErrWedged, errors.New("umount -f timed out on "+dir))
}
