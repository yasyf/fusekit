// Package lease implements per-directory session leases: deterministic flock
// files on local APFS that bind "this dir is in use" to kernel lock state
// instead of consumer attestations, TTLs, or revoke RPCs.
//
// A lease file lives at <root>/<hex(sha256(filepath.Clean(dir)))[:16]>.lease
// — never under a mount. The canonical-path contract: dir must be ABSOLUTE;
// filepath.Clean is applied exactly once (here) and is the ONLY
// normalization — pure string, deterministic, NO symlink resolution (the
// no-realpath rule as cc-pool's Keychain service names), so /x/mnt and
// /x/./mnt are ONE lease while distinct symlink spellings stay distinct by
// design. The holder canonicalizes identically at its wire ingress, so lease
// key, registry, journal, and mount ops all agree byte-for-byte. The file
// carries an advisory JSON Header describing the ACQUIRER; the lock itself is
// the truth.
//
// Semantics (spike-1-proven on Darwin):
//
//   - Acquire opens the file WITHOUT O_CLOEXEC via raw syscall.Open
//     (os.OpenFile force-adds O_CLOEXEC on Darwin) and takes flock(LOCK_SH).
//     The shared lock survives syscall.Exec and is inherited by fork+exec
//     children, so the lease binds to the open-file-description refcount
//     across the WHOLE session tree and releases only when the last holder's
//     descriptor closes — fd close or process death. There is no other
//     release path: no TTL, no revoke, no daemon in the loop.
//   - A leaked descendant pinning the lease is FAIL-CLOSED by design: the dir
//     stays busy, surfaced by provenance (the advisory Header), never reaped.
//     fd inheritors are not enumerable; the Header describes the acquirer.
//   - Seize takes flock(LOCK_EX|LOCK_NB). EWOULDBLOCK means busy — the caller
//     gets ErrBusy plus best-effort provenance. Success means the caller holds
//     the exclusive fence for the ENTIRE teardown action (an in-kernel TOCTOU
//     fence: no session can acquire mid-teardown), then releases and unlinks
//     under the still-held lock (GC). The Fence descriptor IS O_CLOEXEC — it
//     must never leak into a spawned server and pin itself.
//   - Probe takes LOCK_EX|LOCK_NB and releases immediately on acquire: a
//     read-only held/free diagnostic that never tears anything down and never
//     unlinks.
package lease

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"crypto/sha256"
	"encoding/hex"
)

// acquireWait bounds Acquire's wait on a Seize's transient exclusive fence; a
// var so tests shrink it. Fences span one teardown action, so seconds suffice —
// a lease still busy past the wait surfaces as HeldError provenance.
var acquireWait = 5 * time.Second

// acquirePoll paces Acquire's LOCK_SH retry while a fence is held.
const acquirePoll = 20 * time.Millisecond

// seizeRetries bounds Seize's reopen loop against release-unlink races; each
// retry lands on the freshly created file, so contention resolves in one or
// two rounds.
const seizeRetries = 8

// ErrBusy means the lease is held; HeldError carries the provenance.
var ErrBusy = errors.New("lease is held")

// DefaultRoot is the fleet-wide lease directory, ~/.fusekit/leases — shared by
// every consumer and the holder, on local APFS, never under a mount. A home
// resolution failure is an error, never a fallback: a relative root would let
// two processes lock DIFFERENT files for the same dir and fail the fence open.
func DefaultRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("lease: resolve home for the fleet lease root: %w", err)
	}
	return filepath.Join(home, ".fusekit", "leases"), nil
}

// PathFor returns dir's lease file under root:
// hex(sha256(filepath.Clean(dir)))[:16].lease. Clean is the only
// normalization — never realpath (package doc).
func PathFor(root, dir string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(dir)))
	return filepath.Join(root, hex.EncodeToString(sum[:])[:16]+".lease")
}

// Header is the advisory provenance record the acquirer writes into the lease
// file — the LAST acquirer only: a later shared Acquire overwrites it while
// an earlier holder may still hold the lock, so a busy verdict can attribute
// to an exited acquirer. Advisory-only by design; the flock is the truth, and
// inherited holders (fork+exec children) are not enumerable and deliberately
// not recorded.
type Header struct {
	Dir     string    `json:"dir"`
	Owner   string    `json:"owner"`
	PID     int       `json:"pid"`
	Argv0   string    `json:"argv0"`
	Started time.Time `json:"started"`
}

// HeldError reports a busy lease with best-effort provenance; errors.Is
// matches ErrBusy.
type HeldError struct {
	Path   string
	Header Header
	// HeaderErr records why the advisory header could not be read; the lock
	// verdict stands regardless.
	HeaderErr error
}

// Error formats the busy verdict with the acquirer's provenance.
func (e *HeldError) Error() string {
	if e.HeaderErr != nil {
		return fmt.Sprintf("lease %s is held (provenance unreadable: %v)", e.Path, e.HeaderErr)
	}
	h := e.Header
	return fmt.Sprintf("lease %s is held by %s (owner %q, pid %d, argv0 %q, started %s)",
		e.Path, h.Dir, h.Owner, h.PID, h.Argv0, h.Started.Format(time.RFC3339))
}

// Unwrap makes errors.Is(err, ErrBusy) match.
func (e *HeldError) Unwrap() error { return ErrBusy }

// Handle is a held shared lease. Its ONLY release paths are Close and process
// death; the descriptor is deliberately non-CLOEXEC so exec'd and forked
// children inherit and pin it.
type Handle struct {
	f    *os.File
	path string
}

// Path returns the lease file path.
func (h *Handle) Path() string { return h.path }

// Close releases this holder's share of the lease. Children that inherited
// the descriptor keep the lease alive until they exit.
func (h *Handle) Close() error { return h.f.Close() }

// Acquire takes a shared lease on dir under root, writing the advisory Header
// for owner. It blocks up to a short deadline while a Seize fence is held,
// then fails with HeldError.
func Acquire(root, dir, owner string) (*Handle, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("lease: dir %q must be absolute", dir)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("lease: create root: %w", err)
	}
	p := PathFor(root, dir)
	deadline := time.Now().Add(acquireWait)
	for {
		// Raw syscall.Open: os.OpenFile force-adds O_CLOEXEC on Darwin.
		fd, err := syscall.Open(p, syscall.O_RDWR|syscall.O_CREAT, 0o600)
		if err != nil {
			return nil, fmt.Errorf("lease: open %s: %w", p, err)
		}
		if err := flockUntil(fd, syscall.LOCK_SH, deadline); err != nil {
			syscall.Close(fd)
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return nil, heldError(p)
			}
			return nil, fmt.Errorf("lease: flock %s: %w", p, err)
		}
		ok, err := fdIsPath(fd, p)
		if err != nil {
			syscall.Close(fd)
			return nil, err
		}
		if !ok {
			syscall.Close(fd)
			// One deadline bounds the WHOLE acquisition, the unlink/recreate
			// retry included — repeated release-unlink races must not exceed
			// the documented wait.
			if !time.Now().Before(deadline) {
				return nil, fmt.Errorf("lease: acquire %s: kept racing lease-file replacement past %s", p, acquireWait)
			}
			continue
		}
		f := os.NewFile(uintptr(fd), p)
		if err := writeHeader(f, Header{
			Dir:     dir,
			Owner:   owner,
			PID:     os.Getpid(),
			Argv0:   os.Args[0],
			Started: time.Now(),
		}); err != nil {
			f.Close()
			return nil, err
		}
		return &Handle{f: f, path: p}, nil
	}
}

// Fence is a held exclusive lease: the in-kernel TOCTOU fence a teardown
// action holds from busy-check through completion. Release unlinks the file
// under the still-held lock (GC) and closes.
type Fence struct {
	f        *os.File
	path     string
	released bool
}

// Path returns the lease file path.
func (f *Fence) Path() string { return f.path }

// Held reports whether the fence is still held (not yet released).
func (f *Fence) Held() bool { return !f.released }

// Release unlinks the lease file under the held exclusive lock, then closes.
func (f *Fence) Release() error {
	if f.released {
		return nil
	}
	f.released = true
	rmErr := os.Remove(f.path)
	if err := f.f.Close(); err != nil {
		return err
	}
	if rmErr != nil && !os.IsNotExist(rmErr) {
		return fmt.Errorf("lease: unlink %s: %w", f.path, rmErr)
	}
	return nil
}

// Seize takes dir's lease exclusively, non-blocking. A held lease returns
// HeldError with the acquirer's provenance. The fence descriptor is O_CLOEXEC:
// it must never leak into a spawned server.
func Seize(root, dir string) (*Fence, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("lease: dir %q must be absolute", dir)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("lease: create root: %w", err)
	}
	p := PathFor(root, dir)
	for range seizeRetries {
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, fmt.Errorf("lease: open %s: %w", p, err)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			f.Close()
			if errors.Is(err, syscall.EWOULDBLOCK) {
				return nil, heldError(p)
			}
			return nil, fmt.Errorf("lease: flock %s: %w", p, err)
		}
		ok, err := fdIsPath(int(f.Fd()), p)
		if err != nil {
			f.Close()
			return nil, err
		}
		if ok {
			return &Fence{f: f, path: p}, nil
		}
		// Raced another fence's release-unlink; retry on the fresh inode.
		f.Close()
	}
	return nil, fmt.Errorf("lease: seize %s: kept racing lease-file replacement", p)
}

// Probe reports whether dir's lease is held, with the advisory header when it
// is. It acquires LOCK_EX|LOCK_NB and releases immediately — read-only, never
// an unlink, never a teardown surface.
func Probe(root, dir string) (held bool, hdr Header, err error) {
	return probePath(PathFor(root, dir))
}

func probePath(p string) (held bool, hdr Header, err error) {
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return false, Header{}, nil
	}
	if err != nil {
		return false, Header{}, fmt.Errorf("lease: open %s: %w", p, err)
	}
	defer f.Close()
	hdr, hdrErr := readHeader(f)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return true, hdr, nil
		}
		return false, hdr, fmt.Errorf("lease: flock %s: %w", p, err)
	}
	if hdrErr != nil && !errors.Is(hdrErr, errEmptyHeader) {
		return false, Header{}, nil
	}
	return false, hdr, nil
}

// Info is one lease file's diagnostic state.
type Info struct {
	File   string
	Held   bool
	Header Header
}

// List enumerates root's lease files with held/free state and advisory
// headers — the read-only backing of the holder's "leases" op. A missing root
// is an empty list.
func List(root string) ([]Info, error) {
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lease: read root: %w", err)
	}
	var infos []Info
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".lease" {
			continue
		}
		p := filepath.Join(root, e.Name())
		held, hdr, err := probePath(p)
		if err != nil {
			return nil, err
		}
		infos = append(infos, Info{File: p, Held: held, Header: hdr})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].File < infos[j].File })
	return infos, nil
}

// flockUntil retries a non-blocking flock of how until deadline. flock(2) has
// no timeout, so blocking-with-deadline is a NB retry loop.
func flockUntil(fd int, how int, deadline time.Time) error {
	for {
		err := syscall.Flock(fd, how|syscall.LOCK_NB)
		if err == nil || !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		if !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(acquirePoll)
	}
}

// fdIsPath reports whether fd still refers to the live file at p — the
// unlink-race guard: a lock on an inode a fence GC'd is invisible to every
// future Seize and must be retried on the fresh file.
func fdIsPath(fd int, p string) (bool, error) {
	var fst syscall.Stat_t
	if err := syscall.Fstat(fd, &fst); err != nil {
		return false, fmt.Errorf("lease: fstat %s: %w", p, err)
	}
	if fst.Nlink == 0 {
		return false, nil
	}
	var pst syscall.Stat_t
	err := syscall.Stat(p, &pst)
	if errors.Is(err, syscall.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lease: stat %s: %w", p, err)
	}
	return fst.Dev == pst.Dev && fst.Ino == pst.Ino, nil
}

var errEmptyHeader = errors.New("empty lease header")

func writeHeader(f *os.File, hdr Header) error {
	data, err := json.Marshal(hdr)
	if err != nil {
		return fmt.Errorf("lease: marshal header: %w", err)
	}
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("lease: truncate header: %w", err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		return fmt.Errorf("lease: write header: %w", err)
	}
	return nil
}

func readHeader(f *os.File) (Header, error) {
	st, err := f.Stat()
	if err != nil {
		return Header{}, err
	}
	if st.Size() == 0 {
		return Header{}, errEmptyHeader
	}
	buf := make([]byte, st.Size())
	if _, err := f.ReadAt(buf, 0); err != nil {
		return Header{}, err
	}
	var hdr Header
	if err := json.Unmarshal(buf, &hdr); err != nil {
		return Header{}, err
	}
	return hdr, nil
}

// heldError builds the busy verdict with best-effort provenance from p's
// advisory header.
func heldError(p string) error {
	e := &HeldError{Path: p}
	f, err := os.Open(p)
	if err != nil {
		e.HeaderErr = err
		return e
	}
	defer f.Close()
	e.Header, e.HeaderErr = readHeader(f)
	return e
}
