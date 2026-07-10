package mountd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/state"
)

// Self-retire: a journaling holder polls RetireSkew for version skew between
// its running build and the installed one. On skew it keeps SERVING NORMALLY
// — new mounts and bridges land — until every journaled mount is provably
// idle per its IdlePolicy; a consumer that never attests defers the retire,
// never the service. Only once the idle gate passes does the holder enter the
// retiring state (new mounts and bridges bounce retryable ClassBusy), record
// the attempt in the persisted strike history, and drain gracefully — the
// journal entries survive — then exit so the LaunchAgent relaunches the
// installed build, which replays the journal.
//
// The drain is GRACEFUL-ONLY: any busy or wedged teardown aborts the sweep
// and remounts what was already swept — the holder NEVER force-unmounts (the
// kernel-panic invariant). A retire storm (aborted sweeps thrashing consumers
// in-process, or a broken install kill-cycling successor generations) trips a
// persisted strike breaker that parks the holder loudly instead of exiting;
// a generation that cannot PERSIST its attempt never sweeps at all, so an
// unwritable state dir cannot defeat the cross-generation breaker.

// Self-retire schedule; vars so tests shrink them.
var (
	retireTick         = 60 * time.Second
	retireStrikeLimit  = 3
	retireStrikeWindow = 10 * time.Minute
	retireParkLadder   = []time.Duration{10 * time.Minute, 30 * time.Minute, time.Hour}
)

// retiringBusy bounces a new mount/bridge request while the holder is
// retiring: retryable ClassBusy, so drivers retry into the successor.
func (s *Server) retiringBusy(op string) (Response, bool) {
	if !s.retiring.Load() {
		return Response{}, false
	}
	return Response{OK: false, ErrClass: ClassBusy, Error: op + ": busy: holder is retiring for an upgrade; retry"}, true
}

func (s *Server) retireLoop(ctx context.Context) {
	r := newRetirer(s)
	t := time.NewTicker(retireTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if r.tick(time.Now()) {
			return
		}
	}
}

// retirer holds the self-retire loop's state across ticks. The strike
// history, park deadline, and idle-gate deferral live on the Server under
// retireMu so OpHealth snapshots them without racing the tick; lastSkewErr
// and lastDefer only dedupe the tick's logging.
type retirer struct {
	s           *Server
	ladder      *proc.Ladder
	strikesPath string
	lastSkewErr string
	lastDefer   string
}

// setRetireDeferred publishes (or, with empty args, clears) the idle-gate
// deferral OpHealth reports.
func (s *Server) setRetireDeferred(dir, reason string) {
	s.retireMu.Lock()
	s.retireDeferredDir, s.retireDeferredReason = dir, reason
	s.retireMu.Unlock()
}

func newRetirer(s *Server) *retirer {
	r := &retirer{
		s:           s,
		ladder:      &proc.Ladder{Steps: retireParkLadder},
		strikesPath: filepath.Join(filepath.Dir(s.journal.path), "holder-retires.json"),
	}
	times, err := loadStrikeTimes(r.strikesPath)
	if err != nil {
		s.Log.Printf("retire: load strike history: %v; starting fresh", err)
	}
	strikes := &proc.Strikes{Limit: retireStrikeLimit, Window: retireStrikeWindow}
	strikes.Load(times)
	s.retireMu.Lock()
	s.strikes = strikes
	s.parkedUntil = time.Time{}
	s.retireDeferredDir, s.retireDeferredReason = "", ""
	s.retireMu.Unlock()
	return r
}

// tick runs one self-retire evaluation at now, reporting whether the holder
// retired (shutdown triggered; the loop must exit).
func (r *retirer) tick(now time.Time) bool {
	s := r.s
	s.retireMu.Lock()
	parked := now.Before(s.parkedUntil)
	s.retireMu.Unlock()
	if parked {
		return false
	}
	skewed, reason, err := s.RetireSkew()
	if err != nil {
		if msg := err.Error(); msg != r.lastSkewErr {
			r.lastSkewErr = msg
			s.Log.Printf("retire: skew check: %v", err)
		}
		return false
	}
	r.lastSkewErr = ""
	if !skewed {
		if s.retiring.CompareAndSwap(true, false) {
			s.Log.Printf("retire: version skew cleared; serving normally")
		}
		r.ladder.Reset()
		r.lastDefer = ""
		s.setRetireDeferred("", "")
		return false
	}
	if dir, missing := s.attestGateMissing(now); missing {
		// Deferred, NOT retiring: the holder serves normally (new mounts and
		// bridges land) while it waits for idleness — an always-busy or
		// never-attesting consumer must never wedge the holder into bouncing
		// all new work.
		s.retiring.Store(false)
		s.setRetireDeferred(dir, reason)
		if r.lastDefer != dir {
			r.lastDefer = dir
			s.Log.Printf("retire: version skew: %s; deferred: no fresh idle attestation for %s (serving normally)", reason, dir)
		}
		return false
	}
	r.lastDefer = ""
	s.setRetireDeferred("", "")
	if s.retiring.CompareAndSwap(false, true) {
		s.Log.Printf("retire: version skew: %s; draining (new mounts and bridges answer busy)", reason)
	}
	// A sweep is a retire attempt; strike BEFORE sweeping and persist, so a
	// kill-cycle of clean-sweeping successor generations parks the third
	// generation instead of thrashing (in-process aborted sweeps count too).
	// retireMu is never held across the persist I/O.
	s.retireMu.Lock()
	struck := s.strikes.Strike(now)
	times := s.strikes.Times()
	s.retireMu.Unlock()
	persistErr := r.persistStrikes(times)
	if struck {
		park := r.ladder.Next()
		s.retireMu.Lock()
		s.parkedUntil = now.Add(park)
		s.retireMu.Unlock()
		s.retiring.Store(false)
		s.Log.Printf("retire: STORM BREAKER: %d retire attempts within %s; parking for %s and serving normally (skew persists: %s)",
			retireStrikeLimit, retireStrikeWindow, park, reason)
		return false
	}
	if persistErr != nil {
		// A generation that cannot record its attempt must not sweep: exiting
		// here would let a broken install kill-cycle successors past the
		// cross-generation breaker. The in-memory strikes still park by the
		// third tick, capping the spam.
		s.retiring.Store(false)
		s.Log.Printf("retire: %v; DEFERRING the sweep and serving normally (an unrecorded attempt must not kill-cycle)", persistErr)
		return false
	}
	if !s.retireSweep() {
		// An aborted sweep is not a drain: serve normally until the next
		// tick re-passes the gate — bouncing new work between attempts would
		// be the same wedge the deferred branch exists to prevent.
		s.retiring.Store(false)
		return false
	}
	s.Log.Printf("retire: drained clean; exiting so the LaunchAgent relaunches the installed build (%s); the journal replays on start", reason)
	// A retire-triggered exit PRESERVES the journal for the successor's
	// replay; Run's clean-shutdown drain keeps only mounts that failed to
	// come down.
	s.retired.Store(true)
	if s.triggerShutdown != nil {
		s.triggerShutdown()
	}
	return true
}

func (r *retirer) persistStrikes(times []time.Time) error {
	data, err := json.MarshalIndent(times, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal strike history: %w", err)
	}
	if err := state.AtomicWrite(r.strikesPath, data, 0o600); err != nil {
		return fmt.Errorf("persist strike history: %w", err)
	}
	return nil
}

// loadStrikeTimes reads the persisted retire-strike times; a missing file is
// an empty history.
func loadStrikeTimes(path string) ([]time.Time, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var times []time.Time
	if err := json.Unmarshal(data, &times); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return times, nil
}

// attestGateMissing reports the first journaled mount whose IdlePolicy
// requires a consumer attestation ("attest", including absent — fail-closed)
// that is missing, foreign, or expired. IdlePolicy "probe" mounts prove
// idleness at teardown time instead.
func (s *Server) attestGateMissing(now time.Time) (dir string, missing bool) {
	mounts, _ := s.journal.snapshot()
	for _, m := range mounts {
		if m.IdlePolicy == fusekit.IdlePolicyProbe {
			continue
		}
		if !s.attestFresh(m.Dir, m.Owner, now) {
			return m.Dir, true
		}
	}
	return "", false
}

// retireSweep gracefully drains every journaled mount while KEEPING the
// journal entries — the successor replays them. Any busy claim, expired
// attestation, or failed graceful teardown (EBUSY/wedge) ABORTS the sweep and
// remounts the already-swept prefix. A wedged PLAIN mount is never touched —
// the provider restored its handle, so its surviving row stays honest and it
// keeps serving; a wedged mux tenant is already detached (the only error
// source is the last-child root unmount), so its lying row is dropped and the
// abort re-attaches it into the surviving root. Bridges need no sweep: the
// dispatch gate refuses new ones, and Run's shutdown cancels and drains the
// live runners on the way out with their journal entries intact.
func (s *Server) retireSweep() bool {
	mounts, _ := s.journal.snapshot()
	var swept []mountEntry
	abort := func(why string) bool {
		s.Log.Printf("retire: %s; aborting the sweep and remounting %d swept mount(s)", why, len(swept))
		for _, m := range swept {
			if resp := s.handleMount(m.mountRequest()); !resp.OK {
				s.Log.Printf("retire: remount %s after aborted sweep: %s (still journaled; the consumer or a successor heals it)", m.Dir, resp.Error)
			}
		}
		return false
	}
	for _, m := range mounts {
		// Re-verify attest freshness right before the irreversible teardown:
		// the gate ran at tick start, and an attestation may have expired (or
		// the consumer come back to life) mid-sweep.
		if m.IdlePolicy != fusekit.IdlePolicyProbe && !s.attestFresh(m.Dir, m.Owner, time.Now()) {
			return abort("idle attestation for " + m.Dir + " expired mid-sweep")
		}
		release, ok := s.claim(m.Dir)
		if !ok {
			return abort("busy: another operation is in flight on " + m.Dir)
		}
		var rootRelease func()
		if m.MuxRoot != "" {
			if rootRelease, ok = s.claim(m.MuxRoot); !ok {
				release()
				return abort("busy: another operation is in flight on mux root " + m.MuxRoot)
			}
		}
		_, registered := s.registered(m.Dir)
		var err error
		if registered {
			s.drain(m.Dir)
			err = s.Host.Teardown(m.Base, m.Dir, m.CarcassPolicy)
			// A mux tenant's only teardown error source is the last-child
			// native-root unmount, AFTER the tenant detached — so on error its
			// row is a lie either way. Drop it (a plain mount's restored
			// handle keeps ITS row honest) and count the tenant as swept, so
			// the abort re-attaches it into the surviving root.
			if err == nil || m.MuxRoot != "" {
				// Drop the row but NOT the journal entry (deregister would).
				s.mu.Lock()
				delete(s.registry, m.Dir)
				s.mu.Unlock()
			}
		}
		if rootRelease != nil {
			rootRelease()
		}
		release()
		if err != nil {
			if registered && m.MuxRoot != "" {
				swept = append(swept, m)
			}
			return abort(fmt.Sprintf("graceful unmount of %s refused (%v)", m.Dir, err))
		}
		if registered {
			s.Log.Printf("retire: drained %s", m.Dir)
			swept = append(swept, m)
		}
	}
	// A mount that slipped past the dispatch gate before retiring flipped may
	// have landed (or still be in flight) after the snapshot; abort so the
	// next tick drains it too instead of exiting under it.
	s.mu.Lock()
	late := len(s.inflight) > 0 || len(s.registry) > 0
	s.mu.Unlock()
	if late {
		return abort("an operation landed mid-drain")
	}
	return true
}

// SkewCheck builds the RetireSkew detector for a self-owning holder. The cask
// holder (running from HolderExe) compares its compiled-in version — pass
// version.Version — with the installed bundle's Info.plist
// CFBundleShortVersionString; a private, cask-less holder keys on the hash of
// its executable file, which an in-place upgrade replaces. A "dev" build (or
// an empty version) never skews.
func SkewCheck(compiled string) func() (skewed bool, reason string, err error) {
	exe, exeErr := os.Executable()
	if exeErr != nil {
		return func() (bool, string, error) {
			return false, "", fmt.Errorf("resolve executable: %w", exeErr)
		}
	}
	if exe == HolderExe || strings.HasPrefix(exe, HolderApp+string(os.PathSeparator)) {
		return plistSkew(compiled, filepath.Join(HolderApp, "Contents", "Info.plist"))
	}
	return exeHashSkew(exe)
}

// plistSkew detects skew between the compiled-in version and the installed
// bundle's CFBundleShortVersionString; both sides normalize a leading "v".
func plistSkew(compiled, plistPath string) func() (bool, string, error) {
	compiled = strings.TrimPrefix(compiled, "v")
	return func() (bool, string, error) {
		if compiled == "" || compiled == "dev" {
			return false, "", nil
		}
		installed, err := readBundleShortVersion(plistPath)
		if err != nil {
			return false, "", fmt.Errorf("read installed bundle version: %w", err)
		}
		installed = strings.TrimPrefix(installed, "v")
		if installed == compiled {
			return false, "", nil
		}
		return true, fmt.Sprintf("installed bundle is v%s, this holder is v%s", installed, compiled), nil
	}
}

// exeHashSkew detects skew for a cask-less holder: the executable file's hash
// at first check is the baseline; a later differing hash means the binary was
// replaced on disk by an upgrade.
func exeHashSkew(exe string) func() (bool, string, error) {
	baseline, baseErr := fileHash(exe)
	return func() (bool, string, error) {
		if baseErr != nil {
			return false, "", fmt.Errorf("hash executable baseline: %w", baseErr)
		}
		cur, err := fileHash(exe)
		if err != nil {
			return false, "", fmt.Errorf("hash executable: %w", err)
		}
		if cur == baseline {
			return false, "", nil
		}
		return true, fmt.Sprintf("holder binary %s was replaced on disk", exe), nil
	}
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// readBundleShortVersion extracts CFBundleShortVersionString from an XML
// Info.plist (the format the release workflow writes). A binary plist — or a
// missing key — is an error; RetireSkew fails safe on it (no retire).
func readBundleShortVersion(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	dec := xml.NewDecoder(f)
	wantNext := false
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", path, err)
		}
		el, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch el.Name.Local {
		case "key":
			var k string
			if err := dec.DecodeElement(&k, &el); err != nil {
				return "", fmt.Errorf("parse %s: %w", path, err)
			}
			wantNext = k == "CFBundleShortVersionString"
		case "string":
			if !wantNext {
				continue
			}
			var v string
			if err := dec.DecodeElement(&v, &el); err != nil {
				return "", fmt.Errorf("parse %s: %w", path, err)
			}
			return v, nil
		default:
			wantNext = false
		}
	}
	return "", fmt.Errorf("no CFBundleShortVersionString in %s", path)
}
