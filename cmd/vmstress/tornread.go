//go:build darwin

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// tornread is the attribute-cache torn-read gate. With `-o attrcache` the
// go-nfsv4 server answers GETATTR from its own cache, so a stale cached size
// can clamp a client read after the served bytes changed — truncating the
// document. Every through-mount read of a synth entry must therefore parse as
// a COMPLETE envelope (a clamped read cuts the JSON mid-token and fails to
// unmarshal) and its Gen must never regress. The carve-out symlink must keep
// resolving alongside. In --writer mode an external consumer-side writer
// alternates a small and a large payload (the shapes a stale-size clamp
// punishes hardest) and the reader measures how long a committed rewrite can
// stay unobserved through the mount — the staleness bound the gate records.
const (
	// tornSmall/tornLarge are the alternating writer payload fills: the size
	// swing (256x) guarantees a stale-size clamp truncates mid-JSON instead of
	// landing on a plausible boundary.
	tornSmall = 1 << 10
	tornLarge = 256 << 10

	// tornWriteEvery paces the external writer; slower than the refresh path so
	// consecutive seqs are individually observable, fast enough to cross many
	// attrcache TTL windows in one gate run.
	tornWriteEvery = 500 * time.Millisecond

	// tornConvergeBound caps how stale a through-mount read may be in --writer
	// mode: attrcache TTL (5s in the scenario) + client attr cache (~5s) +
	// holder refresh latency, with generous headroom. Beyond it the cache is
	// not converging — a coherence failure, not jitter.
	tornConvergeBound = 30 * time.Second
)

// tornPayload is the writer-mode payload: Fill's declared length pins the
// document's size so validation catches a clamp even when the JSON happens to
// stay parseable.
type tornPayload struct {
	Seq  int64  `json:"seq"`
	N    int    `json:"n"`
	Fill string `json:"fill"`
}

// validateEnvelope proves one through-mount read is a complete, coherent
// envelope: it must unmarshal (a stale-size clamp cuts JSON mid-token), Gen
// must not regress below floor, and a writer-mode payload must carry exactly
// its declared fill. It returns the envelope's Gen and the payload seq (0 when
// the payload is not writer-shaped — churn traffic reads as seq 0 and skips
// staleness accounting).
func validateEnvelope(buf []byte, floor int64) (gen, seq int64, err error) {
	var env envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		return 0, 0, fmt.Errorf("torn read: envelope does not parse (%d bytes): %w", len(buf), err)
	}
	if env.Gen < floor {
		return 0, 0, fmt.Errorf("gen regressed: read %d after %d", env.Gen, floor)
	}
	var p tornPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil || p.Seq == 0 {
		return env.Gen, 0, nil // not writer-shaped (seed or churn payload) — envelope integrity was the check
	}
	if len(p.Fill) != p.N {
		return 0, 0, fmt.Errorf("torn read: seq %d declares %d fill bytes, carries %d", p.Seq, p.N, len(p.Fill))
	}
	return env.Gen, p.Seq, nil
}

// tornWriter rewrites the consumer-side synth copy the way an external editor
// would — atomic tmp+rename, no bridge, no gen bump — alternating small and
// large fills, and records each seq's commit time for the reader's staleness
// accounting.
type tornWriter struct {
	path string

	mu       sync.Mutex
	commits  map[int64]time.Time
	lastSeq  atomic.Int64
	rewrites int
}

func (w *tornWriter) rewrite() error {
	seq := w.lastSeq.Load() + 1
	n := tornSmall
	if seq%2 == 0 {
		n = tornLarge
	}
	buf, err := json.Marshal(tornPayload{Seq: seq, N: n, Fill: strings.Repeat("t", n)})
	if err != nil {
		return fmt.Errorf("render seq %d: %w", seq, err)
	}
	tmp := fmt.Sprintf("%s.torn.%d", w.path, seq)
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("write seq %d: %w", seq, err)
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("commit seq %d: %w", seq, err)
	}
	w.mu.Lock()
	w.commits[seq] = time.Now()
	w.rewrites++
	w.mu.Unlock()
	w.lastSeq.Store(seq)
	return nil
}

func (w *tornWriter) commitTime(seq int64) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.commits[seq]
	return t, ok
}

func cmdTornread(args []string) error {
	fs := flag.NewFlagSet("tornread", flag.ContinueOnError)
	dir := fs.String("dir", filepath.Join(guestRoot(), "mnt"), "live mountpoint to validate")
	state := fs.String("state", filepath.Join(guestRoot(), "stress"), "serve state dir (locates the consumer copy for --writer)")
	seconds := fs.Int("seconds", 30, "how long to validate")
	writer := fs.Bool("writer", false, "drive external consumer-side rewrites and measure the staleness bound")
	parse(fs, args)

	p := newPaths(*state, *dir)
	for _, name := range []string{synthName, privateSynthName} {
		if _, err := os.Stat(filepath.Join(p.dir, name)); err != nil {
			return fmt.Errorf("mount not serving (is `vmstress serve` running?): %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	end := time.Now().Add(time.Duration(*seconds) * time.Second)
	dog := newWedgeWatchdog(wedgeLimit, "tornread")

	var w *tornWriter
	var writerErr error
	var wg sync.WaitGroup
	if *writer {
		w = &tornWriter{path: filepath.Join(p.consumer, synthName), commits: map[int64]time.Time{}}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(tornWriteEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := w.rewrite(); err != nil {
						writerErr = err
						cancel()
						return
					}
				}
			}
		}()
	}

	// The reader validates both synth entries and the carve-out symlink every
	// pass; the gate's verdict is "no read was ever torn", so the first failure
	// aborts loudly.
	genFloor := map[string]int64{}
	var passes int
	var maxSeq int64
	var maxStale time.Duration
	sharedNote := filepath.Join(p.dir, sharedDirName, "note.txt")
	for time.Now().Before(end) && ctx.Err() == nil {
		for _, name := range []string{synthName, privateSynthName} {
			buf, err := os.ReadFile(filepath.Join(p.dir, name))
			if err != nil {
				cancel()
				wg.Wait()
				return fmt.Errorf("read %s: %w", name, err)
			}
			gen, seq, err := validateEnvelope(buf, genFloor[name])
			if err != nil {
				cancel()
				wg.Wait()
				return fmt.Errorf("%s pass %d: %w", name, passes, err)
			}
			genFloor[name] = gen
			if w != nil && seq > maxSeq {
				maxSeq = seq
				if t, ok := w.commitTime(seq); ok {
					if stale := time.Since(t); stale > maxStale {
						maxStale = stale
					}
				}
			}
		}
		if _, err := os.Lstat(filepath.Join(p.dir, sharedDirName)); err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("carve-out lstat: %w", err)
		}
		if _, err := os.ReadFile(sharedNote); err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("carve-out read-through: %w", err)
		}
		passes++
		dog.beat()
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if writerErr != nil {
		return fmt.Errorf("writer: %w", writerErr)
	}

	// Convergence barrier (--writer): the final committed rewrite must become
	// visible through the mount within the bound, or the cache is serving a
	// world that never catches up.
	if w != nil {
		final := w.lastSeq.Load()
		if final == 0 {
			return fmt.Errorf("writer made no commits in %ds — gate proved nothing", *seconds)
		}
		deadline := time.Now().Add(tornConvergeBound)
		converged := false
		for time.Now().Before(deadline) {
			buf, err := os.ReadFile(filepath.Join(p.dir, synthName))
			if err != nil {
				return fmt.Errorf("converge read: %w", err)
			}
			gen, seq, err := validateEnvelope(buf, genFloor[synthName])
			if err != nil {
				return fmt.Errorf("converge: %w", err)
			}
			genFloor[synthName] = gen
			if seq >= final {
				if t, ok := w.commitTime(seq); ok {
					if stale := time.Since(t); stale > maxStale {
						maxStale = stale
					}
				}
				converged = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !converged {
			return fmt.Errorf("stale beyond bound: seq %d not visible through the mount within %s", final, tornConvergeBound)
		}
		log.Printf("tornread done: passes=%d rewrites=%d max_seq_observed=%d max_staleness=%s (bound %s) torn=0",
			passes, w.rewrites, maxSeq, maxStale.Round(time.Millisecond), tornConvergeBound)
		return nil
	}
	log.Printf("tornread done: passes=%d torn=0 (concurrent mode)", passes)
	return nil
}
