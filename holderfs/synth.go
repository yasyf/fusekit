package holderfs

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/fusekit/content"
)

// synthFhBase is the first handle ID for synthetic read handles, above real
// kernel fds (small ints) and probe handles ([1<<61, 1<<62)).
const synthFhBase = uint64(1) << 62

func synthFh(fh uint64) bool { return fh >= synthFhBase && fh != ^uint64(0) }

// synthView serves one synthetic entry whose bytes the consumer computes over the
// bridge. Reads serve a holder-side cached snapshot refreshed OFF the fuse handler
// path, so a hung consumer is stale-but-served, never a parked fuse-t worker. A
// writable open passes through to a durable local file (writePath); its close
// schedules a background write-through. Merge/split lives consumer-side, so this
// view holds no domain knowledge.
type synthView struct {
	name      string // entry name the consumer knows (e.g. ".claude.json")
	domain    string
	client    *content.BridgeClient
	writePath string   // durable local backing for writable opens
	freshness []string // local files whose (mtime,size) gate the cached bytes

	mu       sync.Mutex
	cacheSig string
	cacheBuf []byte
	cacheOK  bool
	readErr  error
	writeErr error
	dirtyFds map[uint64]struct{}

	refreshing     bool
	refreshPending bool

	wtRunning bool
	wtPending bool
	wtIdle    chan struct{}
}

func newSynthView(name, domain string, client *content.BridgeClient, writePath string, freshness []string) *synthView {
	return &synthView{
		name:      name,
		domain:    domain,
		client:    client,
		writePath: writePath,
		freshness: freshness,
		dirtyFds:  map[uint64]struct{}{},
	}
}

// freshSig digests the freshness files' (mtime, size). An empty signature (no
// freshness paths, or none stattable) reads as always-stale, so every access
// schedules a refresh — correct, just RPC-heavier.
func (v *synthView) freshSig() string {
	var b strings.Builder
	any := false
	for _, p := range v.freshness {
		if fi, err := os.Lstat(p); err == nil {
			any = true
			fmt.Fprintf(&b, "%d:%d;", fi.ModTime().UnixNano(), fi.Size())
		} else {
			b.WriteString("-;")
		}
	}
	if !any {
		return ""
	}
	return b.String()
}

// currentBytes returns the cached snapshot, scheduling an off-handler refresh
// when the freshness signature has changed or the cache is cold. It NEVER blocks
// on the bridge: a stale or cold cache returns the last-good bytes (ok=false
// when the consumer has never answered).
func (v *synthView) currentBytes() ([]byte, bool) {
	sig := v.freshSig()
	v.mu.Lock()
	fresh := v.cacheOK && sig != "" && v.cacheSig == sig
	buf, ok := v.cacheBuf, v.cacheOK
	v.mu.Unlock()
	if !fresh {
		v.scheduleRefresh()
	}
	return buf, ok
}

// scheduleRefresh starts the background refresh worker, coalescing a request
// arriving mid-refresh into one more pass.
func (v *synthView) scheduleRefresh() {
	v.mu.Lock()
	if v.refreshing {
		v.refreshPending = true
		v.mu.Unlock()
		return
	}
	v.refreshing = true
	v.mu.Unlock()
	go v.refreshLoop()
}

func (v *synthView) refreshLoop() {
	for {
		v.refreshOnce()
		v.mu.Lock()
		if v.refreshPending {
			v.refreshPending = false
			v.mu.Unlock()
			continue
		}
		v.refreshing = false
		v.mu.Unlock()
		return
	}
}

// refreshOnce fetches the synth bytes over the bridge and replaces the cache. It
// runs only on the worker goroutine or Build's pre-warm, never a fuse handler.
// The signature is sampled before the read, so a file changing during the read
// leaves the next access stale and reschedules.
func (v *synthView) refreshOnce() {
	sig := v.freshSig()
	buf, err := v.client.Read(context.Background(), v.domain, v.name)
	v.mu.Lock()
	logIt := err != nil && (v.readErr == nil || v.readErr.Error() != err.Error())
	if err != nil {
		v.readErr = err
	} else {
		v.cacheBuf, v.cacheSig, v.cacheOK, v.readErr = buf, sig, true, nil
	}
	v.mu.Unlock()
	if logIt {
		log.Printf("holderfs: read %s/%s: %v", v.domain, v.name, err)
	}
}

// markDirty records that a real writePath fd mutated the file, so its Release
// runs the write-through. A write-capable fd that never wrote stays clean.
func (v *synthView) markDirty(fh uint64) {
	v.mu.Lock()
	v.dirtyFds[fh] = struct{}{}
	v.mu.Unlock()
}

// takeDirty reports whether fh was dirty and clears the flag — kernel fds are
// reused, so the flag must not outlive the Release.
func (v *synthView) takeDirty(fh uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, dirty := v.dirtyFds[fh]
	delete(v.dirtyFds, fh)
	return dirty
}

// scheduleWriteThrough requests a write-through and returns at once — the bridge RPC
// runs on the worker, never a fuse handler, so a hung consumer cannot wedge the
// mount. A commit arriving mid-cycle coalesces into one more cycle, which re-reads
// the durable file so the latest state wins.
func (v *synthView) scheduleWriteThrough() {
	v.mu.Lock()
	if v.wtRunning {
		v.wtPending = true
		v.mu.Unlock()
		return
	}
	v.wtRunning = true
	v.mu.Unlock()
	go v.writeThroughLoop()
}

func (v *synthView) writeThroughLoop() {
	for {
		err := v.writeThrough()
		v.mu.Lock()
		logIt := err != nil && (v.writeErr == nil || v.writeErr.Error() != err.Error())
		v.writeErr = err
		pending := v.wtPending
		if pending {
			v.wtPending = false
		} else {
			v.wtRunning = false
			if v.wtIdle != nil {
				close(v.wtIdle)
				v.wtIdle = nil
			}
		}
		v.mu.Unlock()
		if logIt {
			log.Printf("holderfs: write-through %s/%s: %v", v.domain, v.name, err)
		}
		if pending {
			continue
		}
		return
	}
}

// writeThrough re-reads the durable local file and ships it to the consumer, which
// persists it however the domain requires. A missing file is a no-op: the consumer
// owns the source of truth.
func (v *synthView) writeThrough() error {
	payload, err := os.ReadFile(v.writePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("write-through %s/%s: read backing: %w", v.domain, v.name, err)
	}
	return v.client.Write(context.Background(), v.domain, v.name, payload)
}

// flushWithin waits up to d for an in-flight write-through to drain, so teardown sees
// the last write reach the consumer. The bound only guards a genuinely stuck consumer
// (the file read and bounded RPC cannot hang on a wedged mirror). d <= 0 waits
// indefinitely.
func (v *synthView) flushWithin(d time.Duration) bool {
	v.mu.Lock()
	if !v.wtRunning {
		v.mu.Unlock()
		return true
	}
	if v.wtIdle == nil {
		v.wtIdle = make(chan struct{})
	}
	ch := v.wtIdle
	v.mu.Unlock()

	if d <= 0 {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}
