//go:build darwin

// mux.go — the single-mount-multiplexing workload driver for the disposable-VM
// panic harness (scripts/vm/scenarios/validate-mux.sh). It stands up ONE native
// fuse-t mount whose subtrees are N source-mode tenants (MountSpec.MuxRoot),
// drives the claude-shaped xattr/rename/mmap churn against ALL tenants
// concurrently — the nfs_vinvalbuf2 reproducer shape, now over one go-nfsv4 for
// the whole pool — and asserts the mux-specific invariants the feature exists to
// hold: per-tenant synth isolation, client-fileid identity + re-attach coherence
// across attach/detach churn, detach-under-load isolation, and native reassembly.
//
// Every subcommand shares the deterministic muxLayout so a long-running
// `mux-serve` (which holds the content bridge) and the transient drill commands
// (which drive AddMount/Unmount against the same live holder) reconstruct
// byte-identical specs. Like every vmstress subcommand this refuses to run
// outside a VM (main's requireVM guard); the scenario owns the run window and
// the mount-table / go-nfsv4 process assertions.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/mountd"
	"golang.org/x/sys/unix"
)

const (
	// muxDefaultTenants is the floor the validate-mux gate requires (>= 3): one
	// native mount must serve several tenants at once for the isolation and
	// fileid-discipline invariants to mean anything.
	muxDefaultTenants = 3
	// muxReadyWait bounds a subtree coming (back) live after an attach; a slower
	// answer is a wedge, surfaced loud rather than hung.
	muxReadyWait = 20 * time.Second
	// muxGoneWait bounds a detached subtree's paths going ENOENT (noattrcache is
	// on for the mux root, so the client re-looks-up promptly).
	muxGoneWait = 8 * time.Second
)

// muxTenant is one subtree of the shared native mount: a distinct content domain
// (its own synth bytes and carve-out), a per-account private store, and the
// logical mountpoint the muxFS presents under the root.
type muxTenant struct {
	name        string // acct-NN
	domain      string // content domain: the account-dir identity string
	consumerDir string // bridge source-of-truth for the synth entry
	privateDir  string // PrivateRoot: private-synth writePath + exact-private files
	carveDir    string // per-tenant symlink carve-out target dir
	subtree     string // muxRoot/acct-NN — a logical subtree, never its own mount
}

// muxLayout is the deterministic on-guest layout shared by every mux subcommand,
// so mux-serve and the drills build identical specs and address the same bridge.
type muxLayout struct {
	state   string
	muxRoot string
	base    string // ONE shared base dir (the ~/.claude analogue)
	bridge  string
	tenants []muxTenant
}

func newMuxLayout(state, muxRoot string, n int) muxLayout {
	l := muxLayout{
		state:   state,
		muxRoot: muxRoot,
		base:    filepath.Join(state, "base"),
		bridge:  filepath.Join(state, "bridge.sock"),
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("acct-%02d", i+1)
		l.tenants = append(l.tenants, muxTenant{
			name:        name,
			domain:      filepath.Join(state, "accounts", name),
			consumerDir: filepath.Join(state, "consumer", name),
			privateDir:  filepath.Join(state, "private", name),
			carveDir:    filepath.Join(state, "carve", name),
			subtree:     filepath.Join(muxRoot, name),
		})
	}
	return l
}

// spec is one tenant's holder registration: a source-mode subtree of the shared
// MuxRoot. The synth entry is PRIVATE so its writePath lands in the per-tenant
// PrivateRoot (a public synth would back onto the shared base and alias every
// tenant's bytes) — the same shape cc-pool's merged .claude.json takes.
func (l muxLayout) spec(t muxTenant) fusekit.MountSpec {
	return fusekit.MountSpec{
		Base:            l.base,
		Dir:             t.subtree,
		Owner:           holderOwner,
		MuxRoot:         l.muxRoot,
		ContentSocket:   l.bridge,
		Domain:          t.domain,
		PrivateRoot:     t.privateDir,
		ContentMode:     fusekit.ContentModeSource,
		ProbePath:       "/" + probeName,
		PrivatePrefixes: []string{privateSynthName, privateExactName},
	}
}

// muxFlags registers the shared flag set every mux subcommand takes and returns
// the resolved layout.
func muxFlags(fs *flag.FlagSet, args []string) muxLayout {
	state := fs.String("state", filepath.Join(guestRoot(), "mux-stress"), "instance state dir (base/, consumer/, private/, carve/, bridge.sock)")
	muxRoot := fs.String("muxroot", filepath.Join(guestRoot(), "mux"), "the ONE native mountpoint whose subtrees are the tenants")
	tenants := fs.Int("tenants", muxDefaultTenants, "number of source-mode tenants attached to the mux root")
	// The subcommands that take extra flags register them before calling this.
	parse(fs, args)
	if *tenants < 1 {
		log.Fatalf("--tenants must be >= 1")
	}
	return newMuxLayout(*state, *muxRoot, *tenants)
}

// --- content source ---------------------------------------------------------

// muxDomainState is one tenant's consumer-side state behind the bridge: its
// durable synth copy and carve-out dir, plus a generation counter so every
// mount-side commit re-renders a different envelope (the refresh churn the panic
// loop rides).
type muxDomainState struct {
	consumerDir string
	carveDir    string

	mu  sync.Mutex
	gen int64
}

func (d *muxDomainState) generation() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.gen
}

// muxSource is the multi-domain content.Source behind the one shared bridge:
// every tenant is a distinct domain serving its OWN synth bytes and carve-out,
// which is exactly what makes cross-tenant leakage observable.
type muxSource struct {
	domains map[string]*muxDomainState
}

func newMuxSource(l muxLayout) *muxSource {
	s := &muxSource{domains: map[string]*muxDomainState{}}
	for _, t := range l.tenants {
		s.domains[t.domain] = &muxDomainState{consumerDir: t.consumerDir, carveDir: t.carveDir}
	}
	return s
}

func (s *muxSource) dom(domain string) (*muxDomainState, error) {
	d, ok := s.domains[domain]
	if !ok {
		return nil, fmt.Errorf("unknown domain %q", domain)
	}
	return d, nil
}

// Manifest serves a private synth entry (per-tenant bytes), an exact-private
// file, and a symlink carve-out into the per-tenant dir.
func (s *muxSource) Manifest(domain string) ([]content.Entry, error) {
	d, err := s.dom(domain)
	if err != nil {
		return nil, err
	}
	gen := strconv.FormatInt(d.generation(), 10)
	return []content.Entry{
		{Name: privateSynthName, Kind: content.EntrySynth, Version: gen, Private: true,
			Freshness: []string{filepath.Join(d.consumerDir, privateSynthName)}},
		{Name: privateExactName, Kind: content.EntryPrivate, Version: gen},
		{Name: sharedDirName, Kind: content.EntrySymlink, Version: gen, Target: d.carveDir},
	}, nil
}

// ReadSynth renders the per-tenant envelope; the Domain field it carries is what
// the isolation gate reads back through the mount to prove no cross-tenant leak.
func (s *muxSource) ReadSynth(domain, name string) ([]byte, error) {
	d, err := s.dom(domain)
	if err != nil {
		return nil, err
	}
	if name != privateSynthName {
		return nil, fmt.Errorf("read synth: unknown entry %q", name)
	}
	payload, err := os.ReadFile(filepath.Join(d.consumerDir, name))
	if err != nil {
		return nil, fmt.Errorf("read synth %s/%s: %w", domain, name, err)
	}
	buf, err := json.Marshal(envelope{Domain: domain, Name: name, Gen: d.generation(), Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("render %s/%s: %w", domain, name, err)
	}
	return append(buf, '\n'), nil
}

// WriteThrough persists a mount-side commit into the tenant's consumer copy and
// bumps its generation so the next render differs.
func (s *muxSource) WriteThrough(domain, name string, data []byte) error {
	d, err := s.dom(domain)
	if err != nil {
		return err
	}
	if name != privateSynthName {
		return fmt.Errorf("write through: unknown entry %q", name)
	}
	dst := filepath.Join(d.consumerDir, name)
	tmp := fmt.Sprintf("%s.wt.%d", dst, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write through %s/%s: %w", domain, name, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("write through %s/%s: %w", domain, name, err)
	}
	d.mu.Lock()
	d.gen++
	d.mu.Unlock()
	return nil
}

func (s *muxSource) Classify(name string) content.EntryKind {
	switch name {
	case privateSynthName:
		return content.EntrySynth
	case sharedDirName:
		return content.EntrySymlink
	default:
		return content.EntryPrivate
	}
}

// prepareMux wipes and reseeds the shared base and every tenant's per-domain
// state. It never touches the muxRoot mountpoint's contents — a stale native
// mount must be torn down through the holder, never RemoveAll'd into.
func prepareMux(l muxLayout) (*muxSource, error) {
	if err := os.RemoveAll(l.state); err != nil {
		return nil, fmt.Errorf("reset state %s: %w", l.state, err)
	}
	dirs := []string{l.base}
	for _, t := range l.tenants {
		dirs = append(dirs, t.consumerDir, t.privateDir, t.carveDir)
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", d, err)
		}
	}
	// The mountpoint's parent must exist; muxRoot itself is what fuse-t mounts
	// over. Never RemoveAll it (a live carcass unmounts through the holder).
	if err := os.MkdirAll(l.muxRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create mux root %s: %w", l.muxRoot, err)
	}
	// Shared passthrough backing (the ~/.claude bulk-I/O analogue): one scratch
	// file every tenant truncates/writes through its own subtree, and one 8 MiB
	// file every tenant's mmap reader maps — the held-page surface the panic
	// rides, now shared across the pool on one native mount.
	if err := os.WriteFile(filepath.Join(l.base, scratchName), bytes.Repeat([]byte{0xA5}, 16*1024), 0o644); err != nil {
		return nil, fmt.Errorf("seed scratch: %w", err)
	}
	if err := os.WriteFile(filepath.Join(l.base, mmapName), mmapPattern(), 0o644); err != nil {
		return nil, fmt.Errorf("seed mmap: %w", err)
	}

	src := newMuxSource(l)
	for i, t := range l.tenants {
		// Distinct per-tenant content: sized AND valued differently so a leak
		// across subtrees is unambiguous.
		payload := fmt.Sprintf(`{"tenant":%q,"slot":%d}`, t.name, i)
		if err := os.WriteFile(filepath.Join(t.consumerDir, privateSynthName), []byte(payload), 0o644); err != nil {
			return nil, fmt.Errorf("seed %s synth: %w", t.name, err)
		}
		note := fmt.Sprintf("carve-out backing for %s\n", t.name)
		if err := os.WriteFile(filepath.Join(t.carveDir, "note.txt"), []byte(note), 0o644); err != nil {
			return nil, fmt.Errorf("seed %s carve: %w", t.name, err)
		}
		creds := fmt.Sprintf("credentials for %s\n", t.name)
		if err := os.WriteFile(filepath.Join(t.privateDir, privateExactName), []byte(creds), 0o644); err != nil {
			return nil, fmt.Errorf("seed %s creds: %w", t.name, err)
		}
		// The private-synth writePath must hold the rendered envelope so Getattr
		// resolves it from the first access with no cold->warm size flap.
		env, err := src.ReadSynth(t.domain, privateSynthName)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(t.privateDir, privateSynthName), env, 0o644); err != nil {
			return nil, fmt.Errorf("seed %s synth writepath: %w", t.name, err)
		}
	}
	return src, nil
}

// --- holder plumbing --------------------------------------------------------

// muxHolderHost drives the shared holder app the push step installed at the
// production cask path (spawns it on the first AddMount).
func muxHolderHost(l muxLayout) *mountd.RemoteHost {
	socket := mountd.DefaultHolderSocket()
	return &mountd.RemoteHost{
		Socket:         socket,
		LogPath:        filepath.Join(l.state, "holder-spawn.log"),
		Args:           []string{"--socket", socket},
		ExecPath:       mountd.HolderExe,
		Owner:          holderOwner,
		CannotHostHint: "run `scripts/vm/vmctl push` to install the holder app into this guest",
	}
}

// muxClient is a raw client onto the running holder — the drills use it for the
// per-tenant Unmount/AddMount/List the RemoteHost facade does not expose.
func muxClient() *mountd.Client {
	return &mountd.Client{Socket: mountd.DefaultHolderSocket(), Owner: holderOwner}
}

// --- mux-serve --------------------------------------------------------------

// cmdMuxServe stands up the shared bridge and attaches every tenant as a subtree
// of ONE native mount, then holds the bridge alive until SIGTERM and detaches
// all (the last detach unmounts the native root). It is the long-running peer
// the transient drill commands drive against.
func cmdMuxServe(args []string) error {
	fs := flag.NewFlagSet("mux-serve", flag.ContinueOnError)
	l := muxFlags(fs, args)

	src, err := prepareMux(l)
	if err != nil {
		return err
	}
	host := muxHolderHost(l)
	cl := muxClient()

	// A crashed previous run can leave subtrees registered on a stale native
	// mount; detach them so this run attaches fresh over a clean root.
	for _, t := range l.tenants {
		if warn, err := host.RemoveMount(l.base, t.subtree); err == nil && warn != "" {
			log.Printf("WARNING: clear stale subtree %s: %s", t.name, warn)
		}
	}

	bridgeCtx, stopBridge := context.WithCancel(context.Background())
	defer stopBridge()
	bridgeErr := startMuxBridge(bridgeCtx, l, src)
	if err := waitMuxBridge(l, bridgeErr); err != nil {
		return err
	}

	for _, t := range l.tenants {
		if err := host.AddMount(l.spec(t)); err != nil {
			return fmt.Errorf("attach %s: %w", t.name, err)
		}
	}
	if err := assertOneNativeMount(l, cl); err != nil {
		return err
	}
	// The steady-state marker the scenario waits on before sampling the mount
	// table and go-nfsv4 process count.
	log.Printf("mux-serve: %d tenants attached on %s (build %s) — steady-state ready", len(l.tenants), l.muxRoot, buildString())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop()
	log.Printf("mux-serve: signal received; detaching %d tenants", len(l.tenants))
	// The bridge must outlive teardown so the holder drains pending write-through
	// before each subtree goes away.
	var tdErr error
	for _, t := range l.tenants {
		warn, err := host.RemoveMount(l.base, t.subtree)
		if err != nil && tdErr == nil {
			tdErr = fmt.Errorf("detach %s: %w", t.name, err)
		}
		if warn != "" {
			log.Printf("WARNING: detach %s: %s", t.name, warn)
		}
	}
	stopBridge()
	if err := <-bridgeErr; err != nil && tdErr == nil {
		tdErr = fmt.Errorf("bridge: %w", err)
	}
	if tdErr != nil {
		return tdErr
	}
	log.Printf("mux-serve: teardown complete")
	return nil
}

// startMuxBridge runs the shared multi-domain content bridge in the background.
func startMuxBridge(ctx context.Context, l muxLayout, src content.Source) <-chan error {
	server := &content.BridgeServer{Socket: l.bridge, Source: src, Version: "vmstress-mux " + buildString()}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Run(ctx) }()
	return errCh
}

// waitMuxBridge blocks until the bridge socket accepts, or its server died.
func waitMuxBridge(l muxLayout, errCh <-chan error) error {
	client := content.NewBridgeClient(l.bridge)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			return fmt.Errorf("bridge exited during startup: %w", err)
		default:
		}
		if client.Available() {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("bridge socket %s not accepting within 10s", l.bridge)
}

// assertOneNativeMount fails unless the holder lists exactly the expected tenant
// rows, every one a live subtree of the SAME MuxRoot (never its own mount). The
// scenario re-checks the kernel mount table and go-nfsv4 process count itself. It
// retries briefly: the per-subtree Live verdict can settle a beat after AddMount
// or a reassembly returns.
func assertOneNativeMount(l muxLayout, cl *mountd.Client) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := checkOneNativeMount(l, cl)
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// checkOneNativeMount is one shot of the mux-root invariant check.
func checkOneNativeMount(l muxLayout, cl *mountd.Client) error {
	mounts, err := cl.List()
	if err != nil {
		return fmt.Errorf("list mounts: %w", err)
	}
	want := map[string]bool{}
	for _, t := range l.tenants {
		want[t.subtree] = true
	}
	seen := map[string]bool{}
	for _, m := range mounts {
		if !want[m.Dir] {
			continue
		}
		if m.MuxRoot != l.muxRoot {
			return fmt.Errorf("subtree %s lists MuxRoot %q, want %q", m.Dir, m.MuxRoot, l.muxRoot)
		}
		if !m.Live {
			return fmt.Errorf("subtree %s is not live", m.Dir)
		}
		if fusekit.Mounted(m.Dir) {
			return fmt.Errorf("subtree %s is its own kernel mountpoint; want a logical subtree of the ONE native mount", m.Dir)
		}
		seen[m.Dir] = true
	}
	for _, t := range l.tenants {
		if !seen[t.subtree] {
			return fmt.Errorf("tenant %s missing a live subtree row (holder rows: %+v)", t.name, mounts)
		}
	}
	if !fusekit.Mounted(l.muxRoot) {
		return fmt.Errorf("mux root %s is not a mountpoint while tenants are attached", l.muxRoot)
	}
	return nil
}

// --- churn (the reproducer) -------------------------------------------------

// cmdMuxChurn drives claude-shaped write/xattr/stat/rename traffic against ALL
// tenant subtrees concurrently for the window — the nfs_vinvalbuf2 reproducer
// shape over one native mount. The scenario runs the self-restarting mmap
// readers alongside (the held-page half of the surface).
func cmdMuxChurn(args []string) error {
	fs := flag.NewFlagSet("mux-churn", flag.ContinueOnError)
	seconds := fs.Int("seconds", 60, "how long to churn; 0 runs until killed")
	l := muxFlags(fs, args)

	for _, t := range l.tenants {
		if _, err := os.Stat(filepath.Join(t.subtree, privateSynthName)); err != nil {
			return fmt.Errorf("tenant %s not serving (is mux-serve running?): %w", t.name, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *seconds > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, time.Duration(*seconds)*time.Second)
		defer stop()
	}
	dog := newWedgeWatchdog(wedgeLimit, "mux-churn")

	errCh := make(chan error, len(l.tenants))
	var wg sync.WaitGroup
	for _, t := range l.tenants {
		wg.Add(1)
		go func(t muxTenant) {
			defer wg.Done()
			if err := churnTenantLoop(ctx, t, dog); err != nil {
				select {
				case errCh <- fmt.Errorf("%s: %w", t.name, err):
				default:
				}
				cancel()
			}
		}(t)
		// A light concurrent reader per tenant populates open handles across the
		// atomic-save rewrites — the invalidation-under-open surface.
		wg.Add(1)
		go func(t muxTenant) {
			defer wg.Done()
			readTenantLoop(ctx, t)
		}(t)
	}
	wg.Wait()
	close(errCh)
	select {
	case err := <-errCh:
		return err
	default:
	}
	log.Printf("mux-churn: %d tenants churned to completion", len(l.tenants))
	return nil
}

// churnTenantLoop runs claude-shaped bursts against one subtree until ctx ends,
// beating the shared wedge watchdog each cycle. A hard file-op failure is fatal
// (the mux root wedged); xattr failures are tolerated (a mitigated holder mounts
// without namedattr, so the client fails xattr ops by design).
func churnTenantLoop(ctx context.Context, t muxTenant, dog *wedgeWatchdog) error {
	for i := 0; ; i++ {
		if err := muxChurnCycle(t.subtree, i); err != nil {
			return err
		}
		dog.beat()
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// muxChurnCycle is one claude-shaped burst against a subtree: hold a read handle
// open across an atomic-save of the private synth, storm stats, set
// provenance-style xattrs, truncate/rewrite the shared scratch file, re-read the
// held handle, and periodically list the root, read the carve-out, and pull the
// virtual probe.
func muxChurnCycle(dir string, i int) error {
	synth := filepath.Join(dir, privateSynthName)
	held, err := openRetryNotExist(synth)
	if err != nil {
		return fmt.Errorf("open %s: %w", synth, err)
	}
	defer held.Close()
	head := make([]byte, 512)
	if _, err := held.Read(head); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read held synth: %w", err)
	}

	if err := muxAtomicSave(dir, privateSynthName, i); err != nil {
		return err
	}

	for _, name := range []string{privateSynthName, privateExactName, scratchName, mmapName, probeName} {
		if _, err := statRetryNotExist(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
	}

	muxXattrBurst(dir, i)

	scratch := filepath.Join(dir, scratchName)
	if err := os.Truncate(scratch, int64(4096+(i%8)*4096)); err != nil {
		return fmt.Errorf("truncate scratch: %w", err)
	}
	f, err := os.OpenFile(scratch, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open scratch: %w", err)
	}
	if _, err := f.WriteAt([]byte("mux-scratch-write"), int64(i%4096)); err != nil {
		f.Close()
		return fmt.Errorf("write scratch: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close scratch: %w", err)
	}

	if _, err := held.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reread held synth: %w", err)
	}

	if i%10 == 9 {
		if _, err := os.ReadDir(dir); err != nil {
			return fmt.Errorf("readdir: %w", err)
		}
		if _, err := os.Readlink(filepath.Join(dir, sharedDirName)); err != nil {
			return fmt.Errorf("readlink carve-out: %w", err)
		}
	}
	if i%25 == 24 {
		if err := readProbe(dir); err != nil {
			return err
		}
	}
	return nil
}

// muxAtomicSave rewrites the private synth the way claude saves a config file:
// write a sibling temp (a private-prefix name, so it lands on the PrivateRoot
// volume beside the target) and rename it over the entry.
func muxAtomicSave(dir, name string, i int) error {
	payload := fmt.Sprintf(`{"writer":"mux-churn","pid":%d,"iter":%d,"pad":%q}`,
		os.Getpid(), i, strings.Repeat("x", 1+rand.IntN(16*1024)))
	dst := filepath.Join(dir, name)
	tmp := fmt.Sprintf("%s.tmp.%d.%d", dst, os.Getpid(), i)
	if err := os.WriteFile(tmp, []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	return nil
}

// muxXattrBurst issues the provenance-style xattr traffic implicated in the
// panics. Failures are ignored: on a mitigated holder the client fails xattr ops
// by design, while ordinary ops keep succeeding around them.
func muxXattrBurst(dir string, i int) {
	attrs := []struct {
		name string
		val  []byte
	}{
		{"com.apple.provenance", []byte{1, 0, byte(i), byte(i >> 8), 0xde, 0xad, 0xbe, 0xef}},
		{"com.apple.quarantine", []byte(fmt.Sprintf("0081;%08x;mux;", time.Now().Unix()))},
		{"org.fusekit.mux", []byte(fmt.Sprintf("cycle-%d", i))},
	}
	for _, target := range []string{privateSynthName, scratchName} {
		path := filepath.Join(dir, target)
		for _, a := range attrs {
			_ = unix.Setxattr(path, a.name, a.val, 0)
		}
		_, _ = unix.Listxattr(path, make([]byte, 4096))
		_, _ = unix.Getxattr(path, "org.fusekit.mux", make([]byte, 256))
	}
}

// readTenantLoop opens the synth and the big passthrough file in a loop, the
// concurrent open-handle population refresh churn lands on. Transient errors are
// tolerated: it is load, not an assertion.
func readTenantLoop(ctx context.Context, t muxTenant) {
	synth := filepath.Join(t.subtree, privateSynthName)
	big := filepath.Join(t.subtree, mmapName)
	for {
		if f, err := openRetryNotExist(synth); err == nil {
			_, _ = io.ReadAll(f)
			f.Close()
		}
		if f, err := os.Open(big); err == nil {
			_, _ = io.ReadAll(io.LimitReader(f, 256*1024))
			f.Close()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// --- assertions: isolation --------------------------------------------------

// cmdMuxIsolation proves per-tenant isolation while all tenants are attached:
// each subtree's synth serves its OWN domain's bytes (no cross-tenant leak),
// each carve-out symlink resolves to its OWN tenant's backing, and every synth
// fileid the client observes is distinct across tenants (never aliased). The
// client only ever sees go-nfsv4's own client fileids, never the handler's
// st_ino, so the handler-side SynthInoFloor slot discipline is defense-in-depth
// invisible here — pairwise-distinctness is the sole client-observable fileid
// invariant.
func cmdMuxIsolation(args []string) error {
	fs := flag.NewFlagSet("mux-isolation", flag.ContinueOnError)
	l := muxFlags(fs, args)

	synthBytes := map[string]string{}
	synthInos := map[string]uint64{}
	for _, t := range l.tenants {
		env, raw, err := readSynthEnvelope(t.subtree)
		if err != nil {
			return fmt.Errorf("%s: %w", t.name, err)
		}
		if env.Domain != t.domain {
			return fmt.Errorf("%s synth carries domain %q, want %q — cross-tenant leak", t.name, env.Domain, t.domain)
		}
		if prev, dup := firstKeyFor(synthBytes, raw); dup {
			return fmt.Errorf("%s and %s serve identical synth bytes — not isolated", t.name, prev)
		}
		synthBytes[t.name] = raw

		ino, err := statIno(filepath.Join(t.subtree, privateSynthName))
		if err != nil {
			return fmt.Errorf("%s stat synth: %w", t.name, err)
		}
		for other, oino := range synthInos {
			if oino == ino {
				return fmt.Errorf("%s and %s share synth fileid %d — slot remapping did not isolate them", t.name, other, ino)
			}
		}
		synthInos[t.name] = ino

		target, err := os.Readlink(filepath.Join(t.subtree, sharedDirName))
		if err != nil {
			return fmt.Errorf("%s readlink carve-out: %w", t.name, err)
		}
		if target != t.carveDir {
			return fmt.Errorf("%s carve-out resolves to %q, want its own %q", t.name, target, t.carveDir)
		}
		note, err := os.ReadFile(filepath.Join(t.subtree, sharedDirName, "note.txt"))
		if err != nil {
			return fmt.Errorf("%s read carve-out: %w", t.name, err)
		}
		if !strings.Contains(string(note), t.name) {
			return fmt.Errorf("%s carve-out note = %q, want it to name %s", t.name, note, t.name)
		}
		fmt.Printf("mux-isolation: %s synth fileid=%d (distinct across tenants) domain-scoped bytes + own carve-out OK\n", t.name, ino)
	}
	fmt.Printf("mux-isolation: PASS %d tenants isolated (distinct bytes, pairwise-distinct fileids, per-tenant carve-outs)\n", len(l.tenants))
	return nil
}

// --- assertions: fileid discipline ------------------------------------------

// cmdMuxFileids is the fileid-identity + re-attach-coherence drill: capture every
// tenant's synth fileid, then detach and re-attach ONE victim repeatedly while the
// other tenants churn. go-nfsv4 mints its OWN client fileids (the handler's st_ino
// never reaches the client) and REMINTS a path's fileid on re-lookup once a
// rename/xattr storm invalidates its dentry, so numeric fileid stability under
// churn is not a backend guarantee — a remint on the SAME path is benign ("file
// replaced" semantics). It is also PATH-KEYED across a subtree detach/re-attach
// within one native mount: the re-appearing victim at the same path reclaims its
// PRE-DETACH fileid, so demanding a fresh fileid on re-attach is structurally
// impossible. The one real client-layer hazard is a single fileid appearing on two
// DIFFERENT objects (subtrees) within one go-nfsv4 lifetime. So the drill asserts,
// per cycle:
//   - non-victims keep resolving and serving their OWN domain bytes, and their
//     observed fileids stay pairwise-distinct and never alias the victim's —
//     remints are folded onto the same identity, only a fileid already owned by a
//     different subtree fails (assertStable);
//   - one non-victim held QUIESCENT (no churn ever touches it) keeps the SAME
//     fileid across the victim's detach moment — the client-visible detach-
//     coupling measure, which is the actual mux risk and the only fileid fact
//     here that churn cannot invalidate;
//   - each victim re-attach goes through the SAME identity model as everyone else
//     (noteFileid): reclaiming its old fileid is expected and benign, only a fileid
//     already owned by a DIFFERENT subtree fails. The freshness the drill used to
//     demand is replaced by CONTENT COHERENCE — the victim's synth source is
//     mutated WHILE it is detached, and the re-attached victim must surface the NEW
//     authoritative bytes at the same path (assertReattachCoherent), never a stale
//     page cached under the reused fileid for the pre-detach incarnation.
//
// (The handler's slot remapping is defense-in-depth the client never sees.)
func cmdMuxFileids(args []string) error {
	fs := flag.NewFlagSet("mux-fileids", flag.ContinueOnError)
	cycles := fs.Int("cycles", 6, "detach/re-attach cycles for the victim tenant")
	l := muxFlags(fs, args)
	if len(l.tenants) < 2 {
		return fmt.Errorf("mux-fileids needs >= 2 tenants (one victim, >= 1 non-victim held quiescent)")
	}
	cl := muxClient()
	victim := l.tenants[len(l.tenants)-1]
	others := l.tenants[:len(l.tenants)-1]
	// One non-victim is held QUIESCENT: no churn ever touches it, so its client
	// fileid cannot remint and any move across a victim detach is real coupling.
	// The rest carry the background load the churn-tolerant checks run against
	// (with the default >= 3 tenants there is at least one of each).
	quiescent := others[0]
	churned := others[1:]

	// everSeen maps each client-observed synth fileid to the identity (owning
	// subtree) it belongs to, for THIS native mount's whole lifetime — the drill
	// never force-unmounts the root, so the go-nfsv4 fileid counter never restarts
	// here. Identity model: one subtree may accumulate MANY fileids over time (each
	// churn remint is folded back onto the same subtree), but a fileid must map to
	// at most ONE subtree — a second, different owner is the aliasing hazard. A
	// native remount (e.g. the reassembly drill) starts a fresh go-nfsv4 whose
	// counter restarts, so reuse across that boundary is expected; the map is built
	// per invocation and never carried across a remount, so that never fails here.
	everSeen := map[uint64]string{}
	for _, t := range l.tenants {
		ino, err := statIno(filepath.Join(t.subtree, privateSynthName))
		if err != nil {
			return fmt.Errorf("baseline %s: %w", t.name, err)
		}
		if err := noteFileid(everSeen, ino, t.subtree); err != nil {
			return fmt.Errorf("baseline %s: %w", t.name, err)
		}
	}

	// Concurrent churn on the non-victim tenants: the fileids must hold UNDER
	// load, not just at rest.
	ctx, cancel := context.WithCancel(context.Background())
	dog := newWedgeWatchdog(wedgeLimit, "mux-fileids-load")
	var wg sync.WaitGroup
	// Stop the background load, then reap its goroutines, on every exit path.
	defer func() {
		cancel()
		wg.Wait()
	}()
	for _, t := range churned {
		wg.Add(1)
		go func(t muxTenant) {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				if err := muxChurnCycle(t.subtree, i); err != nil {
					log.Printf("mux-fileids: load churn %s transient: %v", t.name, err)
				}
				dog.beat()
				time.Sleep(20 * time.Millisecond)
			}
		}(t)
	}

	quiescentSynth := filepath.Join(quiescent.subtree, privateSynthName)
	for c := 0; c < *cycles; c++ {
		// Snapshot the quiescent tenant's fileid immediately before the detach,
		// with nothing churning it, so the post-detach compare measures ONLY the
		// detach's client-visible coupling — no remint can muddy it.
		qBefore, err := statIno(quiescentSynth)
		if err != nil {
			return fmt.Errorf("cycle %d quiescent %s pre-detach stat: %w", c, quiescent.name, err)
		}
		if _, err := cl.Unmount(l.base, victim.subtree); err != nil {
			return fmt.Errorf("cycle %d detach %s: %w", c, victim.name, err)
		}
		if err := waitSubtreeGone(filepath.Join(victim.subtree, privateSynthName)); err != nil {
			return fmt.Errorf("cycle %d: %w", c, err)
		}
		qAfter, err := statIno(quiescentSynth)
		if err != nil {
			return fmt.Errorf("cycle %d quiescent %s post-detach stat: %w", c, quiescent.name, err)
		}
		if qAfter != qBefore {
			return fmt.Errorf("cycle %d: quiescent %s synth fileid moved %d -> %d across victim %s detach — client-visible detach coupling",
				c, quiescent.name, qBefore, qAfter, victim.name)
		}
		if err := noteFileid(everSeen, qAfter, quiescent.subtree); err != nil {
			return fmt.Errorf("cycle %d quiescent %s: %w", c, quiescent.name, err)
		}
		if err := assertStable(others, everSeen); err != nil {
			return fmt.Errorf("cycle %d (victim detached): %w", c, err)
		}

		// While the victim is detached, change the authoritative bytes its synth
		// will serve (the drill owns the content source's on-disk truth). Re-attach
		// must then surface the NEW content, not a stale page the client cached
		// under the fileid it reuses for the same path — the coherence half of the
		// re-attach contract that fileid freshness used to (wrongly) stand in for.
		wantContent, err := mutateDetachedVictim(victim, c)
		if err != nil {
			return fmt.Errorf("cycle %d: %w", c, err)
		}

		if err := cl.AddMount(l.spec(victim)); err != nil {
			return fmt.Errorf("cycle %d re-attach %s: %w", c, victim.name, err)
		}
		if err := waitSubtreeReady(filepath.Join(victim.subtree, privateSynthName)); err != nil {
			return fmt.Errorf("cycle %d: %w", c, err)
		}
		// Coherence: the re-attached victim serves its OWN domain's NEW bytes at the
		// same path — never a stale cached page from the pre-detach incarnation.
		if err := assertReattachCoherent(victim, wantContent); err != nil {
			return fmt.Errorf("cycle %d: %w", c, err)
		}
		// Identity: the re-attached victim's fileid goes through the SAME model as
		// everyone else. go-nfsv4 is path-keyed, so it typically reclaims its OLD
		// fileid — benign; only a fileid already owned by a DIFFERENT subtree fails.
		ino, err := statInoRetry(filepath.Join(victim.subtree, privateSynthName))
		if err != nil {
			return fmt.Errorf("cycle %d %s stat re-attached synth: %w", c, victim.name, err)
		}
		if err := noteFileid(everSeen, ino, victim.subtree); err != nil {
			return fmt.Errorf("cycle %d %s: %w", c, victim.name, err)
		}
		if err := assertStable(others, everSeen); err != nil {
			return fmt.Errorf("cycle %d (victim re-attached): %w", c, err)
		}
		fmt.Printf("mux-fileids: cycle %d victim=%s re-attached fileid=%d (identity-consistent, new content coherent; non-victims isolated, %s quiescent fileid held)\n",
			c, victim.name, ino, quiescent.name)
	}
	fmt.Printf("mux-fileids: PASS %d detach/re-attach cycles — no fileid aliased two objects, each re-attach served coherent new content, %s quiescent fileid held across every detach, non-victims stayed isolated under churn\n",
		*cycles, quiescent.name)
	return nil
}

// noteFileid records that client fileid ino was observed on subtree, enforcing the
// one invariant this drill actually guards: a fileid maps to at most ONE identity
// (subtree) per native-mount lifetime. A repeat on the SAME subtree is a benign
// remint — go-nfsv4 re-minted the path's fileid after churn invalidated its
// dentry — and is folded onto that identity; the same fileid surfacing on a
// DIFFERENT subtree is the aliasing hazard (one fileid on two objects) and fails.
func noteFileid(everSeen map[uint64]string, ino uint64, subtree string) error {
	if owner, ok := everSeen[ino]; ok && owner != subtree {
		return fmt.Errorf("synth fileid %d now on %s but already served on %s — one fileid aliasing two objects", ino, subtree, owner)
	}
	everSeen[ino] = subtree
	return nil
}

// assertStable is the churn-tolerant non-victim check. Under churn a synth's
// client fileid can remint at any moment (go-nfsv4 re-mints on re-lookup after a
// rename/xattr storm invalidates the dentry), so numeric fileid stability is NOT
// asserted. Instead every non-victim must still resolve, still serve its OWN
// domain bytes (no cross-tenant leak), and expose a fileid that does not alias a
// DIFFERENT identity: a changed fileid on the same path is folded back onto that
// subtree as a benign remint, and only a fileid already owned by another subtree
// (the victim's included, since its ids live in everSeen too) fails.
func assertStable(nonVictims []muxTenant, everSeen map[uint64]string) error {
	for _, t := range nonVictims {
		env, _, err := readSynthEnvelopeRetry(t.subtree)
		if err != nil {
			return fmt.Errorf("non-victim %s: %w", t.name, err)
		}
		if env.Domain != t.domain {
			return fmt.Errorf("non-victim %s synth carries domain %q, want %q — cross-tenant leak under churn", t.name, env.Domain, t.domain)
		}
		ino, err := statInoRetry(filepath.Join(t.subtree, privateSynthName))
		if err != nil {
			return fmt.Errorf("stat non-victim %s: %w", t.name, err)
		}
		if err := noteFileid(everSeen, ino, t.subtree); err != nil {
			return fmt.Errorf("non-victim %s: %w", t.name, err)
		}
	}
	return nil
}

// mutateDetachedVictim rewrites the authoritative bytes the victim's synth will
// serve while it is DETACHED. The drill owns the content source's on-disk truth —
// the tenant's consumer copy the bridge's ReadSynth renders from, listed as the
// synth's Freshness gate. The rewrite is ATOMIC (sibling temp + rename), matching
// how real consumers save (claude's atomic config saves): no observer — the
// holder's freshness stat, the bridge's ReadSynth, a scenario reader — can ever
// see a truncate window, so a torn/empty read of the source is structurally
// impossible rather than merely unscheduled. The consumer dir is OUTSIDE the
// mount, so the rename never enters the mount's silly-rename diversion territory.
// It returns the exact payload the re-attached victim must surface. The payload's
// byte length is INTENTIONALLY identical every cycle (a one-digit cycle index and
// a fixed-width 19-digit UnixNano nonce): with go-nfsv4 reclaiming the path's
// fileid across re-attach and the size equal, only the served mtime/change
// attribution can tell the client the file changed — the hard case the re-attach
// coherence contract must hold under. (Cycle 0 alone is soft: its payload differs
// in size from the seed envelope, so the client's size-delta invalidation would
// mask a repeated change signal.) Do not "improve" the payload to vary in length.
func mutateDetachedVictim(t muxTenant, cycle int) ([]byte, error) {
	want := []byte(fmt.Sprintf(`{"tenant":%q,"reattach-coherence":%d,"nonce":%d}`, t.name, cycle, time.Now().UnixNano()))
	dst := filepath.Join(t.consumerDir, privateSynthName)
	tmp := fmt.Sprintf("%s.mutate.%d.%d", dst, os.Getpid(), cycle)
	if err := os.WriteFile(tmp, want, 0o644); err != nil {
		return nil, fmt.Errorf("mutate detached %s synth source: %w", t.name, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return nil, fmt.Errorf("mutate detached %s synth source: %w", t.name, err)
	}
	return want, nil
}

// assertReattachCoherent proves the re-attached victim serves its OWN domain's NEW
// authoritative bytes at the same path — the real client-layer hazard a
// detach/re-attach exposes, since go-nfsv4 typically reclaims the victim's
// pre-detach fileid and the kernel could serve pages cached under it for the prior
// incarnation. It polls the synth envelope through the mount until the domain is
// correct AND the payload equals what mutateDetachedVictim wrote: a re-attach's
// first reads legitimately return the stale-but-served writePath seed until the
// holder's off-handler refresh lands the consumer's new bytes, so the poll is the
// mechanism, not a workaround. On timeout the verdict distinguishes the three
// failure classes a bare "last payload" conflates: reads that never parsed at all
// (broken reads: persistent ENOENT/EIO/torn bytes — the error and read counts say
// which), a parsed envelope for the wrong domain (cross-tenant leak), and a
// parsed envelope with stale or empty payload (cache incoherence), with the raw
// bytes retained as evidence.
func assertReattachCoherent(t muxTenant, want []byte) error {
	deadline := time.Now().Add(muxReadyWait)
	var last envelope
	var lastRaw string
	var lastErr error
	okReads, badReads := 0, 0
	for time.Now().Before(deadline) {
		env, raw, err := readSynthEnvelope(t.subtree)
		if err != nil {
			badReads++
			lastErr = err
		} else {
			okReads++
			last, lastRaw = env, raw
			if env.Domain == t.domain && bytes.Equal(env.Payload, want) {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if okReads == 0 {
		return fmt.Errorf("re-attached %s never parsed a synth envelope within %s (%d reads, every one failed; last error: %v) — reads through the mount are broken across the detach/re-attach boundary, not merely stale",
			t.name, muxReadyWait, badReads, lastErr)
	}
	if last.Domain != "" && last.Domain != t.domain {
		return fmt.Errorf("re-attached %s serves domain %q, want %q — cross-tenant leak on re-attach", t.name, last.Domain, t.domain)
	}
	return fmt.Errorf("re-attached %s never surfaced its new authoritative content within %s (%d clean reads, %d failed reads, last read error %v; last raw %q; last payload %q, want %q) — stale or torn content across the detach/re-attach boundary",
		t.name, muxReadyWait, okReads, badReads, lastErr, lastRaw, last.Payload, want)
}

// --- assertions: detach under load ------------------------------------------

// cmdMuxDetachLoad proves a logical detach is isolated: tenant A holds an open
// file and an mmap while tenant B detaches. A's I/O must continue unaffected, B's
// paths must go ENOENT, and B must re-attach and serve again — all with no kernel
// unmount (the scenario re-checks the go-nfsv4 process count around this).
func cmdMuxDetachLoad(args []string) error {
	fs := flag.NewFlagSet("mux-detach-load", flag.ContinueOnError)
	seconds := fs.Int("seconds", 20, "how long to hold A's I/O while B is detached")
	l := muxFlags(fs, args)
	if len(l.tenants) < 2 {
		return fmt.Errorf("mux-detach-load needs >= 2 tenants")
	}
	cl := muxClient()
	a := l.tenants[0]
	b := l.tenants[len(l.tenants)-1]

	// A holds an open synth handle and an mmap over the big passthrough file
	// across B's detach.
	held, err := os.Open(filepath.Join(a.subtree, privateSynthName))
	if err != nil {
		return fmt.Errorf("open A synth: %w", err)
	}
	defer held.Close()
	mfile, err := os.Open(filepath.Join(a.subtree, mmapName))
	if err != nil {
		return fmt.Errorf("open A mmap file: %w", err)
	}
	defer mfile.Close()
	mfi, err := mfile.Stat()
	if err != nil {
		return fmt.Errorf("stat A mmap file: %w", err)
	}
	m, err := unix.Mmap(int(mfile.Fd()), 0, int(mfi.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap A: %w", err)
	}
	defer func() { _ = unix.Munmap(m) }()

	if _, err := cl.Unmount(l.base, b.subtree); err != nil {
		return fmt.Errorf("detach B (%s): %w", b.name, err)
	}
	if err := waitSubtreeGone(filepath.Join(b.subtree, privateSynthName)); err != nil {
		return fmt.Errorf("B did not go ENOENT after detach: %w", err)
	}
	fmt.Printf("mux-detach-load: %s detached; paths ENOENT while %s holds open file + mmap\n", b.name, a.name)

	dog := newWedgeWatchdog(wedgeLimit, "mux-detach-load")
	deadline := time.Now().Add(time.Duration(*seconds) * time.Second)
	head := make([]byte, 512)
	var sum uint64
	for time.Now().Before(deadline) {
		if _, err := held.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("A held-handle read failed while B detached: %w", err)
		}
		for off := 0; off < len(m); off += pageSize {
			sum += uint64(m[off])
		}
		if _, err := os.ReadFile(filepath.Join(a.subtree, privateSynthName)); err != nil {
			return fmt.Errorf("A fresh read failed while B detached: %w", err)
		}
		if _, err := os.Stat(filepath.Join(b.subtree, privateSynthName)); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("B path answered %v while detached, want ENOENT", err)
		}
		dog.beat()
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("mux-detach-load: %s I/O unaffected through B's detach (mmap checksum %d)\n", a.name, sum)

	// B re-attaches and serves its own bytes again.
	if err := cl.AddMount(l.spec(b)); err != nil {
		return fmt.Errorf("re-attach B: %w", err)
	}
	if err := waitSubtreeReady(filepath.Join(b.subtree, privateSynthName)); err != nil {
		return fmt.Errorf("B did not come back: %w", err)
	}
	env, _, err := readSynthEnvelope(b.subtree)
	if err != nil {
		return fmt.Errorf("B read after re-attach: %w", err)
	}
	if env.Domain != b.domain {
		return fmt.Errorf("re-attached B serves domain %q, want %q", env.Domain, b.domain)
	}
	fmt.Printf("mux-detach-load: PASS %s re-attached and serves its own bytes\n", b.name)
	return nil
}

// --- assertions: reassembly -------------------------------------------------

// cmdMuxReassemble runs the native-wedge recovery drill: the scenario has
// force-unmounted the native root out from under the holder, so re-issuing every
// tenant's Mount RPC must remount the root exactly once and re-attach every
// tenant serving its own bytes — the self-reassembly the heal loop relies on.
func cmdMuxReassemble(args []string) error {
	fs := flag.NewFlagSet("mux-reassemble", flag.ContinueOnError)
	l := muxFlags(fs, args)
	cl := muxClient()

	if fusekit.Mounted(l.muxRoot) {
		return fmt.Errorf("mux root %s is still mounted — force-unmount must precede reassembly", l.muxRoot)
	}
	for _, t := range l.tenants {
		if err := cl.AddMount(l.spec(t)); err != nil {
			return fmt.Errorf("re-issue Mount for %s: %w", t.name, err)
		}
	}
	if !fusekit.Mounted(l.muxRoot) {
		return fmt.Errorf("mux root %s did not remount after re-issuing tenant mounts", l.muxRoot)
	}
	for _, t := range l.tenants {
		if err := waitSubtreeReady(filepath.Join(t.subtree, privateSynthName)); err != nil {
			return fmt.Errorf("%s did not re-attach: %w", t.name, err)
		}
		env, _, err := readSynthEnvelope(t.subtree)
		if err != nil {
			return fmt.Errorf("%s read after reassembly: %w", t.name, err)
		}
		if env.Domain != t.domain {
			return fmt.Errorf("reassembled %s serves domain %q, want %q", t.name, env.Domain, t.domain)
		}
		if _, err := os.ReadFile(filepath.Join(t.subtree, sharedDirName, "note.txt")); err != nil {
			return fmt.Errorf("%s carve-out broken after reassembly: %w", t.name, err)
		}
	}
	if err := assertOneNativeMount(l, cl); err != nil {
		return fmt.Errorf("post-reassembly: %w", err)
	}
	fmt.Printf("mux-reassemble: PASS root remounted once, %d tenants re-attached and serving\n", len(l.tenants))
	return nil
}

// --- shared helpers ---------------------------------------------------------

// statIno returns a path's fileid.
func statIno(path string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, err
	}
	return uint64(st.Ino), nil
}

// statInoRetry stats a fileid, tolerating the transient ENOENT a rename-over
// silly-rename gap opens under churn.
func statInoRetry(path string) (uint64, error) {
	var err error
	for i := 0; i < notExistRetries; i++ {
		var ino uint64
		if ino, err = statIno(path); err == nil {
			return ino, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return 0, fmt.Errorf("still absent after %d attempts: %w", notExistRetries, err)
}

// readSynthEnvelope reads a subtree's private synth through the mount and parses
// the rendered envelope, returning both the parsed form and the raw bytes.
func readSynthEnvelope(subtree string) (envelope, string, error) {
	raw, err := os.ReadFile(filepath.Join(subtree, privateSynthName))
	if err != nil {
		return envelope{}, "", fmt.Errorf("read synth: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return envelope{}, "", fmt.Errorf("parse synth envelope %q: %w", raw, err)
	}
	return env, string(raw), nil
}

// readSynthEnvelopeRetry reads a subtree's synth envelope, tolerating the
// transient read/parse failure a concurrent atomic-save's rename-over opens under
// churn (ENOENT in the silly-rename gap, or a momentarily unrenderable read). A
// clean parse carrying an EMPTY payload is treated the same way: no envelope in
// this harness legitimately renders one (every seed and mutation is non-empty),
// so an empty payload is a torn render or a zero-page artifact — transient at
// worst, a real defect if it persists — never success. A persistently unreadable
// or empty synth still fails. The domain check its caller runs happens only after
// a clean parse, so a genuine cross-tenant leak is never swallowed as a transient.
func readSynthEnvelopeRetry(subtree string) (envelope, string, error) {
	var err error
	for i := 0; i < notExistRetries; i++ {
		var env envelope
		var raw string
		if env, raw, err = readSynthEnvelope(subtree); err == nil {
			if len(env.Payload) > 0 {
				return env, raw, nil
			}
			err = fmt.Errorf("synth envelope carries an empty payload (raw %q)", raw)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return envelope{}, "", fmt.Errorf("synth unreadable after %d attempts: %w", notExistRetries, err)
}

// firstKeyFor reports whether val already appears in m and, if so, one key
// holding it, so the isolation gate can name the two tenants serving identical
// synth bytes.
func firstKeyFor(m map[string]string, val string) (string, bool) {
	for k, v := range m {
		if v == val {
			return k, true
		}
	}
	return "", false
}

// waitSubtreeReady waits for a detached-then-reattached path to answer a stat.
func waitSubtreeReady(path string) error {
	deadline := time.Now().Add(muxReadyWait)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s did not become live within %s", path, muxReadyWait)
}

// waitSubtreeGone waits for a detached subtree path to go ENOENT.
func waitSubtreeGone(path string) error {
	deadline := time.Now().Add(muxGoneWait)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%s did not go ENOENT within %s after detach", path, muxGoneWait)
}
