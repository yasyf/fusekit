//go:build darwin

// Command vmstress is the in-guest workload driver for fusekit's disposable-VM
// panic harness (scripts/vm). It reproduces the traffic shape implicated in
// the macOS nfs_vinvalbuf2 kernel panics: synthetic-entry rewrites
// round-tripped through a content.Source over the bridge, provenance-style
// xattr writes, truncate/stat churn, and mmap readers holding pages over the
// NFS mount — all against the fusekit-holder app that vmctl push installed at
// the production cask path.
//
// Subcommands:
//
//	serve     host a minimal content.Source on a bridge socket and register a
//	          "source"-mode content mount on the shared holder (mountd
//	          AddMount); runs until SIGTERM, then tears the mount down
//	churn     drive claude-shaped load through the mount: atomic-save synth
//	          rewrites, xattr sets (com.apple.provenance included), truncate,
//	          stat storms, and parallel readers
//	read      read one file in a loop; --mmap maps it MAP_SHARED and touches
//	          every page each pass instead
//	tornread  the attr-cache torn-read gate: every through-mount read of the
//	          synth entries must parse as a complete envelope (a stale-size
//	          clamp truncates the JSON), Gen must never regress; --writer adds
//	          external consumer-side grow/shrink rewrites and measures how
//	          stale a through-mount read can be
//	mux-*     the single-mount multiplexing gate (validate-mux): mux-serve
//	          attaches N source-mode tenants as subtrees of ONE native mount,
//	          mux-churn drives the reproducer against all of them at once, and
//	          mux-isolation / mux-fileids / mux-detach-load / mux-reassemble
//	          assert the per-tenant isolation, slot-remapped fileid discipline,
//	          detach-under-load isolation, and native-root reassembly (mux.go)
//	selftest  end-to-end proof: serve + mount + read/write/mmap through the
//	          mount + verify + clean teardown; prints PASS or FAIL
//
// Every subcommand refuses to run outside a virtual machine (exit 86): the
// workloads exist to panic kernels, so this binary is inert on bare metal.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/version"
	"golang.org/x/sys/unix"
)

const (
	exitUsage = 64
	exitNotVM = 86

	vmDomain    = "vmstress"
	holderOwner = "vmstress"

	// Entry names mirror cc-pool's production shape: a base-backed synth entry
	// (the .claude.json analogue), a private synth entry, an exact-private
	// file, a live-symlink carve-out, and plain passthrough files.
	probeName        = ".stress-probe"
	synthName        = "config.json"
	privateSynthName = "settings.json"
	privateExactName = "credentials.json"
	sharedDirName    = "shared-dir"
	scratchName      = "scratch.dat"
	mmapName         = "mmap.dat"

	seedConfig      = `{"seed":"config"}`
	seedSettings    = `{"seed":"settings"}`
	seedCredentials = `{"seed":"credentials"}`
	seedSharedNote  = "shared carve-out backing\n"

	mmapFileBytes = 8 << 20
	// probeBytes is holderfs's fixed virtual probe size — a wire contract with
	// the holder (cc-pool pins the same constant for its deep probe).
	probeBytes = 2 << 20
	pageSize   = 4096

	// wedgeLimit bounds a single workload op: a wedged mount must surface as a
	// loud fast failure, never a silent hang that eats the run window (a hung
	// validate run would otherwise time out and read as clean).
	wedgeLimit = 90 * time.Second
)

func main() {
	requireVM()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("vmstress ")
	if len(os.Args) < 2 {
		usage()
		os.Exit(exitUsage)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(os.Args[2:])
	case "churn":
		err = cmdChurn(os.Args[2:])
	case "read":
		err = cmdRead(os.Args[2:])
	case "tornread":
		err = cmdTornread(os.Args[2:])
	case "mux-serve":
		err = cmdMuxServe(os.Args[2:])
	case "mux-churn":
		err = cmdMuxChurn(os.Args[2:])
	case "mux-isolation":
		err = cmdMuxIsolation(os.Args[2:])
	case "mux-fileids":
		err = cmdMuxFileids(os.Args[2:])
	case "mux-detach-load":
		err = cmdMuxDetachLoad(os.Args[2:])
	case "mux-reassemble":
		err = cmdMuxReassemble(os.Args[2:])
	case "selftest":
		err = cmdSelftest(os.Args[2:])
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		fmt.Fprintf(os.Stderr, "vmstress: unknown subcommand %q\n", os.Args[1])
		os.Exit(exitUsage)
	}
	if err != nil {
		log.Fatalf("%s: %v", os.Args[1], err)
	}
}

// requireVM is the hard bare-metal guard: every subcommand drives workloads
// that exist to panic kernels, so any host that is not a VM guest
// (kern.hv_vmm_present != 1) is refused with exit 86 before dispatch.
func requireVM() {
	v, err := unix.SysctlUint32("kern.hv_vmm_present")
	if err == nil && v == 1 {
		return
	}
	fmt.Fprintf(os.Stderr, "vmstress: REFUSING TO RUN OUTSIDE A VM (kern.hv_vmm_present=%d, err=%v)\n", v, err)
	fmt.Fprintln(os.Stderr, "vmstress drives deliberate kernel-panic workloads; run it only inside the disposable tart guest (scripts/vm/README.md)")
	os.Exit(exitNotVM)
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: vmstress <subcommand> [flags]

In-guest workload driver for fusekit's disposable-VM panic harness
(scripts/vm/README.md). Refuses to run outside a VM (exit 86).

subcommands:
  serve     host the stress content source and register the mount on the holder
  churn     drive claude-shaped churn through the mount (--once | --seconds N)
  read      read one file in a loop; --mmap holds a MAP_SHARED mapping
  tornread  validate every synth read is a complete envelope (torn/clamped
            read gate for --attrcache mounts); --writer adds external
            consumer-side rewrites with a measured staleness bound
  mux-serve      host the shared bridge and attach N source-mode tenants as
                 subtrees of ONE native mount (MountSpec.MuxRoot); until SIGTERM
  mux-churn      drive claude-shaped xattr/rename churn across ALL tenants at once
  mux-isolation  assert per-tenant synth bytes, carve-outs, and slot-remapped
                 fileids are isolated while all tenants are attached
  mux-fileids    detach/re-attach one tenant under load; assert no fileid aliases
                 two objects and a quiescent tenant's fileid holds across detach
  mux-detach-load  hold tenant A's open file + mmap while tenant B detaches;
                   assert A is unaffected and B goes ENOENT then re-serves
  mux-reassemble   after a native force-unmount, re-issue every Mount RPC and
                   assert the root remounts once with all tenants serving
  selftest  end-to-end serve+mount+verify+teardown; prints PASS or FAIL
`)
}

// parse applies the harness usage-error convention (exit 64) to a flag set.
func parse(fs *flag.FlagSet, args []string) {
	err := fs.Parse(args)
	if err == nil {
		return
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	os.Exit(exitUsage)
}

// guestRoot is the in-guest install dir vmctl push populates (~/fusekit-vm).
func guestRoot() string {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("resolve home dir: %v", err)
	}
	return filepath.Join(home, "fusekit-vm")
}

// paths lays out one stress instance: the consumer-side state tree and the
// holder-served mountpoint.
type paths struct {
	state    string // instance root: base/, private/, consumer/, bridge.sock
	dir      string // mountpoint served by the holder
	base     string
	private  string
	consumer string
	bridge   string
}

func newPaths(state, dir string) paths {
	return paths{
		state:    state,
		dir:      dir,
		base:     filepath.Join(state, "base"),
		private:  filepath.Join(state, "private"),
		consumer: filepath.Join(state, "consumer"),
		bridge:   filepath.Join(state, "bridge.sock"),
	}
}

// spec is the holder registration: a "source"-mode content mount mirroring
// cc-pool's production wiring — synth entries served over the bridge, a
// private root, a virtual wedge probe, and private prefixes covering the
// private entries' atomic-save temps.
func (p paths) spec() fusekit.MountSpec {
	return fusekit.MountSpec{
		Base:            p.base,
		Dir:             p.dir,
		Owner:           holderOwner,
		ContentSocket:   p.bridge,
		Domain:          vmDomain,
		PrivateRoot:     p.private,
		ContentMode:     "source",
		ProbePath:       "/" + probeName,
		PrivatePrefixes: []string{privateSynthName, privateExactName},
	}
}

// holderHost drives the shared holder app that vmctl push installed at the
// production cask path. Version stays empty on purpose: a tenant never
// version-replaces a shared holder.
func holderHost(p paths) *mountd.RemoteHost {
	socket := mountd.DefaultHolderSocket()
	return &mountd.RemoteHost{
		Socket:         socket,
		LogPath:        filepath.Join(p.state, "holder-spawn.log"),
		Args:           []string{"--socket", socket},
		ExecPath:       mountd.HolderExe,
		Owner:          holderOwner,
		CannotHostHint: "run `scripts/vm/vmctl push` to install the holder app into this guest",
	}
}

// prepare wipes and reseeds the instance state. It never touches the
// mountpoint's contents: a stale dead mount must be torn down through the
// holder, never RemoveAll'd into.
func prepare(p paths) (*stressSource, error) {
	if err := os.RemoveAll(p.state); err != nil {
		return nil, fmt.Errorf("reset state %s: %w", p.state, err)
	}
	for _, d := range []string{p.base, p.private, p.consumer, filepath.Join(p.consumer, sharedDirName), p.dir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", d, err)
		}
	}
	src := newStressSource(p.consumer)
	seeds := map[string][]byte{
		filepath.Join(p.consumer, synthName):                 []byte(seedConfig),
		filepath.Join(p.consumer, privateSynthName):          []byte(seedSettings),
		filepath.Join(p.private, privateExactName):           []byte(seedCredentials),
		filepath.Join(p.consumer, sharedDirName, "note.txt"): []byte(seedSharedNote),
		filepath.Join(p.base, scratchName):                   bytes.Repeat([]byte{0xA5}, 16*1024),
		filepath.Join(p.base, mmapName):                      mmapPattern(),
	}
	for path, data := range seeds {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return nil, fmt.Errorf("seed %s: %w", path, err)
		}
	}
	// The shared carve-out needs a real Base entry so Readdir lists it; the
	// holder overrides its Getattr/Readlink presentation from the manifest.
	if err := os.Symlink(filepath.Join(p.consumer, sharedDirName), filepath.Join(p.base, sharedDirName)); err != nil {
		return nil, fmt.Errorf("seed %s: %w", sharedDirName, err)
	}
	// The synth writePaths hold the last-committed bytes; seed them with the
	// rendered envelopes so both entries resolve from the first Getattr.
	for name, dir := range map[string]string{synthName: p.base, privateSynthName: p.private} {
		env, err := src.ReadSynth(vmDomain, name)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, name), env, 0o644); err != nil {
			return nil, fmt.Errorf("seed %s: %w", name, err)
		}
	}
	return src, nil
}

func mmapPattern() []byte {
	buf := make([]byte, mmapFileBytes)
	for i := range buf {
		buf[i] = byte(i*31 + 7)
	}
	return buf
}

// stressSource is the minimal consumer behind the bridge. Each synth entry
// renders as an envelope over the consumer-side durable copy plus a
// write-through generation counter, so every mount-side commit changes both
// the entry's freshness file and its rendered size — the refresh churn the
// panic loop rides, produced by the same mechanism cc-pool's merge uses.
type stressSource struct {
	consumerDir string

	mu  sync.Mutex
	gen int64
}

func newStressSource(consumerDir string) *stressSource {
	return &stressSource{consumerDir: consumerDir}
}

// envelope is the deterministic rendered form of a synth entry: fixed for a
// given consumer state, changed by every WriteThrough.
type envelope struct {
	Domain  string `json:"domain"`
	Name    string `json:"name"`
	Gen     int64  `json:"gen"`
	Payload []byte `json:"payload"`
}

func (s *stressSource) checkDomain(domain string) error {
	if domain != vmDomain {
		return fmt.Errorf("unknown domain %q (want %q)", domain, vmDomain)
	}
	return nil
}

func (s *stressSource) generation() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// Manifest describes all four entry kinds a production consumer serves: two
// synth entries (one private), one exact-private file, one symlink carve-out.
func (s *stressSource) Manifest(domain string) ([]content.Entry, error) {
	if err := s.checkDomain(domain); err != nil {
		return nil, err
	}
	gen := strconv.FormatInt(s.generation(), 10)
	return []content.Entry{
		{Name: synthName, Kind: content.EntrySynth, Version: gen,
			Freshness: []string{filepath.Join(s.consumerDir, synthName)}},
		{Name: privateSynthName, Kind: content.EntrySynth, Version: gen, Private: true,
			Freshness: []string{filepath.Join(s.consumerDir, privateSynthName)}},
		{Name: privateExactName, Kind: content.EntryPrivate, Version: gen},
		{Name: sharedDirName, Kind: content.EntrySymlink, Version: gen,
			Target: filepath.Join(s.consumerDir, sharedDirName)},
	}, nil
}

// ReadSynth renders the entry envelope from the consumer copy.
func (s *stressSource) ReadSynth(domain, name string) ([]byte, error) {
	if err := s.checkDomain(domain); err != nil {
		return nil, err
	}
	if name != synthName && name != privateSynthName {
		return nil, fmt.Errorf("read synth: unknown entry %q", name)
	}
	payload, err := os.ReadFile(filepath.Join(s.consumerDir, name))
	if err != nil {
		return nil, fmt.Errorf("read synth %s: %w", name, err)
	}
	buf, err := json.Marshal(envelope{Domain: domain, Name: name, Gen: s.generation(), Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("render %s: %w", name, err)
	}
	return append(buf, '\n'), nil
}

// WriteThrough persists a mount-side commit into the consumer copy (atomic
// tmp+rename, which also advances the entry's freshness file) and bumps the
// generation so the next render differs.
func (s *stressSource) WriteThrough(domain, name string, data []byte) error {
	if err := s.checkDomain(domain); err != nil {
		return err
	}
	if name != synthName && name != privateSynthName {
		return fmt.Errorf("write through: unknown entry %q", name)
	}
	dst := filepath.Join(s.consumerDir, name)
	tmp := fmt.Sprintf("%s.wt.%d", dst, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write through %s: %w", name, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("write through %s: %w", name, err)
	}
	s.mu.Lock()
	s.gen++
	s.mu.Unlock()
	return nil
}

// Classify mirrors the manifest; unknown names read as consumer-private.
func (s *stressSource) Classify(name string) content.EntryKind {
	switch name {
	case synthName, privateSynthName:
		return content.EntrySynth
	case sharedDirName:
		return content.EntrySymlink
	default:
		return content.EntryPrivate
	}
}

// startBridge runs the content bridge in the background, returning its
// terminal-error channel.
func startBridge(ctx context.Context, p paths, src content.Source) <-chan error {
	server := &content.BridgeServer{Socket: p.bridge, Source: src, Version: "vmstress " + version.String()}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Run(ctx) }()
	return errCh
}

// waitBridge blocks until the bridge socket accepts, or its server died.
func waitBridge(p paths, errCh <-chan error) error {
	client := content.NewBridgeClient(p.bridge)
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
	return fmt.Errorf("bridge socket %s not accepting within 10s", p.bridge)
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	state := fs.String("state", filepath.Join(guestRoot(), "stress"), "instance state dir (base/, private/, consumer/, bridge.sock)")
	dir := fs.String("dir", filepath.Join(guestRoot(), "mnt"), "mountpoint the holder serves")
	attrCache := fs.Bool("attrcache", false, "opt the mount into the go-nfsv4 attribute cache (validate-attrcache gate)")
	attrTimeout := fs.Duration("attrcache-timeout", 0, "attribute-cache TTL forwarded to go-nfsv4 (whole seconds; needs --attrcache)")
	parse(fs, args)

	p := newPaths(*state, *dir)
	src, err := prepare(p)
	if err != nil {
		return err
	}
	host := holderHost(p)
	// A crashed previous run leaves a mount whose bridge is gone; tear it down
	// so this run serves fresh bytes.
	if err := host.Teardown(p.base, p.dir); err != nil {
		return fmt.Errorf("clear stale mount: %w", err)
	}

	bridgeCtx, stopBridge := context.WithCancel(context.Background())
	defer stopBridge()
	bridgeErr := startBridge(bridgeCtx, p, src)
	if err := waitBridge(p, bridgeErr); err != nil {
		return err
	}

	spec := p.spec()
	spec.AttrCache = *attrCache
	spec.AttrCacheTimeout = *attrTimeout
	if err := host.AddMount(spec); err != nil {
		return fmt.Errorf("register mount: %w", err)
	}
	log.Printf("serving %s (base %s, bridge %s, attrcache=%v timeout=%s, build %s)", p.dir, p.base, p.bridge, *attrCache, *attrTimeout, version.String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	stop()
	log.Printf("signal received; tearing down %s", p.dir)
	// The bridge must outlive the teardown: the holder drains pending
	// write-through over it before the mount goes away.
	if err := host.Teardown(p.base, p.dir); err != nil {
		return fmt.Errorf("teardown: %w", err)
	}
	stopBridge()
	if err := <-bridgeErr; err != nil {
		return fmt.Errorf("bridge: %w", err)
	}
	log.Printf("teardown complete")
	return nil
}

// wedgeWatchdog aborts the process when the workload stops making progress: a
// single NFS op wedging forever must surface as a loud fast failure, never a
// silent hang that eats the run window.
type wedgeWatchdog struct {
	last atomic.Int64
}

func newWedgeWatchdog(limit time.Duration, what string) *wedgeWatchdog {
	w := &wedgeWatchdog{}
	w.beat()
	go func() {
		ticker := time.NewTicker(limit / 4)
		defer ticker.Stop()
		for range ticker.C {
			if time.Since(time.Unix(0, w.last.Load())) > limit {
				log.Printf("FATAL: %s made no progress for %s — mount wedged; aborting", what, limit)
				os.Exit(1)
			}
		}
	}()
	return w
}

func (w *wedgeWatchdog) beat() { w.last.Store(time.Now().UnixNano()) }

func cmdChurn(args []string) error {
	fs := flag.NewFlagSet("churn", flag.ContinueOnError)
	dir := fs.String("dir", filepath.Join(guestRoot(), "mnt"), "live mountpoint to churn")
	seconds := fs.Int("seconds", 60, "how long to churn; 0 runs until killed (ignored with --once)")
	once := fs.Bool("once", false, "run exactly one churn cycle and exit")
	readers := fs.Int("readers", 4, "parallel open/read/close workers")
	parse(fs, args)

	c := &churner{dir: *dir}
	if _, err := os.Stat(filepath.Join(*dir, synthName)); err != nil {
		return fmt.Errorf("mount not serving (is `vmstress serve` running?): %w", err)
	}

	var end time.Time
	if !*once && *seconds > 0 {
		end = time.Now().Add(time.Duration(*seconds) * time.Second)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dog := newWedgeWatchdog(wedgeLimit, "churn")

	readerErrs := make(chan error, *readers)
	var wg sync.WaitGroup
	for i := 0; i < *readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.readWorker(ctx, *once); err != nil {
				readerErrs <- err
				cancel()
			}
		}()
	}

	var loopErr error
	for i := 0; ; i++ {
		if err := c.cycle(i); err != nil {
			loopErr = err
			break
		}
		dog.beat()
		if *once || ctx.Err() != nil {
			break
		}
		if !end.IsZero() && !time.Now().Before(end) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	close(readerErrs)
	for err := range readerErrs {
		if loopErr == nil {
			loopErr = fmt.Errorf("reader: %w", err)
		}
	}
	log.Printf("churn done: %s", c.summary())
	return loopErr
}

// churner drives the claude-shaped write/stat/xattr traffic through the mount.
// File-op failures are fatal (the scenario owns recovery); xattr failures are
// counted, never fatal — a mitigated holder mounts without namedattr, so the
// macOS client fails xattr ops (and the blocked AppleDouble fallback surfaces
// EACCES) by design, while ordinary ops must keep succeeding around them.
type churner struct {
	dir string

	cycles, saves, xattrOK, xattrErr int
	lastXattrErr                     string
	reads                            atomic.Int64
}

// cycle is one claude-shaped burst: hold a read handle open across an
// atomic-save rewrite of the synth entry, storm stats (the probe included),
// set provenance-style xattrs, truncate and rewrite the scratch file, and
// periodically commit the private synth, list the root, and pull the 2 MiB
// probe like a deep-probe pass.
func (c *churner) cycle(i int) error {
	c.cycles++
	synth := filepath.Join(c.dir, synthName)

	held, err := openRetryNotExist(synth)
	if err != nil {
		return fmt.Errorf("open %s: %w", synth, err)
	}
	defer held.Close()
	head := make([]byte, 512)
	if _, err := held.Read(head); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read held %s: %w", synth, err)
	}

	if err := c.atomicSave(synthName, i); err != nil {
		return err
	}
	c.saves++

	for _, name := range []string{synthName, privateSynthName, privateExactName, scratchName, mmapName, probeName} {
		if _, err := statRetryNotExist(filepath.Join(c.dir, name)); err != nil {
			return fmt.Errorf("stat %s: %w", name, err)
		}
	}

	c.xattrBurst(i)

	scratch := filepath.Join(c.dir, scratchName)
	if err := os.Truncate(scratch, int64(4096+(i%8)*4096)); err != nil {
		return fmt.Errorf("truncate %s: %w", scratch, err)
	}
	f, err := os.OpenFile(scratch, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open scratch: %w", err)
	}
	if _, err := f.WriteAt([]byte("vmstress-scratch-write"), int64(i%4096)); err != nil {
		f.Close()
		return fmt.Errorf("write scratch: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close scratch: %w", err)
	}

	// Re-read through the held handle: on an unmitigated holder the rewrite
	// above flips size/mtime/ino under this open file — the invalidation the
	// panic rides.
	if _, err := held.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reread held %s: %w", synth, err)
	}

	if i%10 == 9 {
		if err := c.atomicSave(privateSynthName, i); err != nil {
			return err
		}
		c.saves++
		if _, err := os.ReadDir(c.dir); err != nil {
			return fmt.Errorf("readdir root: %w", err)
		}
		if _, err := os.Readlink(filepath.Join(c.dir, sharedDirName)); err != nil {
			return fmt.Errorf("readlink %s: %w", sharedDirName, err)
		}
	}
	if i%25 == 24 {
		if err := readProbe(c.dir); err != nil {
			return err
		}
	}
	return nil
}

// atomicSave rewrites a synth entry the way claude saves .claude.json: write a
// sibling temp, rename it over the entry. The rename is what schedules the
// holder's write-through; the payload size varies so the rendered size churns.
func (c *churner) atomicSave(name string, i int) error {
	payload := fmt.Sprintf(`{"writer":"churn","pid":%d,"iter":%d,"pad":%q}`,
		os.Getpid(), i, strings.Repeat("x", 1+rand.IntN(32*1024)))
	dst := filepath.Join(c.dir, name)
	tmp := fmt.Sprintf("%s.tmp.%d.%d", dst, os.Getpid(), i)
	if err := os.WriteFile(tmp, []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

// xattrBurst issues the xattr traffic implicated in the panics, headlined by
// provenance-style com.apple.provenance sets, plus read-side list/get calls.
func (c *churner) xattrBurst(i int) {
	attrs := []struct {
		name string
		val  []byte
	}{
		{"com.apple.provenance", []byte{1, 0, byte(i), byte(i >> 8), 0xde, 0xad, 0xbe, 0xef}},
		{"com.apple.quarantine", []byte(fmt.Sprintf("0081;%08x;vmstress;", time.Now().Unix()))},
		{"org.fusekit.vmstress", []byte(fmt.Sprintf("cycle-%d", i))},
	}
	for _, target := range []string{synthName, scratchName} {
		path := filepath.Join(c.dir, target)
		for _, a := range attrs {
			if err := unix.Setxattr(path, a.name, a.val, 0); err != nil {
				c.noteXattrErr(fmt.Sprintf("setxattr %s %s: %v", target, a.name, err))
				continue
			}
			c.xattrOK++
		}
		if _, err := unix.Listxattr(path, make([]byte, 4096)); err != nil {
			c.noteXattrErr(fmt.Sprintf("listxattr %s: %v", target, err))
		} else {
			c.xattrOK++
		}
		if _, err := unix.Getxattr(path, "org.fusekit.vmstress", make([]byte, 256)); err != nil {
			c.noteXattrErr(fmt.Sprintf("getxattr %s: %v", target, err))
		} else {
			c.xattrOK++
		}
	}
}

func (c *churner) noteXattrErr(msg string) {
	c.xattrErr++
	c.lastXattrErr = msg
}

// readWorker loops opens over the synth entry and the big passthrough file,
// holding each briefly — the concurrent open-handle population that refresh
// churn lands on.
func (c *churner) readWorker(ctx context.Context, once bool) error {
	synth := filepath.Join(c.dir, synthName)
	big := filepath.Join(c.dir, mmapName)
	for {
		if err := readFull(synth, 50*time.Millisecond); err != nil {
			return err
		}
		if err := readPrefix(big, 256*1024); err != nil {
			return err
		}
		c.reads.Add(1)
		if once {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// readFull reads the whole file, holding the handle open for hold afterward.
func readFull(path string, hold time.Duration) error {
	f, err := openRetryNotExist(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	if _, err := io.ReadAll(f); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	time.Sleep(hold)
	return nil
}

// notExistRetries bounds the tolerance for transient ENOENT on a rename-over
// target: the NFS client silly-renames an open target away for an instant
// before the commit rename lands, so a hot-looping open/stat can catch the
// gap. A persistent miss still fails loud.
const notExistRetries = 4

// openRetryNotExist opens the file, retrying only ErrNotExist briefly.
func openRetryNotExist(path string) (*os.File, error) {
	var err error
	for i := 0; i < notExistRetries; i++ {
		var f *os.File
		if f, err = os.Open(path); err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("still absent after %d attempts: %w", notExistRetries, err)
}

// statRetryNotExist stats the file, retrying only ErrNotExist briefly.
func statRetryNotExist(path string) (os.FileInfo, error) {
	var err error
	for i := 0; i < notExistRetries; i++ {
		var fi os.FileInfo
		if fi, err = os.Stat(path); err == nil {
			return fi, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("still absent after %d attempts: %w", notExistRetries, err)
}

// readPrefix reads the first n bytes of the file.
func readPrefix(path string, n int) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	if _, err := io.ReadAll(io.LimitReader(f, int64(n))); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

// readProbe pulls the full 2 MiB virtual probe file — the deep-probe read
// shape (rwsize=1 MiB, so multiple NFS READs of freshly minted bytes).
func readProbe(dir string) error {
	buf, err := os.ReadFile(filepath.Join(dir, probeName))
	if err != nil {
		return fmt.Errorf("read probe: %w", err)
	}
	if len(buf) != probeBytes {
		return fmt.Errorf("probe read %d bytes, want %d", len(buf), probeBytes)
	}
	return nil
}

func (c *churner) summary() string {
	s := fmt.Sprintf("cycles=%d saves=%d parallel_reads=%d xattr_ok=%d xattr_err=%d",
		c.cycles, c.saves, c.reads.Load(), c.xattrOK, c.xattrErr)
	if c.lastXattrErr != "" {
		s += " last_xattr_err=" + strconv.Quote(c.lastXattrErr)
	}
	return s
}

func cmdRead(args []string) error {
	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	file := fs.String("file", filepath.Join(guestRoot(), "mnt", mmapName), "file to read through the mount")
	useMmap := fs.Bool("mmap", false, "map the file MAP_SHARED and touch every page each pass")
	seconds := fs.Int("seconds", 60, "how long to read; 0 runs until killed")
	parse(fs, args)

	var deadline time.Time
	if *seconds > 0 {
		deadline = time.Now().Add(time.Duration(*seconds) * time.Second)
	}
	if *useMmap {
		return mmapReadLoop(*file, deadline)
	}
	return readLoop(*file, deadline)
}

// mmapReadLoop maps the file MAP_SHARED and touches every page each pass,
// re-statting between passes so fresh attributes land on held mapped pages —
// the ubc_msync invalidation surface behind nfs_vinvalbuf2. A file shrinking
// under the mapping can SIGBUS the touch and kill the process; the guest
// wrapper loop restarts it.
func mmapReadLoop(path string, deadline time.Time) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	size := int(fi.Size())
	if size == 0 {
		return fmt.Errorf("mmap %s: file is empty", path)
	}
	m, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap %s (%d bytes): %w", path, size, err)
	}
	defer func() { _ = unix.Munmap(m) }()
	log.Printf("mmap %s: %d bytes mapped", path, size)

	dog := newWedgeWatchdog(wedgeLimit, "mmap read")
	var passes int
	var sum uint64
	for deadline.IsZero() || time.Now().Before(deadline) {
		for off := 0; off < len(m); off += pageSize {
			sum += uint64(m[off])
		}
		passes++
		dog.beat()
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("stat between passes: %w", err)
		}
		time.Sleep(150 * time.Millisecond)
	}
	log.Printf("mmap read done: passes=%d checksum=%d", passes, sum)
	return nil
}

// readLoop reads the whole file each pass, with a stat in between.
func readLoop(path string, deadline time.Time) error {
	dog := newWedgeWatchdog(wedgeLimit, "read")
	var passes int
	var total int64
	for deadline.IsZero() || time.Now().Before(deadline) {
		buf, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		total += int64(len(buf))
		passes++
		dog.beat()
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("stat between passes: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Printf("read done: passes=%d bytes=%d", passes, total)
	return nil
}

func cmdSelftest(args []string) error {
	fs := flag.NewFlagSet("selftest", flag.ContinueOnError)
	state := fs.String("state", filepath.Join(guestRoot(), "selftest", "stress"), "selftest state dir")
	dir := fs.String("dir", filepath.Join(guestRoot(), "selftest", "mnt"), "selftest mountpoint")
	parse(fs, args)

	if err := runSelftest(newPaths(*state, *dir)); err != nil {
		fmt.Printf("vmstress selftest: FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("vmstress selftest: PASS")
	return nil
}

// runSelftest proves the whole stack in one process: bridge and holder mount
// up, all four entry kinds served, a write-through round trip, ordinary file
// ops, an mmap read, the probe, and a clean teardown. It must pass on ANY
// pushed build — mitigated or not — so it asserts no mitigation-specific
// behavior (the AppleDouble release gate lives in validate-mitigation.sh).
func runSelftest(p paths) error {
	src, err := prepare(p)
	if err != nil {
		return err
	}
	host := holderHost(p)
	// A failed earlier selftest leaves its mount up; clear it so this run
	// serves fresh bytes.
	if err := host.Teardown(p.base, p.dir); err != nil {
		return fmt.Errorf("clear stale mount: %w", err)
	}

	bridgeCtx, stopBridge := context.WithCancel(context.Background())
	defer stopBridge()
	bridgeErr := startBridge(bridgeCtx, p, src)
	if err := waitBridge(p, bridgeErr); err != nil {
		return err
	}

	if err := host.AddMount(p.spec()); err != nil {
		return fmt.Errorf("register mount: %w", err)
	}

	dog := newWedgeWatchdog(2*time.Minute, "selftest")
	steps := []struct {
		name string
		fn   func(paths, *stressSource) error
	}{
		{"entries-serve", checkEntries},
		{"root-listing", checkRootListing},
		{"write-through-round-trip", checkWriteThrough},
		{"ordinary-file-ops", checkOrdinaryOps},
		{"mmap-read", checkMmap},
		{"probe", checkProbe},
	}
	for _, s := range steps {
		if err := s.fn(p, src); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
		dog.beat()
		log.Printf("selftest: %s ok", s.name)
	}

	if err := host.Teardown(p.base, p.dir); err != nil {
		return fmt.Errorf("teardown: %w", err)
	}
	if fusekit.Mounted(p.dir) {
		return fmt.Errorf("%s is still a mountpoint after teardown", p.dir)
	}
	stopBridge()
	if err := <-bridgeErr; err != nil {
		return fmt.Errorf("bridge: %w", err)
	}
	return nil
}

// checkEntries reads every manifest entry kind through the live mount and
// compares against the source of truth.
func checkEntries(p paths, src *stressSource) error {
	for _, name := range []string{synthName, privateSynthName} {
		want, err := src.ReadSynth(vmDomain, name)
		if err != nil {
			return err
		}
		got, err := os.ReadFile(filepath.Join(p.dir, name))
		if err != nil {
			return fmt.Errorf("read %s through mount: %w", name, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%s through mount = %q, want rendered envelope %q", name, got, want)
		}
	}
	got, err := os.ReadFile(filepath.Join(p.dir, privateExactName))
	if err != nil {
		return fmt.Errorf("read %s through mount: %w", privateExactName, err)
	}
	if string(got) != seedCredentials {
		return fmt.Errorf("%s through mount = %q, want %q", privateExactName, got, seedCredentials)
	}
	target, err := os.Readlink(filepath.Join(p.dir, sharedDirName))
	if err != nil {
		return fmt.Errorf("readlink %s: %w", sharedDirName, err)
	}
	if want := filepath.Join(p.consumer, sharedDirName); target != want {
		return fmt.Errorf("%s target = %q, want %q", sharedDirName, target, want)
	}
	note, err := os.ReadFile(filepath.Join(p.dir, sharedDirName, "note.txt"))
	if err != nil {
		return fmt.Errorf("read through %s carve-out: %w", sharedDirName, err)
	}
	if string(note) != seedSharedNote {
		return fmt.Errorf("carve-out note = %q, want %q", note, seedSharedNote)
	}
	return nil
}

// checkRootListing asserts every entry lists through the mount and the
// virtual probe never does.
func checkRootListing(p paths, _ *stressSource) error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	seen := map[string]bool{}
	for _, e := range entries {
		seen[e.Name()] = true
	}
	for _, name := range []string{synthName, privateSynthName, privateExactName, sharedDirName, scratchName, mmapName} {
		if !seen[name] {
			return fmt.Errorf("%s missing from root listing %v", name, entries)
		}
	}
	if seen[probeName] {
		return fmt.Errorf("virtual probe %s must never list", probeName)
	}
	return nil
}

// checkWriteThrough commits a fresh payload through the mount (the claude
// atomic-save shape), waits for it to land in the consumer copy, then for the
// re-rendered envelope to serve back through the mount.
func checkWriteThrough(p paths, src *stressSource) error {
	payload := fmt.Sprintf(`{"selftest":%d}`, time.Now().UnixNano())
	dst := filepath.Join(p.dir, synthName)
	tmp := dst + ".tmp.selftest"
	if err := os.WriteFile(tmp, []byte(payload), 0o644); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("rename commit: %w", err)
	}
	if err := waitFor(15*time.Second, func() bool {
		got, err := os.ReadFile(filepath.Join(p.consumer, synthName))
		return err == nil && string(got) == payload
	}); err != nil {
		return fmt.Errorf("write-through never reached the consumer copy: %w", err)
	}
	want, err := src.ReadSynth(vmDomain, synthName)
	if err != nil {
		return err
	}
	if err := waitFor(15*time.Second, func() bool {
		got, err := os.ReadFile(dst)
		return err == nil && bytes.Equal(got, want)
	}); err != nil {
		return fmt.Errorf("refreshed envelope never served back through the mount: %w", err)
	}
	return nil
}

// checkOrdinaryOps proves plain create/write/read/delete through the mount.
func checkOrdinaryOps(p paths, _ *stressSource) error {
	path := filepath.Join(p.dir, fmt.Sprintf("selftest-%d.txt", os.Getpid()))
	const body = "ordinary create through the mount\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		return fmt.Errorf("create: %w", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read back: %w", err)
	}
	if string(got) != body {
		return fmt.Errorf("read back %q, want %q", got, body)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove: %w", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat after remove = %v, want ErrNotExist", err)
	}
	return nil
}

// checkMmap maps the 8 MiB passthrough file through the mount and verifies
// every byte against the seeded pattern.
func checkMmap(p paths, _ *stressSource) error {
	path := filepath.Join(p.dir, mmapName)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	m, err := unix.Mmap(int(f.Fd()), 0, mmapFileBytes, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap %s: %w", path, err)
	}
	defer func() { _ = unix.Munmap(m) }()
	want := mmapPattern()
	if !bytes.Equal(m, want) {
		for i := range want {
			if m[i] != want[i] {
				return fmt.Errorf("mmap content differs at offset %d: got %#x, want %#x", i, m[i], want[i])
			}
		}
	}
	return nil
}

// checkProbe stats and pulls the virtual wedge-probe file.
func checkProbe(p paths, _ *stressSource) error {
	path := filepath.Join(p.dir, probeName)
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat probe: %w", err)
	}
	if fi.Size() != probeBytes {
		return fmt.Errorf("probe size = %d, want %d", fi.Size(), probeBytes)
	}
	return readProbe(p.dir)
}

// waitFor polls cond every 200ms until it holds or the budget elapses.
func waitFor(budget time.Duration, cond func() bool) error {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within %s", budget)
}
