# ![fusekit](docs/assets/readme-banner.webp)

**kill -9 the daemon. Every mount stays up.** fusekit parks each FUSE-T mount in a detached holder process, so daemon restarts, upgrades, and crashes never take a filesystem down.

[![CI](https://github.com/yasyf/fusekit/actions/workflows/ci.yml/badge.svg)](https://github.com/yasyf/fusekit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/fusekit)](https://github.com/yasyf/fusekit/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm--Noncommercial--1.0.0-blue)](LICENSE)

## Get started

```sh
go get github.com/yasyf/fusekit@latest
```

<img src="docs/assets/demo.png" alt="Terminal running 'kill -9' on the demo daemon — cat through the mount still prints 'all mounts alive'" width="700">

That capture is a real run — [docs/scripts/demo.sh](docs/scripts/demo.sh) regenerates it. The daemon dies mid-flight and the mount keeps serving, because the holder owns it. The root package and `fusekit/mountd` build pure with `CGO_ENABLED=0` on every platform; only a process that hosts mounts needs `-tags fuse` and cgo, against FUSE-T on macOS or libfuse3 on Linux.

Driving with an agent? Paste this:

```text
Run `go get github.com/yasyf/fusekit@latest` in my Go project.
Wire my mounts through mountd.RemoteHost so a detached holder keeps them alive across my daemon's restarts.
Read https://pkg.go.dev/github.com/yasyf/fusekit and docs/overlay.md for the overlay backends and holder protocol.
```

---

## Use cases

### Keep FUSE mounts alive through daemon restarts, upgrades, and crashes

When your daemon owns its mounts, every deploy drops them and every crash strands them. Hand them to a detached holder instead — `mountd.RemoteHost` spawns one when missing, adopts an already-live mirror with zero RPC, and drives mount and unmount over a unix socket:

```go
host := &mountd.RemoteHost{
    Socket:         socket,
    LogPath:        logPath,
    Args:           []string{"mount-holder", "--socket", socket}, // your holder argv
    CannotHostHint: "install the fuse build: brew install myapp",
    Owner:          "myapp", // every op is owner-scoped
}
if err := host.Setup(repoRoot, mountpoint); err != nil {
    // errors.Is(err, fusekit.ErrMountNotLive) → first-mount macOS TCC grant;
    // errors.Is(err, mountd.ErrCannotHost)    → a pure build that can't host one.
}
```

The demo above is this exact wiring: `kill -9` the driving process, and reads through the mountpoint keep answering. Upgrades need nothing from you: the holder journals its specs, detects version skew against the installed bundle itself, and self-retires — lease-gated — so the relaunched build replays everything it served.

### Serve one shared base dir as isolated per-tenant views

N tenants sharing one base directory can't share one filesystem view — each needs its own private entries over the common content. Declare the classification once in an `overlay.Spec` and let `overlay.Select` probe the machine:

```go
spec := overlay.Spec{
    IsPrivate: func(name string) bool { return name == "identity.json" },
    Skip:      map[string]bool{".DS_Store": true},
    Holder:    &overlay.HolderSpec{Socket: socket, Args: holderArgv, Version: version.String()},
}
provider, backend, reason, err := overlay.Select(ctx, spec)
// backend: fskit or nfs through the holder, else symlink — with the reason why
if err := provider.Setup(base, accountDir); err != nil {
    log.Fatal(err)
}
```

Each tenant dir becomes a live mirror of the base with its private names redirected to per-tenant backing, and the verdict degrades cleanly: no fuse build, no reachable holder, or a failed probe mount all fall back to symlinks with a human-readable reason.

### Never lose a live mount to a teardown race

A dead FUSE-T mount is not inert — a stat on it hangs the caller, and a stack of them wedges the host. fusekit's answer is bounded, graceful-only teardown plus one tightly guarded exception. The library exports **no force-unmount**: every consumer-reachable teardown is graceful, runs on timeout ladders, and re-verifies each OK against kernel state — a lost RPC response or a wedged mirror reads as the wedge it is, never as a clean teardown, so callers never `RemoveAll` through a live mount.

The one exception lives inside the holder: its pre-mount and journal-replay carcass clears force-unmount a dead holder generation's leftovers, and only under carcass proof — the mount's stat answers a dead errno *immediately* (a hanging stat is never proof of death), its kernel identity is pinned, and its NFS server is proven dead first — all executed under a seized session-lease fence. A per-directory flock lease (`fusekit/lease`) is how sessions pin a mount: hold `lease.Acquire` while you use a dir and no teardown, retire, or carcass clear can touch it; the busy verdict carries your provenance.

## Mount in-process

For a process that owns its own mount lifetime (built with `-tags fuse`), `Mount` returns as soon as the mount is live and the returned `Handle` owns bounded teardown:

```go
h, err := fusekit.Mount(fusekit.Config{
    Base:    repoRoot,   // dir whose contents the mount mirrors
    Dir:     mountpoint, // where the mount is served
    FS:      myFS,       // your fuse.FileSystemInterface
    Options: fusekit.MountOptions{Volname: "myapp", NoBrowse: true}.Build(),
    // Ready defaults to MountAlive(Base, Dir); set it for a synthetic tree
    // whose Base contents never show through.
})
if err != nil {
    // classify with errors.Is: fusekit.ErrFuseUnavailable (no fuse runtime),
    // fusekit.ErrMountNotLive (first-mount macOS TCC grant), fusekit.ErrMountTimeout.
}
defer h.Unmount()
```

`fusekit.Serve(ctx, cfg)` is the same mount for your process's whole lifetime: it blocks until `ctx` cancels or the mount is removed externally, then tears down. NFS attribute caching hides same-second edits; the opt-in `CacheDefeat` decorator (`Config.CacheDefeat`) bumps mtime nanoseconds and commits on both `Flush` and `Fsync`, so edits stay visible and a bad save fails loudly at `close(2)`.

## Host the holder

The holder is a subcommand of your own binary, built with `-tags fuse`. It wraps a `fusekit.MountSet` (the `mountd.Host` seam) and serves until signalled:

```go
srv := &mountd.Server{
    Socket:  socket,
    Version: version.String(), // your version on the wire, never fusekit's
    Host: &fusekit.MountSet{
        Build: func(spec fusekit.MountSpec) (fusekit.Config, error) {
            return fusekit.Config{
                Base: spec.Base, Dir: spec.Dir, FS: newFS(spec.Base),
                Options: fusekit.MountOptions{Volname: "myapp", NoBrowse: true}.Build(),
            }, nil
        },
        StateFn: func(base, dir string) (mounted, alive bool) {
            m := fusekit.Mounted(dir)
            return m, m && fusekit.MountAlive(base, dir)
        },
    },
}
if err := srv.Run(ctx); err != nil {
    log.Fatal(err)
}
```

The wire protocol is newline-JSON and capability-negotiated: a consumer opens with `hello` and `HelloInfo.Require`s the features it needs; within proto 2 evolution is additive-only (new op or optional field + a new feature string), and a cross-generation request is refused with a message naming which side to upgrade. [cmd/holder](cmd/holder) is the ready-made serve-only variant that mirrors any base passthrough-style — the demo drives it unmodified.

Because the holder outlives your daemon, an upgrade leaves an old-version holder serving live mounts. The holder retires itself: it journals every mount and bridge spec, polls `Server.RetireSkew` against the installed bundle, and — only once no journaled mount's session lease is held — drains gracefully and exits, so the `service.AppKeepAlive` LaunchAgent relaunches the installed build, which replays the journal. The drain never force-unmounts (a forced unmount of a live mount panics the Apple NFS kext): any busy claim, held lease, or wedge aborts the sweep and remounts what was already swept, and a persisted strike breaker parks a retire storm instead of kill-cycling. There is no consumer-driven retire or converge: sessions hold `lease.Acquire` on the dirs they use, and everything else is the holder's.

## Overlay backends

`overlay` realizes the same per-tenant view through three backends: `symlink` links each top-level base entry in-process, holder-free; `nfs` and `fskit` (macOS 26+, passthrough-only) serve a passthrough mirror through the detached holder. `overlay.Parse` is the only way in from a stored string, and `overlay.ProviderFor` reconstructs a provider from a recorded verdict without re-probing — it never silently substitutes backends.

The consumer stays blind to the mechanics. Beyond `Select` and `Setup`, the package owns the operational edges: `(Backend).Enablement()` names the macOS Settings pane and deep links a fuse backend needs before mounts come live, and the migration helpers (`MovePrivateEntries`, `MoveSharedOrphans`, `HasPrivateEntries`) move exactly the right entries between backends, surfacing every last-write-wins collision through `ResolvedConflictLogf`. For the architecture — why the fuse half lives out-of-process, how the private root works, and how conversions stay crash-safe — see [docs/overlay.md](docs/overlay.md).

## Package map

| Package | What it holds |
|---|---|
| `fusekit` | In-process mount lifecycle: `Config`, `Mount`/`Serve`, `Handle` teardown, `MountSet`, liveness probes, `CacheDefeat` — graceful-only, no force API |
| `fusekit/mountd` | The detached holder: `Server`, `Client`, `RemoteHost`, `Spawn`, the spec journal + lease-gated self-retire, frozen proto-2 wire protocol — builds pure |
| `fusekit/lease` | Per-directory flock session leases: `Acquire`/`Seize`/`Probe`/`List` — the fence every teardown honors |
| `fusekit/overlay` | Three-backend per-tenant overlay: `Spec`, `Select`, `ProviderFor`, enablement and migration helpers |
| `fusekit/holderfs` | The shared holder's passthrough mirror filesystem (`-tags fuse`) |
| `fusekit/proc` | Stdlib-only process primitives: detached spawn, single-entrant bind, backoff, `Strikes`/`Ladder` |
| `fusekit/fuset` | macOS fuse-t facts: dylib path, Homebrew cask, install and FSKit availability |
| `fusekit/state` | A consumer's `~/.<App>` private state dir and atomic status mirror |
| `fusekit/service` | macOS LaunchAgent install and manage — daemon agents and the holder's `AppKeepAlive` relauncher — reconciled with Homebrew services |
| `cmd/holder` | The dedicated serve-only holder binary |

The exhaustive contracts live in the [godoc](https://pkg.go.dev/github.com/yasyf/fusekit).

Used by [cc-pool](https://github.com/yasyf/cc-pool), [cc-notes](https://github.com/yasyf/cc-notes), and [cc-squash](https://github.com/yasyf/cc-squash). Licensed under [PolyForm Noncommercial 1.0.0](LICENSE).
