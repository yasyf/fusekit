# fusekit

![fusekit banner](docs/assets/readme-banner.webp)

[![CI](https://img.shields.io/github/actions/workflow/status/yasyf/fusekit/ci.yml?branch=main&label=CI)](https://github.com/yasyf/fusekit/actions/workflows/ci.yml)
[![License: PolyForm-Noncommercial-1.0.0](https://img.shields.io/badge/License-PolyForm--Noncommercial--1.0.0-blue.svg)](https://github.com/yasyf/fusekit/blob/main/LICENSE)

FUSE-T overlay backends, a detached mount-holder, and mount-lifecycle primitives for Go.

fusekit mirrors one shared base dir into many per-tenant views and keeps the mounts alive across
your own restarts. The `overlay` package picks a backend — `symlink`, `nfs`, or `fskit` — from a
consumer-supplied `Spec`, then realizes it: it owns the symlink fallback in-process and drives
the fuse-t backends through a **detached mount-holder**, a long-lived process that owns FUSE-T
mounts over a frozen unix-socket protocol, so your daemon can restart, upgrade, or crash without
dropping a live session. Around it sit the lifecycle primitives you need to drive mounts safely:
bounded mount and teardown, cgofuse-load panic recovery, wedged-carcass cleanup, and an opt-in
NFS cache-defeat decorator.

## Install

```sh
go get github.com/yasyf/fusekit@latest
```

The root package and `fusekit/mountd` build pure with `CGO_ENABLED=0` on every platform. The
in-process FUSE host needs `-tags fuse` and cgo, against FUSE-T on macOS and libfuse3 on Linux.

## Quickstart

Mount a `fuse.FileSystemInterface` at a mountpoint in-process. `Mount` returns as soon as the
mount is live; the returned `Handle` owns teardown.

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

To hand the mount your process's whole lifetime, use `fusekit.Serve(ctx, cfg)` instead: it
blocks until `ctx` is cancelled (SIGINT/SIGTERM) or the mount is removed externally, then tears
down.

## Overlay backends

`overlay` realizes a per-tenant view of one shared base dir across three backends that yield the
same observable result by different means. `symlink` (`overlay.BackendSymlink`) links each
top-level base entry into the account dir in-process, complete and holder-free. `nfs`
(`overlay.BackendNFS`) and `fskit` (`overlay.BackendFSKit`) serve a passthrough mirror through
the detached mount-holder; `nfs` honors libfuse `fi->fh` read semantics, while `fskit` (macOS
26+) is passthrough-only. `overlay.Parse` is the only way in from a stored string — it rejects
anything but those three, including the legacy `"fuse"`, with `overlay.ErrUnknownBackend`.

The consumer stays a blind consumer. It declares its classification once through an
`overlay.Spec` — which top-level names are private (`IsPrivate`), excluded, shared, or skipped,
and whether the filesystem is `PassthroughOnly` — and, for fuse, supplies the cgofuse filesystem
the holder serves via `Spec.Holder`. `overlay` then both selects and performs the overlay, and
owns the symlink fallback. `overlay.Select` probes the machine — build capability via
`fusekit.Built()`, holder reachability, and a holder-side probe mount — and returns a fuse
backend only when all three hold, else `overlay.BackendSymlink` plus a human-readable reason.
`overlay.ProviderFor` reconstructs a `Provider` from a stored backend without probing, so a
recorded verdict is honored verbatim across processes; it never silently substitutes, and errors
on a fuse backend when `Spec.Holder` is nil.

fusekit owns the macOS-grant guidance per backend: `(Backend).Enablement()` returns the
`Pane`, `Guidance`, and deep-link `URLs` a fuse backend needs before its mounts come live, and
`(Backend).OpenSettings(ctx)` opens that pane. Migration between backends moves only the right
entries: `overlay.MovePrivateEntries` relocates per-account private state between private roots,
`overlay.MoveSharedOrphans` returns shared writes to the base, and `overlay.HasPrivateEntries`
detects state stranded by an interrupted conversion. `overlay.FusePrivateRoot(accountDir)` names
the fuse private backing dir, and `overlay.ResolvedConflictLogf` surfaces every last-write-wins
collision the moves reconcile.

Build a `Spec`, call `Select`, and `Setup` the chosen provider against the base:

```go
spec := overlay.Spec{
    IsPrivate: func(name string) bool { return name == "identity.json" }, // your private names
    Excluded:  map[string]bool{},     // empty per-account dirs (each must satisfy IsPrivate)
    Shared:    map[string]bool{},      // always-materialized shared dirs
    Skip:      map[string]bool{".DS_Store": true},
    // PassthroughOnly: serves only real backing files (no synthetic content) — fskit when true
    // and available, else nfs. Leave false unless your FS generates content in its handlers.
    PassthroughOnly: false,
    Holder: &overlay.HolderSpec{ // nil disables fuse selection — Select returns symlink
        Socket:         socket,
        LogPath:        logPath,
        Args:           []string{"mount-holder", "--socket", socket}, // your holder argv
        CannotHostHint: "install the fuse build: brew install myapp",
        Version:        version.String(), // your wire version, never fusekit's
    },
}

provider, backend, reason, err := overlay.Select(ctx, spec)
if err != nil {
    log.Fatal(err)
}
if backend == overlay.BackendSymlink && reason != "" {
    log.Printf("using symlinks: %s", reason) // empty reason on a fuse verdict
}
if err := provider.Setup(base, accountDir); err != nil {
    log.Fatal(err)
}
```

For the architecture behind the three backends — why the fuse half lives out-of-process, how the
private root works, and how conversions stay crash-safe — see [docs/overlay.md](docs/overlay.md)
and the package godoc.

## The detached holder

To keep mounts alive across your own restarts, run them out-of-process. Host a `mountd.Server`
in a detached holder, then drive it from your CLI or daemon.

The holder is a subcommand of your binary, built with `-tags fuse`. It wraps a
`fusekit.MountSet` (which satisfies the `mountd.Host` seam) and serves until it is signalled:

```go
srv := &mountd.Server{
    Socket:  socket,
    Version: version.String(), // your version on the wire, never fusekit's
    Host: &fusekit.MountSet{
        Build: func(base, dir string) fusekit.Config {
            return fusekit.Config{
                Base: base, Dir: dir, FS: newFS(base),
                Options: fusekit.MountOptions{Volname: "myapp", NoBrowse: true}.Build(),
            }
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

Drive the holder from any build with a `mountd.RemoteHost`. It auto-spawns the holder when one
is not already running (via `mountd.Spawn`), then wraps a `mountd.Client` for the mount and
unmount RPCs:

```go
host := &mountd.RemoteHost{
    Socket:         socket,
    LogPath:        logPath,
    Args:           []string{"mount-holder", "--socket", socket}, // your holder argv
    CannotHostHint: "install the fuse build: brew install myapp",
}
if err := host.Setup(repoRoot, mountpoint); err != nil {
    // errors.Is(err, fusekit.ErrMountNotLive) → first-mount macOS TCC grant;
    // errors.Is(err, mountd.ErrCannotHost)    → a pure build that can't host one.
}
defer host.Teardown(repoRoot, mountpoint)
```

## What problems does this solve?

- Your mounts outlive your process. The detached holder owns the FUSE-T mount, so your daemon
  can restart, upgrade, or crash without dropping a live session.
- The wire protocol is frozen and skew-safe. `mountd`'s newline-JSON protocol is versioned and
  additive-only, so a newer client and an older holder interoperate in either direction. The
  version on the wire is the one you inject through `Server.Version`, never fusekit's own.
- Teardown is bounded and refuses to wedge. Mount and unmount run on timeout ladders with a
  forced fallback and a post-unmount mountpoint re-check, and `ClearCarcass` reaps the dead NFS
  mounts that otherwise freeze the host.
- NFS cache defeat is opt-in. The `CacheDefeat` decorator bumps mtime nanoseconds and commits
  on both `Flush` and `Fsync`, so same-second edits stay visible and a bad save fails loudly at
  `close(2)`. It stays off until you set `Config.CacheDefeat`.

## Used by

- [cc-pool](https://github.com/yasyf/cc-pool), the canonical consumer, pools several Claude
  subscriptions, mirroring `~/.claude` per account over an overlay.
- [cc-notes](https://github.com/yasyf/cc-notes) renders a synthetic notes tree over a repo
  through a fuse mount.
- [cc-squash](https://github.com/yasyf/cc-squash) drives the holder and lifecycle primitives as
  a second Go consumer.

## License

fusekit is licensed under PolyForm-Noncommercial-1.0.0. See
[LICENSE](https://github.com/yasyf/fusekit/blob/main/LICENSE).
