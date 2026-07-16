# fusekit Development Guide

Detached FUSE-T mount-holder and mount-lifecycle primitives for Go.

## Repository Structure

```
fusekit/
├── *.go              # root pkg: mount-core primitives (errors, live, mounted, unmount,
│                     #   carcass, nsec, options) + the in-process fuse host (mount,
│                     #   cachedefeat, mountset, hostprobe, probefs). The primitive/
│                     #   errors files build pure (CGO_ENABLED=0, any GOOS); the fuse
│                     #   host builds under `-tags fuse` with cgo (FUSE-T/macOS,
│                     #   libfuse3/linux). Platform syscalls split into _darwin/_other.
├── mountd/           # the detached mount-holder: frozen wire protocol, Client, Server,
│                     #   the Host seam, Spawn, RemoteHost — FULLY PURE (imports no cgofuse)
├── docs/assets/      # generated brand images (logo, banner, social card)
├── .github/workflows/ci.yml   # pure vet/test/-race + -tags fuse build on macOS & linux
├── AGENTS.md         # This file — shared conventions
├── STYLEGUIDE.md     # Full style guide
└── README.md         # Project overview
```

This library is **extracted from [cc-pool](https://github.com/yasyf/cc-pool)** (canonical) and consumed by it and [cc-notes](https://github.com/yasyf/cc-notes). When porting code in, `cp` the file then edit in place — never recreate from scratch — so the frozen wire protocol and lifecycle bytes stay identical.

## Testing — always via `scripts/test.sh`

Run tests with `scripts/test.sh ./...` (a `ulimit -u` wrapper around `go test`). **Never run bare `go test`, especially `-tags fuse`, on a real machine.** The holder spawn path (`proc.Spawn`) materializes and execs `os.Executable()`; if that executable is a *test* binary, Go's flag parser stops at the non-flag holder subcommand and `testing.Main` re-runs the whole suite, which re-enters the spawn — an exponential fork bomb that exhausts the process table and freezes the machine. The harness caps the per-UID process count so a runaway fails fast with `EAGAIN`. `proc.Spawn` also lowers the spawned child's `RLIMIT_NPROC` (darwin) as a second backstop. CI runs through the harness too. See the 2026-06-24 mount-holder fork-storm incident (recorded in cc-pool's cc-notes: `ccn doc show ef281ea`). (The durable fix is moving the holder out of self-`exec` into a single signed multi-tenant `fusekit-holder` daemon — see that plan.)
