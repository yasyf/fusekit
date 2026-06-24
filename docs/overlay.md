# The overlay model

One shared base dir, made to appear inside every per-tenant dir, with writes shared straight back. That is the whole overlay model. Each tenant opens its own dir and sees the same projects, settings, and history as the base; a write through any tenant lands in the base, so the next tenant sees it. The base is the single source of truth, and the per-tenant dirs are views onto it that also carve out a few private names.

Think of it as a shared Dropbox folder that every account mounts at its own path: one set of files underneath, many doors into it, edits visible to all. The analogy breaks on the carve-outs. A few top-level names are deliberately *not* shared, because they hold instance-local runtime state that would corrupt if two tenants fought over one copy. So the overlay is a shared folder with a handful of private rooms cut into each tenant's view.

The `overlay` package realizes that model across three backends — `symlink`, `nfs`, and `fskit` — that reach the same observable result by different means. The symlink backend links each top-level base entry into the tenant dir in-process. The two fuse-t backends serve a passthrough mirror hosted by a detached mount holder, so the mounts outlive the daemon and CLI processes that ask for them. A tenant cannot tell which backend is underneath: the dir looks the same either way.

## Mechanism, policy, and content

The design splits cleanly along three axes, and keeping them separate is what lets one package serve any consumer without naming that consumer's files. The columns below name each axis, its owner, and how each backend realizes it.

| Axis | What it is | Owner | Symlink | Fuse (nfs / fskit) |
|------|-----------|-------|---------|--------------------|
| Mechanism | Present the base inside a per-tenant dir with private carve-outs | `fusekit` (generic) | `SymlinkProvider` plus the migration primitives | The mount lifecycle (`Mount` / `MountSet` / `Unmount`) plus the `RemoteFuseProvider` wire client |
| Policy | Which top-level names are private, shared, or excluded | The consumer, passed in | The injected `Spec` predicates | The same `Spec` predicates, unchanged |
| Content | The actual bytes served at each path | The consumer | Applied out-of-band by the consumer | A consumer-supplied cgofuse FS the holder serves |

Mechanism is generic and lives in `fusekit`. The same private-carve-out logic works for any base dir and any tenant dir, so the package owns it whole. Policy is the one place a consumer's domain knowledge enters: which names are identity, which are excluded empty dirs, which are always-shared. That classification is identical across both backends — the symlink provider and the fuse mirror read the same `Spec` predicates — so switching backends never reshuffles what is private. Content is where the two mechanisms diverge hardest, and it drives the whole asymmetry below.

## What fusekit owns and what the consumer declares

The boundary runs through three entry points: `Select`, `ProviderFor`, and `Spec`.

`fusekit` owns selection. `Select` probes this machine — build capability via `fusekit.Built()`, holder reachability, and a holder-side probe mount — and decides whether a fuse backend can host here. The symlink fallback is also its job: any time no fuse backend can host, the verdict is `BackendSymlink` plus a human-readable reason string explaining why. It drives the mount lifecycle, taking mounts up and down through the detached holder. And it carries the per-backend enablement guidance, where `Backend.Enablement` names the one-time macOS grant a fuse backend needs and `Backend.OpenSettings` routes the user to the right System Settings pane.

The consumer declares the classification through `Spec`: the `IsPrivate` predicate, the `Excluded`, `Shared`, and `Skip` sets, the `PassthroughOnly` flag, and the `Holder` wiring. One invariant binds the predicates together — every `Excluded` name must also satisfy `IsPrivate`, because the migration primitives treat the excluded dirs as private state. A nil `Holder` disables fuse selection entirely: `Select` returns symlink, and `ProviderFor` errors if a fuse backend is requested.

For fuse, the consumer supplies one more thing the symlink path never needs: the cgofuse FS the holder serves. `Spec.Holder` wires the detached mount holder, and the holder hosts the consumer's `fuse.FileSystemInterface` as the live content engine behind every fuse mount.

## Batteries included for symlink, bring your own FS for fuse

`ProviderFor(BackendSymlink, spec)` hands back a complete, self-sufficient provider. The `SymlinkProvider` runs entirely in-process and needs no holder, no mount, and no content engine — it links base entries into the tenant dir and is done. Batteries included.

A fuse backend is different. `ProviderFor(BackendNFS, spec)` or `ProviderFor(BackendFSKit, spec)` returns a `RemoteFuseProvider`, which is only the wire and lifecycle half. It wraps a `mountd.RemoteHost` and drives the holder over its socket: Setup, Sync, Health, Teardown. It does not contain the bytes. The consumer supplies those by hosting its cgofuse FS in the holder, wired through `Spec.Holder`. Bring your own FS.

This asymmetry is inherent, not a leak. A symlink overlay needs no content engine — the kernel resolves each link to a real base file, and the bytes are whatever the base already holds. A fuse overlay *is* a content engine: every read and write flows through a `fuse.FileSystemInterface` that decides what bytes to serve, where to redirect a private path, and how to merge a synthetic file. There is nothing for `fusekit` to ship there, because the content is the consumer's domain logic. So the two constructors are deliberately asymmetric. `ProviderFor(BackendSymlink)` returns a finished provider; `ProviderFor` for a fuse backend returns the half `fusekit` can own and leaves the content half to the consumer, who plugs it in through `Spec.Holder`.

The fuse half also lives out-of-process for a reason. Mount capability and the macOS grant are per-process, and the holder — not the `overlay` package — is the process that hosts and outlives the mounts. So even the lifecycle half is a wire client to a separate process, never an in-process mount.

## How PassthroughOnly picks the fuse backend

`PassthroughOnly` chooses between the two fuse backends, and only between them. When a fuse verdict is reached, `FuseBackend(spec)` returns `BackendFSKit` if `spec.PassthroughOnly` is true and fuse-t's FSKit backend is available, else `BackendNFS`.

The reason is a hard FSKit limitation: FSKit ignores `fi->fh`. fuse-t's NFS backend honors libfuse's `fi->fh` read semantics, so an FS that returns handler-generated bytes for a synthetic file handle — a merged, injected, or virtual file — reads correctly under NFS. FSKit drops that handle, so the same synthetic content reads torn or wrong. Any FS that serves synthetic content must therefore declare `PassthroughOnly` false and land on NFS. Only a pure-passthrough FS, one whose every read delegates to a real backing file, is safe on FSKit. The root `fusekit.PassthroughOnly` interface is the per-FS marker behind the same choice: a `Config.FS` that implements it and returns true opts into FSKit when available, and the safe default — not implementing it — keeps NFS.

Symlink is orthogonal to all of this. `PassthroughOnly` never selects symlink. Symlink is `Select`'s verdict whenever no fuse backend can host at all — a pure build, no holder, a failed probe, or a pending grant — regardless of whether the FS would have been passthrough. The flag picks nfs-versus-fskit; the host probe picks fuse-versus-symlink.

## Example: cc-pool

cc-pool pools several Claude subscriptions and gives each its own pool account dir that mirrors `~/.claude`. The base is `~/.claude`; the tenant dirs are the per-account config dirs. Every account sees the same projects, settings, and history, while a few names stay private per account so two concurrent Claude sessions never fight over one runtime.

Its `Spec` maps the policy axis directly onto `~/.claude` entries. The identity, state, and credential files — `.claude.json` and its atomic-write temp siblings, `.credentials.json`, the auto-update result, and the cached remote settings — are private, so each account keeps its own and no account adopts another's login. The `daemon`, `ide`, and `backups` dirs are excluded: each becomes a private empty dir per account, because Claude Code's PID-keyed supervisor, per-process IDE locks, and rotating config backups all corrupt if shared. `plans` is shared, materialized in the base and linked so plan-mode plans land in one place rather than scattering per account. And `.DS_Store` is skipped as OS cruft. Because every excluded name is also private, the `Excluded`-implies-`IsPrivate` invariant holds.

For the fuse backend, cc-pool supplies a cgofuse FS — its `mirrorFS` — as the content engine the holder serves. `mirrorFS` is a passthrough mirror of `~/.claude` with two twists: it redirects the private names to a per-account backing dir, and it serves `/.claude.json` as a merged identity file, splicing the base's shareable keys over each account's private file on read and writing the shareable subset back to `~/.claude.json` on commit. That merged file is synthetic, handler-generated content keyed on a fuse file handle. So cc-pool sets `PassthroughOnly` false, which forces `FuseBackend` to NFS — the backend that honors `fi->fh` and serves the merged read intact. The merged `/.claude.json` is precisely the kind of content that reads torn under FSKit, which is why cc-pool stays on NFS.

## Where to go next

For the API surface — every `Spec` field, the `Provider` interface, the migration primitives, and the constants — read the package godoc and the [README](../README.md). For the runnable end-to-end symlink walkthrough, see the `ProviderFor` example in the package tests. This page is the why; those are the what and the how.
