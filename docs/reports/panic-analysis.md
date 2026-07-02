<!-- status: draft -->

# Kernel panic analysis: `nfs_vinvalbuf2: ubc_msync failed!, error 22`

macOS 26.5 (25F71) halted three times in five days on one Apple Silicon Mac (Mac17,7), each time while an ordinary unprivileged process wrote files inside an NFSv4 mount served by a third-party userspace server on localhost. Two of the panics carry the identical string — `"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260` — and the third is a kernel data abort inside the same NFS kext, entered through the same call path. This document is the technical analysis behind the Feedback report: what the panic reports prove, why the kext's own source makes this a kernel robustness bug, and what the userspace side was doing that made the race likely.

The core defect is one special case in Apple's NFS client kext (NFS-343.100.5, `kext/nfs_bio.c`). When `nfs_vinvalbuf2` flushes and invalidates a vnode's cached pages, every error from `ubc_msync` is retried up to ten times and then logged — except `EINVAL`, which panics the machine immediately. Per the xnu source, `EINVAL` from this call site means exactly one thing: the vnode's UBC state was torn down concurrently, so there were no pages left to flush. The invalidation's goal state already holds. The kext panics on finding its work already done, in a race window its caller cannot close, on input any NFS server — local or remote, healthy or buggy — can steer it into.

## Three panics in five days

All three reports come from `/Library/Logs/DiagnosticReports/Retired/` on the affected machine and share the same OS build (25F71), kernel (`xnu-12377.121.6~2/RELEASE_ARM64_T6050`), and hardware (Mac17,7). The table below summarizes them; timestamps are local (PDT), taken from each report's calendar-time field.

| # | Panic time | Panic | Panicked task | Uptime at panic |
|---|------------|-------|---------------|-----------------|
| 1 | 2026-06-27 03:35:18 | Kernel data abort at pc inside `com.apple.filesystems.nfs`, faulting address `0x90` | pid 36090, `2.1.191` | 11.4 days |
| 2 | 2026-06-27 03:52:26 | `"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260` | pid 17406, `2.1.195` | 16 min 50 s |
| 3 | 2026-07-01 14:53:04 | `"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260` | pid 66615, `2.1.197` | 4.5 days |

The panicked task was the same application all three times: the Claude Code CLI, whose executable is version-named (`2.1.191`, `2.1.195`, `2.1.197` across its auto-updates), running unprivileged as uid 501. Panic 2 struck less than seventeen minutes after the reboot from panic 1. Panic 3 struck after the machine had run four and a half days without NFS mounts — shortly after a single NFS mount was restored on the same server revision.

Report filenames, for cross-reference inside the Feedback attachment:

- `panic-base+socd-2026-06-27-033630.000.panic` (panic 1)
- `panic-full-2026-06-27-035332.0002.panic` (panic 2)
- `panic-full-2026-07-01-145345.0002.panic` (panic 3)

## What the backtraces prove

Panics 2 and 3 are the same crash. After subtracting each boot's kext load addresses, their panicked-thread backtraces are frame-for-frame identical across all 22 kernel-space frames (0–21) — same module, same offset — four days and two claude versions apart. Frame 22, the userspace return address that entered the kernel, differs raw between the two reports; the reports record no shared-cache slide to resolve it, but the two values share their 16 KB page offset, consistent with the same address under each boot's shared-cache ASLR. Here is panic 3's backtrace with each frame resolved to module+offset (panic 2's kernel-space frames differ only by the per-boot slide):

```text
frame  lr (panic 3)         module + offset
  0    0xfffffe0053c283e8   kernel     + 0x583e8   ─┐
  1    0xfffffe0053de0ec8   kernel     + 0x210ec8   │
  2    0xfffffe0053dde9c8   kernel     + 0x20e9c8   │ panic/debugger
  3    0xfffffe0053bd5ee4   kernel     + 0x5ee4     │ machinery
  4    0xfffffe0053c28730   kernel     + 0x58730    │
  5    0xfffffe0053c27d28   kernel     + 0x57d28    │
  6    0xfffffe005455e780   kernel     + 0x98e780  ─┘
  7    0xfffffe0053b06f7c   nfs        + 0x9a74c   <- panic caller: nfs_vinvalbuf2, nfs_bio.c:4260
  8    0xfffffe0053acea4c   nfs        + 0x6221c
  9    0xfffffe0053accee8   nfs        + 0x606b8
 10    0xfffffe0053aa1afc   nfs        + 0x352cc
 11    0xfffffe0053eca720   kernel     + 0x2fa720
 12    0xfffffe005453b964   kernel     + 0x96b964
 13    0xfffffe00536449a4   quarantine + 0x6714    <- com.apple.security.quarantine
 14    0xfffffe0053643400   quarantine + 0x5170
 15    0xfffffe005452c19c   kernel     + 0x95c19c  ─┐
 16    0xfffffe0053ec96e8   kernel     + 0x2f96e8   │
 17    0xfffffe0053eb0750   kernel     + 0x2e0750   │ syscall entry
 18    0xfffffe0053eb167c   kernel     + 0x2e167c   │ path
 19    0xfffffe005437371c   kernel     + 0x7a371c   │
 20    0xfffffe0053ddeaa4   kernel     + 0x20eaa4   │
 21    0xfffffe0053bd5ee4   kernel     + 0x5ee4    ─┘
 22    0x0000000185357954   userspace
```

Read bottom-up, the call chain is: a userspace syscall (frame 22) enters the kernel, passes through `com.apple.security.quarantine` — the MACF policy that stamps quarantine and provenance extended attributes — back into VFS dispatch, descends into `com.apple.filesystems.nfs`, and dies at the `nfs_vinvalbuf2` panic (frame 7 is the exact caller address from the panic string). The operation that killed the machine was not the application's own read or write: it was metadata stamping the OS performed on the application's behalf, against a file on the NFS mount.

Panic 1, the data abort, is the same story with a harder ending. Its backtrace runs through the identical quarantine hook frame (`quarantine + 0x6714`) and the identical VFS dispatch frames (`kernel + 0x96b964`, `kernel + 0x2fa720`) into the NFS kext — where instead of reaching a `panic()` call, the kext dereferenced a field at offset `0x90` of a NULL pointer (`far: 0x90`, `esr: 0x96000006`, faulting pc at `nfs + 0x5aee0`). Same entry path, same kext, same lifecycle window; the state it raced was gone before the code could even reach its own panic check.

Both full reports list the same two kexts in the panicked thread's backtrace, with identical binary UUIDs across all three panics:

```text
Kernel Extensions in backtrace:
   com.apple.security.quarantine(4.0)[C596707D-78DA-3EAA-B0CD-B85AB8F5C63E]
   com.apple.filesystems.nfs(1.0)[0DC2270D-B94C-3556-AA41-004C48BB8194]
   [dependency lines omitted]
```

Memory pressure was not a factor: panic 2's report shows the compressor at 0% of its pages limit with zero swapfiles, sixteen minutes after a fresh boot.

## The panicking code

The panic site is in [NFS-343.100.5 `kext/nfs_bio.c`](https://raw.githubusercontent.com/apple-oss-distributions/NFS/NFS-343.100.5/kext/nfs_bio.c), at the tail of `nfs_vinvalbuf2` — the routine that flushes and invalidates a vnode's buffers, called whenever the client must discard cached data (attribute changes, opens, revalidation). After the buffer-level work, it purges the VM-cached pages:

```c
	/* get the pages out of vm also */
	if (UBCINFOEXISTS(vp) && (size = ubc_getsize(vp))) {
		if ((error = ubc_msync(vp, 0, size, NULL, ubcflags))) {
			if (error == EINVAL) {
				panic("nfs_vinvalbuf2: ubc_msync failed!, error %d", error);
			}
			if (retry++ < 10) { /* retry invalidating a few times */
				if (retry > 1 || error == ENXIO) {
					ubcflags &= ~UBC_PUSHALL;
				}
				goto again;
			}
			/* give up */
			printf("nfs_vinvalbuf2: ubc_msync failed!, error %d\n", error);
		}
	}
```

That is nfs_bio.c lines 4256–4271; the `ubc_msync` call is line 4258 and the `panic()` sits on line 4260, matching the panic string byte for byte. The error policy is inverted. A genuine I/O failure — `EIO`, `ENXIO`, anything the pager reports while pushing dirty pages — gets ten retries and then a log line. `EINVAL` alone gets an instant kernel halt.

So what does `EINVAL` mean here? `ubc_msync` cannot relay an `EINVAL` from the pager; it manufactures this errno itself. From [xnu-12377.121.6 `bsd/kern/ubc_subr.c`](https://raw.githubusercontent.com/apple-oss-distributions/xnu/xnu-12377.121.6/bsd/kern/ubc_subr.c), lines 1775–1791:

```c
errno_t
ubc_msync(vnode_t vp, off_t beg_off, off_t end_off, off_t *resid_off, int flags)
{
	int retval;
	int io_errno = 0;

	if (resid_off) {
		*resid_off = beg_off;
	}

	retval = ubc_msync_internal(vp, beg_off, end_off, resid_off, flags, &io_errno);

	if (retval == 0 && io_errno == 0) {
		return EINVAL;
	}
	return io_errno;
}
```

`EINVAL` means `ubc_msync_internal` returned 0 without an I/O error. Given the flags `nfs_vinvalbuf2` always passes (`UBC_PUSHALL | UBC_SYNC | UBC_INVALIDATE`, nfs_bio.c:4202) and the caller's own `size > 0` guard, `ubc_msync_internal` (ubc_subr.c:1858) returns 0 in exactly two ways:

```c
	if (!UBCINFOEXISTS(vp)) {
		return 0;
	}
```

— the vnode's `ubc_info` is gone (ubc_subr.c:1865–1867) — or, at the function's tail, the return is `(kret == KERN_SUCCESS) ? 1 : 0` (line 1915), which yields 0 when `memory_object_lock_request` against `vp->v_ubcinfo->ui_control` (line 1909) returned non-success without reporting an I/O error, meaning the memory object's control state was torn down underneath the request.

Both cases describe the same condition: **the vnode's page-cache machinery was destroyed between `nfs_vinvalbuf2`'s `UBCINFOEXISTS(vp)` check on line 4257 and the flush it requested on line 4258.** That is a textbook check-to-use race against vnode reclaim, and the "failure" it produces is vacuous — an invalidation finding zero pages to invalidate has already achieved its goal state. The kext panics on it anyway.

Apple's own code agrees this state is benign — twice, in the same file. Two hundred lines up, `nfs_vinvalbuf_internal` handles the same teardown surfacing through UPL setup (nfs_bio.c:4061–4066):

```c
					if (error == EINVAL) {
						/* vm object must no longer exist */
						/* hopefully we don't need to do */
						/* anything for this buffer */
					} else if (error) {
						printf("nfs_vinvalbuf_internal: upl setup failed %d\n", error);
					}
```

And `nfs_buf_release` (nfs_bio.c:1348–1350) calls `ubc_msync(…, UBC_INVALIDATE)` and merely logs any failure, `EINVAL` included. The lineage explains the leftover: in Mac OS X 10.5 (xnu-1228, `bsd/nfs/nfs_bio.c:3677`), *any* failure of this flush panicked. The modern code added the retry-then-log path for real errors but left the one errno that never indicated an I/O problem on the panic branch.

## Why this is a kernel bug regardless of the server

The server in this incident was third-party, userspace, and local, and — as the next section details — it served unstable attributes that made invalidations frequent. None of that changes the verdict, for four reasons.

1. **An NFS server is untrusted input.** The client kext must remain sound against any sequence of attributes, fileids, and errors a server produces — servers reboot, export snapshots, or are buggy, and nothing in the NFSv4 protocol lets a server opt the client out of memory-safety. Attribute churn may legitimately cost correctness of cached data or performance; it must never cost the kernel.
2. **The trigger is unprivileged.** Every panicking thread was an ordinary uid-501 process doing ordinary file I/O (with the OS's own quarantine/provenance stamping riding along). Any user who can reach an NFS mount — and any local user can start a userspace NFS server and mount it — can roll these dice.
3. **The panic condition is vacuous.** `EINVAL` here means "there was nothing to flush" — the invalidation's goal state already holds, as shown above from the xnu source. The same file already tolerates this exact teardown in two sibling paths.
4. **The race is the kernel's own.** Userspace controls how often invalidation runs, not the interleaving between `UBCINFOEXISTS` and `ubc_msync`. No server-side or client-userspace discipline can close that window; only the kext can.

Panic 1 sharpens the point. The same entry path, racing the same lifecycle window, also crashes as a NULL dereference inside the kext — so the teardown race is real and reaches state the code dereferences, not just the explicit panic call. The `EINVAL` branch is the half of the problem with a one-line fix.

## The trigger environment

The mounts were served by [fuse-t](https://github.com/macos-fuse-t/fuse-t) 1.2.7, a kextless FUSE implementation that runs a userspace NFSv4 server on localhost and mounts it through the macOS NFS client. The FUSE filesystem behind it was `fusekit`'s mount holder (`fusekit-holder`, a Go process; it appears alive in the process tables of both full panic reports — pids 2035 and 60580 — so this was not a dead-server scenario). The workload was Claude Code rewriting small JSON state files at high frequency inside the mount: an atomic-rename rewrite of a config file every few seconds, plus reads across the tree.

Two properties of this stack matter for the panic mechanism.

**Named attributes were enabled.** The mounts were created with `-o namedattr`, so the client negotiated NFSv4 named-attribute support. On such a mount, every xattr operation walks the named-attribute path in the kext — `nfs4_vnop_setxattr` (NFS-343.100.5 `kext/nfs4_vnops.c:9289`) drives `nfs4_named_attr_get` (`nfs4_vnops.c:8401`), which opens a *hidden attribute vnode* via OPENATTR, writes it, and closes it. These attribute vnodes are created and reclaimed at high rate.

**The OS itself generated the xattr traffic.** macOS stamps quarantine/provenance extended attributes (`com.apple.provenance`) on files created by tracked processes — Quarantine.kext's MACF hook does this synchronously inside the creating process's syscall. That is why `com.apple.security.quarantine` sits in all three backtraces between the syscall and the NFS kext: every one of the workload's file creations dragged a named-attribute open/write/close across the mount, no application code involved. The combination — rapid short-lived attribute vnodes plus constant attribute-driven invalidation — is precisely the population of vnodes the `nfs_vinvalbuf2`-versus-reclaim race needs.

## Attribute instability: what made the race frequent

The panics correlate 3-for-3 with one specific revision of the userspace filesystem, and the delta is what it served, not how it mounted. The predecessor revision ran the identical workload for nearly two weeks (June 12–24) — same fuse-t 1.2.7, same mount options including `namedattr`, same OS build (installed May 19) — and never panicked. The revision that panicked differed in one dimension: attribute stability. It served, for a small set of synthetic files:

- Fileid churn on rewrite: the synthetic merged view of the hottest file reported its backing file's real inode as the NFS fileid, and that backing file was replaced by atomic rename on every write-through, so the path's fileid changed on every rewrite while the writer held it open.
- Size and mtime flapping: after a cold start the server could briefly report the raw backing size for a path whose reads served larger merged content; the served mtime was a maximum over freshness-marker files that regressed when they vanished; a path GETATTR and an open-handle GETATTR could disagree.
- A cache-defeating probe file: a health-probe file advanced its mtime on every GETATTR and served fresh random bytes on every open.

Every one of those attribute changes is something the client answers with invalidation — `nfs_vinvalbuf2` on a vnode whose identity keeps shifting under a live writer. The deployment timeline makes the dose-response visible:

| Date | Event |
|------|-------|
| 2026-05-19 | macOS 26.5 (25F71) installed |
| 2026-06-12 | Attribute-stable server revision live (fuse-t 1.2.7, `namedattr` on) serving this workload |
| 2026-06-24 | NFS mounts retired (unrelated migration); machine runs clean |
| 2026-06-27 03:21 | Attribute-unstable revision serves its first mounts |
| 2026-06-27 03:35:18 | Panic 1 (14 minutes later) |
| 2026-06-27 03:52:26 | Panic 2 (16 min 50 s after the reboot, mounts restored at login) |
| 2026-06-27 | Mounts rolled back to a non-NFS mechanism; machine runs clean for 4.5 days |
| 2026-07-01 14:53:04 | Panic 3, shortly after one NFS mount was restored on the same revision |

Across the three events, the gap between NFS mounts coming up and the machine going down ranged from under a minute to about thirty minutes (per the managing daemon's logs). The same machine ran indefinitely with the mounts absent, and for weeks with an attribute-stable server on identical mount options.

To be explicit about the division of blame: the attribute instability was our bug, and we fixed it (stable synthetic fileids, monotonic mtimes, no cold-start size flap, probe churn removed, `namedattr` no longer requested). Those fixes shrink the trigger frequency from "minutes" to — hopefully — "never". They do not close the kernel's race window, and nothing userspace does can.

## Prior reports

I could not find any public occurrence of this panic string. Web searches for `"nfs_vinvalbuf2: ubc_msync failed"` turn up nothing — no forum thread, no bug tracker, no crash-dump paste. The nearest neighbor is a 2021 Apple Developer Forums thread, [ubc_msync have a bug](https://developer.apple.com/forums/thread/672174), which describes `ubc_msync`-versus-pageout races producing stale data (not panics) in an NFS-like kext on macOS 10.14 and ends unresolved. As far as I can tell, this document describes the first reported occurrences of the nfs_bio.c:4260 panic.

## The fix this report asks for

Route `EINVAL` through the path the function already has. The minimal change deletes the special case so `EINVAL` is retried and, if it persists, logged like every other errno — the retry loop already tolerates abandoning dirty data after ten failed attempts, so the data-loss posture does not change. A more surgical fix treats the condition as what it is: if `ubc_msync` reports `EINVAL` here, the vnode's UBC state is gone and there is nothing left to invalidate, so the function can log and return success, exactly as the `nfs_vinvalbuf_internal` UPL path ("vm object must no longer exist") has done since Mac OS X 10.5.

The NULL-dereference crash of panic 1 (faulting address `0x90`, pc at `nfs + 0x5aee0` in the 25F71 build of `com.apple.filesystems.nfs`) is presumably the same reclaim race reaching a different dereference and deserves its own audit of vnode/UBC lifetime in the invalidation paths; the attached report contains the full register state and backtrace.

## Provenance and sanitization

Every quoted panic string, build number, timestamp, backtrace frame, and process detail in this document was extracted directly from the three named `.panic` files; the kernel-source quotes were fetched verbatim from Apple's published trees ([NFS-343.100.5](https://github.com/apple-oss-distributions/NFS/tree/NFS-343.100.5) and [xnu-12377.121.6](https://github.com/apple-oss-distributions/xnu/tree/xnu-12377.121.6)) with line numbers checked against the raw files. Excerpts omit boot-session and incident identifiers, the crash-reporter key, and hardware provisioning blobs; the kext UUIDs shown are build identifiers common to every Mac on 25F71, not device identifiers. The unredacted panic reports accompany this analysis in the Feedback attachment.
