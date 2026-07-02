<!-- status: draft -->
<!--
Feedback Assistant form fields (filled at submission time):
  Title:       Kernel panic: nfs_vinvalbuf2: ubc_msync failed!, error 22 — nfs_bio.c panics unconditionally on EINVAL
  Area:        macOS > File System
  Type:        Incorrect/Unexpected Behavior (unexpected restart / kernel panic)
  Description: everything below the title heading, pasted verbatim
  Attachments: one ZIP (three .panic reports + panic-analysis.md), assembled at submission — see "Attachments"
  On submit:   replace this file's status header with the FB number.
-->

# Kernel panic: nfs_vinvalbuf2: ubc_msync failed!, error 22 — nfs_bio.c panics unconditionally on EINVAL

macOS 26.5 (25F71) kernel-panicked three times in five days on one Apple Silicon Mac (Mac17,7), each time while an ordinary unprivileged process wrote files inside an NFSv4 mount. The cause is visible in the NFS client kext's published source (NFS-343.100.5, `kext/nfs_bio.c:4260`): when `ubc_msync` fails while `nfs_vinvalbuf2` invalidates a vnode's cached pages, every errno is retried up to ten times and then logged — except `EINVAL`, which panics the machine immediately. Per the xnu source, `EINVAL` at that call site means the vnode's UBC state was torn down concurrently, so there were no pages left to flush: the invalidation's goal state already held. The kext halts the machine on finding its work already done.

Expected: an invalidation that finds the page cache already gone is a no-op, or at worst a logged retry — the treatment two sibling paths in the same file already give this exact condition. Actual: unprivileged file I/O takes down the kernel.

## What happened

Three panics on one machine, all on macOS 26.5 (25F71), kernel `xnu-12377.121.6~2/RELEASE_ARM64_T6050`, hardware Mac17,7. The table lists each occurrence with the local (PDT) calendar time from its panic report:

| # | Panic time (PDT) | Panic | Uptime at panic |
|---|------------------|-------|-----------------|
| 1 | 2026-06-27 03:35:18 | Kernel data abort inside `com.apple.filesystems.nfs`: NULL dereference (far `0x90`, esr `0x96000006`, pc at `nfs + 0x5aee0`) | 11.4 days |
| 2 | 2026-06-27 03:52:26 | `"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260` | 16 min 50 s |
| 3 | 2026-07-01 14:53:04 | `"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260` | 4.5 days |

The panicked task was the same unprivileged application every time — a developer CLI tool (Claude Code, whose executable is named by its version: `2.1.191`, `2.1.195`, `2.1.197` across auto-updates), running as uid 501, pids 36090, 17406, and 66615 — doing ordinary file reads and writes inside the mount.

Panics 2 and 3 are the same crash. After subtracting each boot's kext slide, their panicked-thread backtraces are frame-for-frame identical across all 22 kernel-space frames — same module, same offset — four days and two application versions apart; the one remaining frame, the userspace return address, differs raw between the reports, as expected under per-boot shared-cache ASLR. Read bottom-up, the chain is: a userspace syscall enters the kernel, passes through `com.apple.security.quarantine` (the MACF policy that stamps quarantine and provenance extended attributes), descends through VFS into `com.apple.filesystems.nfs` (frames at `nfs + 0x352cc`, `+ 0x606b8`, `+ 0x6221c`), and dies at the panic call in `nfs_vinvalbuf2` (`nfs + 0x9a74c`, the exact caller address in the panic string). The operation that killed the machine was metadata stamping the OS performed on the application's behalf, not the application's own read or write.

Panic 1 is the same entry path with a harder ending. Its backtrace runs through the identical quarantine and VFS dispatch frames into the NFS kext, where instead of reaching a `panic()` call it dereferenced a field at offset `0x90` of a NULL pointer. The state it raced was gone before the code reached its own check — evidence the teardown race reaches state the kext dereferences, not just the branch that panics deliberately.

Panic 2 struck 16 minutes 50 seconds after the reboot from panic 1, as soon as the mounts were restored at login. Memory pressure was not a factor: panic 2's report shows an empty compressor and no memory-pressure flag on a sixteen-minute-old boot.

## Source analysis

The panic site is the tail of `nfs_vinvalbuf2` in [NFS-343.100.5 `kext/nfs_bio.c`](https://github.com/apple-oss-distributions/NFS/blob/NFS-343.100.5/kext/nfs_bio.c) — the routine that flushes and invalidates a vnode's buffers whenever the client must discard cached data. Lines 4256–4271:

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

The `panic()` sits on line 4260, matching the panic string byte for byte. The error policy is inverted: a genuine I/O failure while pushing dirty pages — `EIO`, `ENXIO`, anything real — gets ten retries and then a log line, while `EINVAL`, the one errno that never indicates an I/O problem at this call site, halts the machine instantly.

`EINVAL` cannot come from the pager here; `ubc_msync` manufactures it. From [xnu-12377.121.6 `bsd/kern/ubc_subr.c`](https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/kern/ubc_subr.c), lines 1775–1791:

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

`EINVAL` means `ubc_msync_internal` returned 0 without an I/O error. Given the flags `nfs_vinvalbuf2` always passes (`UBC_PUSHALL | UBC_SYNC | UBC_INVALIDATE`, nfs_bio.c:4202) and the caller's own `size > 0` guard, that happens in exactly two ways: the vnode's `ubc_info` is already gone (the `!UBCINFOEXISTS` guard, ubc_subr.c:1865–1867), or `memory_object_lock_request` against `vp->v_ubcinfo->ui_control` returns non-success without an I/O error because the memory object's control state was torn down underneath the request (ubc_subr.c:1909, 1915). Both describe one condition: the vnode's page-cache machinery was destroyed between `nfs_vinvalbuf2`'s `UBCINFOEXISTS` check on line 4257 and the flush on line 4258. That is a check-to-use race against vnode reclaim — a window the caller cannot close — and the "failure" it produces is vacuous, because an invalidation that finds zero pages to invalidate has already achieved its goal state.

The same file already treats this condition as benign, twice. `nfs_vinvalbuf_internal` handles the identical teardown surfacing through UPL setup with a comment and no action ("vm object must no longer exist", nfs_bio.c:4061–4066), and `nfs_buf_release` merely logs any `ubc_msync` failure, `EINVAL` included (nfs_bio.c:1348–1350). The lineage explains the leftover: in Mac OS X 10.5 (xnu-1228, `bsd/nfs/nfs_bio.c:3677`) any failure of this flush panicked; the retry-then-log path was added later for real errors, and the one errno that never indicated a real error stayed behind on the panic branch.

## Reproduction context

Full disclosure on the environment: the mounts were served by fuse-t 1.2.7, a third-party kextless FUSE implementation that runs a userspace NFSv4 server on localhost and mounts it through the macOS NFS client. Behind it ran our own userspace filesystem daemon (`fusekit-holder`, a Go process), which appears alive in the process tables of both full panic reports (pids 2035 and 60580) — this was not a dead-server case. The workload was the CLI rewriting small JSON state files at high frequency inside the mount: an atomic-rename rewrite of a config file every few seconds, plus reads across the tree.

Two properties of the stack made invalidation constant. The mounts used `-o namedattr`, so every xattr operation walked the NFSv4 named-attribute path — `nfs4_vnop_setxattr` (nfs4_vnops.c:9289) driving `nfs4_named_attr_get` (nfs4_vnops.c:8401), which opens, writes, and closes a hidden attribute vnode — and macOS itself generated that xattr traffic by stamping provenance attributes on every file the application created, which is why `com.apple.security.quarantine` sits in all three backtraces. On top of that, the server revision that panicked served unstable attributes for a handful of synthetic files (fileid churn on every atomic-rename write-through, size and mtime flapping, a cache-defeating probe file), so the client invalidated aggressively. A predecessor revision with stable attributes ran the same workload on identical mount options for nearly two weeks without incident. We have since fixed the attribute instability on our side; that lowers the trigger frequency but cannot close the race, which sits between the kext's `UBCINFOEXISTS` check and its `ubc_msync` call.

None of that context changes the verdict:

1. An NFS server is untrusted input. The client kext must remain memory-safe against any sequence of attributes, fileids, and errors a server produces — servers reboot, export snapshots, and have bugs, and nothing in NFSv4 lets a server opt the client out of soundness. Attribute churn may legitimately cost cache correctness or performance; it must never cost the kernel.
2. The trigger is unprivileged. Every panicking thread was uid 501 doing ordinary file I/O, and any local user can start a userspace NFS server and mount it.
3. The panic condition is vacuous. `EINVAL` here means "there was nothing to flush" — the invalidation's goal state already holds, and the same file tolerates the identical condition in two sibling paths.
4. The race is the kernel's own. Userspace controls how often invalidation runs, not the interleaving between the `UBCINFOEXISTS` check and the flush; no server-side or client-userspace discipline can close that window.

## Reproduction steps — to be filled from VM validation (plan Phase 4)

> **Placeholder — intentionally unwritten in this draft.** Before submission, this section receives the deterministic reproduction from an isolated macOS 26.5 (25F71) virtual machine: the scripted workload (localhost userspace NFSv4 server plus a file-churn driver exercising atomic-rename rewrites and xattr stamping), the observed time-to-panic, and confirmation that the resulting panic string and backtrace match the attached reports. The report is submitted only after these steps are validated.

## Requested fix

Route `EINVAL` through the path the function already has. The minimal change deletes the `EINVAL` special case in `nfs_vinvalbuf2` so it is retried and, if it persists, logged like every other errno — the loop already tolerates abandoning the flush after ten attempts, so the data-loss posture does not change. The more precise fix treats the condition as what it is: `EINVAL` here means the vnode's UBC state is gone and there is nothing left to invalidate, so log and return success, exactly as the `nfs_vinvalbuf_internal` UPL path has done since Mac OS X 10.5.

Panic 1 also deserves attention beyond the one-line fix: the same reclaim race reaching a NULL dereference (pc at `nfs + 0x5aee0` in the 25F71 build, faulting address `0x90`) suggests the invalidation path's vnode/UBC lifetime handling merits an audit. The attached report contains the full register state and backtrace.

## Attachments

One ZIP, assembled at submission time (the raw reports are not committed to any repository because they contain device identifiers):

- `panic-base+socd-2026-06-27-033630.000.panic` — panic 1 (kernel data abort)
- `panic-full-2026-06-27-035332.0002.panic` — panic 2
- `panic-full-2026-07-01-145345.0002.panic` — panic 3
- `panic-analysis.md` — the full technical analysis: complete de-slid backtraces for all three panics, the source walk-through summarized above, and the deployment timeline correlating mount activity with each panic
