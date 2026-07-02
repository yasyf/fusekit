<!-- status: submitted 2026-07-02 — https://github.com/macos-fuse-t/fuse-t/issues/109 -->
<!-- filed title: macOS kernel panic "nfs_vinvalbuf2: ubc_msync failed!, error 22" reached through a fuse-t mount — Apple NFS kext bug (FB23527406 filed), not a fuse-t defect -->

Filing this for coordination and searchability, not as a fuse-t defect — feel free to close once read. Three kernel panics in five days on macOS 26.5 (25F71, xnu-12377.121.6, Apple Silicon), each raised through a fuse-t 1.2.7 mount, trace to Apple's NFS client kext: `nfs_vinvalbuf2` halts the machine unconditionally when a page-cache invalidation races vnode reclaim, on a condition that is not an error, in a window only Apple can close. The trigger was our own FUSE filesystem serving unstable attributes. fuse-t behaved correctly throughout — it relayed exactly what our server handed it, and the kernel has to stay up no matter what an NFS server serves.

Posting here because every fuse-t mount is a localhost NFSv4 mount, which makes fuse-t filesystems the most likely place this panic surfaces — and a web search for the panic string turns up nothing public today. Below: the mechanism, what filesystem authors can do to stay out of the race window, and one existing issue (#75) that looks like the same kext fragility. The full source-level analysis behind our Apple report is in [docs/reports/panic-analysis.md](https://github.com/yasyf/fusekit/blob/main/docs/reports/panic-analysis.md) in the fusekit repo.

## The panic

Two of the three panics carry the identical string; the first was a kernel data abort inside the same kext, entered through the same call path (more on that under #75 below). The panicked task was an ordinary unprivileged process every time — a CLI (Claude Code) doing atomic-rename rewrites of small JSON files inside the mount — and the FUSE server process was alive in both full panic reports (the first panic produced only a minimal report with no process table), so this is not a dead-server case.

```text
"nfs_vinvalbuf2: ubc_msync failed!, error 22" @nfs_bio.c:4260
```

The two string panics have frame-for-frame identical panicked-thread backtraces across all 22 kernel-space frames once each boot's kext slide is subtracted, four days apart (the final frame, a userspace return address, differs raw, as expected under per-boot shared-cache ASLR). The frames that tell the story (from the most recent panic; panic-machinery frames 0-6 and syscall-entry/userspace frames 15-22 trimmed):

```text
frame   module + offset
  7     nfs        + 0x9a74c    panic caller: nfs_vinvalbuf2, nfs_bio.c:4260
  8     nfs        + 0x6221c
  9     nfs        + 0x606b8
 10     nfs        + 0x352cc
 11     kernel     + 0x2fa720
 12     kernel     + 0x96b964
 13     quarantine + 0x6714     com.apple.security.quarantine MACF hook
 14     quarantine + 0x5170
```

Read bottom-up: a userspace syscall enters the kernel, passes through `com.apple.security.quarantine` — macOS stamping quarantine/provenance extended attributes on the application's behalf — and dies in `com.apple.filesystems.nfs`. The operation that killed the machine was the OS's own metadata stamping, not the application's read or write.

It died at the tail of `nfs_vinvalbuf2` ([NFS-343.100.5, `kext/nfs_bio.c:4256-4271`](https://github.com/apple-oss-distributions/NFS/blob/NFS-343.100.5/kext/nfs_bio.c#L4256-L4271)), where `EINVAL` alone is special-cased into a panic while every other errno gets ten retries and a log line:

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

## Mechanism

`EINVAL` is the one errno at that call site that never means an I/O failure. `ubc_msync` cannot relay it from the pager; it manufactures the value itself ([xnu-12377.121.6, `bsd/kern/ubc_subr.c:1775-1791`](https://github.com/apple-oss-distributions/xnu/blob/xnu-12377.121.6/bsd/kern/ubc_subr.c#L1775-L1791)) when the flush found nothing to do — which, given the flags this call site always passes, means the vnode's UBC state was torn down between the kext's `UBCINFOEXISTS` check one line up and the `ubc_msync` call itself. That is a check-to-use race against vnode reclaim, and the "failure" is vacuous: an invalidation that finds zero pages to invalidate has already reached its goal state. The same file treats the identical teardown as benign in two sibling paths — the UPL-setup path comments "vm object must no longer exist" and moves on (nfs_bio.c:4061-4066), and `nfs_buf_release` merely logs (nfs_bio.c:1348-1350). Only this call site panics, a leftover from Mac OS X 10.5 where any failure here halted the machine; the retry/log path was added later and `EINVAL` never moved onto it.

Userspace cannot close that window, but it controls how often the race runs, because invalidation frequency tracks attribute churn. The filesystem revision that panicked served unstable attributes for a few synthetic files: the fileid changed on every atomic-rename write-through (we passed the backing file's real inode through) while a writer held the path open; size and mtime could flap or regress between GETATTRs; and a health-probe file advanced its mtime on every GETATTR. Each change the client notices is answered with `nfs_vinvalbuf2` on a vnode whose identity keeps shifting under a live writer. `-o namedattr` multiplied the vnode churn: on a namedattr mount, every xattr operation — including the quarantine/provenance stamping macOS performs on every file create, no application code involved — opens, writes, and closes a hidden attribute vnode via OPENATTR (`nfs4_vnops.c:9289`, driving `nfs4_named_attr_get`, `nfs4_vnops.c:8401`), mass-producing exactly the short-lived vnodes the reclaim race needs. The dose-response was clean: a predecessor revision with stable attributes ran the identical workload for nearly two weeks on identical mount options, namedattr included, without incident; the unstable revision took the machine down within half an hour of its mounts coming up, three times out of three.

## Status with Apple

FB number: FB23527406 — filed under macOS > System Crashes / Unexpected Reboot, with the three host panic reports, a VM-reproduction panic report, and the source-level analysis attached. The ask is minimal: route `EINVAL` through the retry/log path `nfs_vinvalbuf2` already has for every other errno, or log and return success as the sibling UPL path has done since Mac OS X 10.5. Until that lands, any NFS mount — fuse-t or network, healthy server or buggy — can hit this; server-side hygiene lowers the frequency but cannot close the kernel's window.

## What fuse-t filesystem authors can do

We fixed our side — stable synthetic fileids, monotonic mtimes, no size flapping, probe churn removed, `namedattr` dropped — and the rules generalize to any fuse-t filesystem:

- Keep served attributes stable. A path's fileid should survive rewrites: if your backing storage churns inodes (an atomic-rename rewrite mints a new one every time), mint a stable synthetic fileid instead of passing the backing inode through, especially for paths a client holds open. Never let size or mtime regress or disagree between GETATTRs. Every instability the client notices turns into an invalidation pass over that vnode.
- Skip `-o namedattr` unless you need real xattr round-trips. Named attributes route every xattr op through short-lived hidden attribute vnodes in the kext, and macOS generates xattr traffic on your behalf: quarantine/provenance stamping on every create — the `com.apple.quarantine`/`com.apple.provenance` blobs in the `._` hexdump over in #81 ("._ files always created when mounting") are exactly this traffic. Without namedattr, xattr writes return ENOTSUP and macOS falls back to AppleDouble `._` sidecars — the litter #81 describes; we suppress those in the filesystem itself, macFUSE `noappledouble`-style (refuse `._*` creates, hide existing ones), instead of re-enabling namedattr.
- Serve no cache-defeating files. A file whose attributes change on every GETATTR (a liveness probe, a counter) is a standing invalidation generator.

These mitigations lower how often you roll the dice; only the kext fix removes the race.

## Possibly the same bug: #75

#75 ("macOS 15 system crash when copy file using finder") reports a kernel panic while Finder copies files onto a fuse-t mount: a page fault at fault address `0x90`, faulting instruction inside `com.apple.filesystems.nfs` (macOS 15.1, x86_64). Our first panic was a kernel data abort at faulting address `0x90` inside the same kext (macOS 26.5, arm64), entered through the same quarantine-stamping path as the two string panics — and Finder copies drag xattr traffic along with the data. Same fault offset, same kext, on x86_64 and arm64, on macOS 15 and macOS 26. I can't prove from the outside that it's the same defect, but it reads like the same lifecycle race reaching a NULL dereference before the explicit panic check — which would make #75 an Apple bug too, not a fuse-t one. Our VM reproduction strengthens the link: driving attribute churn through fuse-t against the unstable-attribute server build panics a macOS 26.5 guest deterministically — 28 attempts, 28 panics, typically within ~2 seconds — and every one lands on this data abort (`far 0x90`, de-slid pc at `nfs + 0x5aee0`), not the panic string, so #75's signature is the race's most common ending under churn.

## The repro harness

Reproducing a kernel panic on purpose needs a machine you can afford to lose, so we built a disposable-VM harness: [`scripts/vm` in the fusekit repo](https://github.com/yasyf/fusekit/tree/main/scripts/vm). It clones a tart macOS VM entirely under `/tmp`, provisions fuse-t in the guest, drives attribute-churn workloads under a panic watcher, and scrapes the guest's `.panic` reports into evidence, with exit codes mapped to expectations (panic expected vs. clean run expected). On a macOS 26.5 (25F71) guest it reproduces this panic deterministically against our unstable-attribute build (28/28, ~2 s to panic) and validates the fixed build clean under the identical workload (72+ min, 45k+ write-through saves, no panic). If it's useful for testing fuse-t against this class of kernel fragility, take it — happy to help adapt it.
