# vmctl — a disposable tart VM harness for kernel-panic testing

fusekit's fuse workloads have kernel-panicked real Macs: Apple's NFS kext panics unconditionally when `ubc_msync` returns EINVAL (`nfs_vinvalbuf2: ubc_msync failed!, error 22`; analysis in [docs/reports/panic-analysis.md](../../docs/reports/panic-analysis.md)). Reproducing that on purpose needs a machine we can afford to lose. `vmctl` builds one: a tart macOS VM whose entire footprint lives under `/tmp/fusekit-vm` — VM disk, pulled image cache, ssh key, results — plus a panic watcher that turns "the guest rebooted" into scraped evidence and an exit code.

> **Warning.** Never run scenarios or `vmstress` on bare metal — the workloads provoke kernel panics by design. `vmctl` refuses ssh targets that are not VMs (`kern.hv_vmm_present` must be 1), guest workloads carry the same guard (exit 86), and the holder that `push` builds is never executed on the host.

## Requirements

- An Apple Silicon Mac. tart rides Virtualization.framework and is arm64-only; the builds `push` ships are arm64.
- Homebrew. `vmctl create` runs `brew install cirruslabs/cli/tart` if tart is missing; that install is the only host mutation outside `/tmp/fusekit-vm`, and it announces itself loudly first.
- About 90 GB free in `/tmp` at peak (image cache plus VM disk).

## Quickstart

Run everything from the repo root:

```sh
scripts/vm/vmctl create                            # install tart if needed, clone the image (first pull: 20-60 min)
scripts/vm/vmctl provision                         # boot, ssh key, fuse-t, sleep off, TCC grant
scripts/vm/vmctl push                              # build holder.app + vmstress, install into the guest, selftest
scripts/vm/vmctl run scenarios/repro-panic.sh      # exit 0 means the panic reproduced
```

`run` prints a verdict and leaves evidence under `/tmp/fusekit-vm/results/<ts>-<scenario>/`. When testing ends, copy anything you need into `docs/reports/` assets, then tear it all down:

```sh
scripts/vm/vmctl destroy                           # delete the VM, then rm -rf /tmp/fusekit-vm
```

## Commands

| Command | What it does |
|---|---|
| `create` | Install tart if missing (loud), clone `VMCTL_IMAGE` into `/tmp/fusekit-vm/tart`, apply CPU/memory/disk sizing. Idempotent. |
| `provision` | Ensure `create`, boot the VM, bootstrap the ssh key (one-time `expect`-driven password auth), enable passwordless sudo, install fuse-t, disable sleep, grant Network Volumes TCC. |
| `push` | Delegate to `push.sh`: host-build `fusekit-holder.app` (BUILD ONLY) and `vmstress`, install into the guest, run the in-guest selftest. |
| `run <scenario>` | Execute one scenario under the panic watcher; write `meta.json` and evidence; map the verdict to an exit code. |
| `shell [cmd...]` | ssh into the guest; interactive without arguments. |
| `collect` | Scrape panic reports and guest facts into a fresh results dir, outside any run. |
| `status` | Read-only harness/VM/guest report; always exits 0, even with no tart and no VM. |
| `destroy` | Stop and delete the VM via tart, then remove `/tmp/fusekit-vm` wholesale. |

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `VMCTL_NAME` | `fusekit-test` | tart VM name. |
| `VMCTL_IMAGE` | `ghcr.io/cirruslabs/macos-tahoe-base:latest` | Image to clone (matches the host's macOS 26). |
| `VMCTL_CPUS` | `4` | Guest CPUs. |
| `VMCTL_MEMORY_MB` | `8192` | Guest memory in MB. |
| `VMCTL_DISK_GB` | `60` | Guest disk in GB (tart can only grow an image's disk). |
| `VMCTL_RUN_TIMEOUT_MIN` | `120` | Hard bound on every `run`, validation runs included. There is no unbounded soak mode. |
| `VMCTL_GRAPHICS` | `0` | `1` opens the VM window — needed once for the TCC click-Allow fallback. |
| `VMCTL_TCC_CLIENTS` | `com.apple.sshd-keygen-wrapper com.yasyf.fusekit-holder` | Space-separated Network Volumes grantees: bundle ids, or absolute paths. |
| `BUILD_REV` | short `git` HEAD (`-dirty` when unclean) | `push.sh` only: the revision recorded in the guest and in `meta.json`. |

## Exit codes for `run`

| Code | Meaning |
|---|---|
| 0 | Expectation met: panicked under `EXPECT=panic`, or clean for the full window under `EXPECT=clean`. |
| 1 | Infra failure: VM/ssh unreachable at verdict time, or the workload broke before proving anything. |
| 2 | Guest panicked under `EXPECT=clean` — the mitigation failed. |
| 3 | No repro under `EXPECT=panic` within `VMCTL_RUN_TIMEOUT_MIN`. |

Other commands exit 0 on success and 1 on failure; usage errors exit 64.

## Scenario contract

A scenario is a bash file that `vmctl run` sources on the HOST with `lib.sh` already loaded and `set -euo pipefail` inherited. It drives the guest exclusively through the helpers below; `vmctl` owns the timeout, the panic watcher, tart relaunch, evidence collection, `meta.json`, and exit-code mapping.

1. Declare `EXPECT=panic` or `EXPECT=clean` bare on its own line: `vmctl` greps `^EXPECT=`, so no quotes and no indentation.
2. Touch the guest only via the helpers. Guest-side workload code must guard itself: `sysctl -n kern.hv_vmm_present` must print 1, else exit 86 (`scenarios/workload.sh` owns that guard for shared guest functions).
3. Call `vm_phase <label>` on entering each workload phase; the active label lands in `meta.json`. Labels match `[A-Za-z0-9._-]+`.
4. Bound churn loops with `vm_seconds_left`; `vmctl` kills the scenario process group at the deadline regardless.
5. A guest panic makes in-flight `vm_ssh` calls fail and `set -e` ends the scenario. That is expected: the verdict comes from the watcher, never from the scenario's exit code.

| Helper | Does |
|---|---|
| `vm_ssh <cmd...>` | Run a command in the guest; nonzero when unreachable. |
| `vm_sudo "<cmd>"` | Run one command string as root in the guest. |
| `vm_scp_to <local> <remote>` | Copy a file or directory into the guest. |
| `vm_scp_from <remote> <local>` | Copy a file or directory out of the guest. |
| `vm_phase <label>` | Record the active workload phase for `meta.json`. |
| `vm_seconds_left` | Print seconds until the run deadline (0 when past). |
| `log` / `warn` / `die` | Timestamped stderr logging; `die` exits 1. |

Exported context: `VMCTL_SCENARIO`, `VMCTL_RESULTS_DIR`, `VMCTL_PHASE_FILE`, `VMCTL_DEADLINE_EPOCH`, `VMCTL_GUEST_DIR`, `VMCTL_GUEST_VMSTRESS`, `VMCTL_GUEST_HOLDER_APP`, `VM_GUEST_USER`, `VM_GUEST_HOME`.

A minimal scenario:

```bash
# scenarios/example.sh — sourced by vmctl run; lib.sh is already loaded.
EXPECT=clean

vm_phase phase1-churn
while (($(vm_seconds_left) > 0)); do
  vm_ssh "'$VMCTL_GUEST_VMSTRESS' churn --once"
done
```

## What push installs

| Guest path | Contents |
|---|---|
| `/Applications/fusekit-holder.app` | The `-tags fuse` holder, ad-hoc signed, at the production cask path — `mountd.HolderApp`, `HolderExe`, and `DefaultHolderSocket()` work unmodified in the guest. |
| `~/fusekit-vm/bin/vmstress` | The pure-Go guest driver (`serve`, `churn`, `read --mmap`, `selftest`). |
| `~/fusekit-vm/BUILD_REV` | The pushed revision; `run` refuses to start without it and copies it into `meta.json`. |

`push` kills any holder already serving in the guest first — a surviving holder would keep serving the old build and make `BUILD_REV` a lie — then closes with `vmstress selftest`, which mounts through the freshly pushed holder and proves fuse plus TCC end-to-end.

## Panic detection and evidence

The watcher polls the guest every 10 seconds while the scenario runs:

- Reboot in place: the tart process is alive but `kern.boottime` changed. The guest kernel panicked and auto-restarted; that is the panic verdict.
- tart process death: `vmctl` relaunches `tart run` headless and probes for `.panic` reports newer than the run-start marker. A relaunch always yields a fresh boottime, so boottime alone is not evidence on this path.
- Scenario end or timeout: one final pass re-checks boottime and fresh `.panic` reports, waiting up to 5 minutes for a guest mid-reboot, so a panic landing just as the workload stops still counts.

On a panic, `vmctl` scrapes `/Library/Logs/DiagnosticReports/*.panic` and `/Library/Logs/DiagnosticReports/Retired/*.panic` from the guest via sudo into `results/<ts>-<scenario>/panics/`.

`meta.json` records: `scenario`, `expect`, `build_rev`, `phase` (the last `vm_phase` label active when the run ended), `started_at`/`ended_at`/`duration_s`, `timeout_min`, `panicked`, `panic_reports_new`, `boottime_start`/`boottime_end`, `tart_relaunches`, `end_reason`, `scenario_exit`, `exit_code`, and `note`.

## TCC (Network Volumes)

Reading a fuse-t volume is NFS access, which macOS gates behind the `kTCCServiceSystemPolicyNetworkVolumes` prompt. Headless guests cannot click Allow, so:

1. **Automatic (default)**: `provision` inserts grants for each `VMCTL_TCC_CLIENTS` entry into the guest's system TCC.db (`/Library/Application Support/com.apple.TCC/TCC.db`) and bounces `tccd`. This works only when the image ships with SIP disabled (cirruslabs images do); with SIP enabled, `provision` warns and moves on.
2. **Click-Allow fallback (once)**: `VMCTL_GRAPHICS=1 scripts/vm/vmctl provision` boots the VM with a window. Run `scripts/vm/vmctl push` — its selftest triggers the prompt — and click Allow inside the VM window. The grant persists for the VM's lifetime.

The defaults cover both access paths: `com.apple.sshd-keygen-wrapper` is the TCC responsible process for everything run over ssh, and `com.yasyf.fusekit-holder` covers the holder itself, which LaunchServices (`open -g`) makes its own responsible process. Verification is always the same: `scripts/vm/vmctl push` fails loudly on a TCC denial in its closing selftest.

## Budgets

| Cost | Size |
|---|---|
| Image pull | 20-60 min and tens of GB, re-incurred after every `destroy` — the cache lives inside `/tmp/fusekit-vm`, the accepted price of clean-up-when-done. |
| Disk | ~90 GB peak in `/tmp` (image cache + VM disk). |
| `provision` through `push` selftest | ~15 min on a warm image. |
| Any `run` | Bounded by `VMCTL_RUN_TIMEOUT_MIN` (default 120 min). |

## State layout

```
/tmp/fusekit-vm/
├── tart/          # TART_HOME: VM disk + pulled image cache (never ~/.tart)
├── ssh/           # generated ed25519 key pair
├── stage/         # push.sh build staging
├── state/         # build-rev of the last push
├── logs/          # tart run logs
├── run/           # tart pidfile
└── results/       # <ts>-<scenario>/ {meta.json, scenario.log, phase, panics/}
```

`destroy` removes all of it after deleting the VM through tart. Results die with it — copy panic scrapes and `meta.json` into `docs/reports/` assets first.

## Reusing the harness in another repo

`vmctl` and `lib.sh` are repo-agnostic; everything fusekit-specific sits in `push.sh` and `scenarios/`. To adopt it:

1. Copy `vmctl` and `lib.sh`; adjust the `VM_ROOT` prefix if you want a different `/tmp` namespace.
2. Write your own `push.sh`: build your binaries, install them into the guest, write the revision to `$VMCTL_GUEST_DIR/BUILD_REV`, and end with an in-guest selftest that exercises whatever TCC-gated surface you rely on.
3. Override `VMCTL_NAME`, `VMCTL_IMAGE`, and `VMCTL_TCC_CLIENTS` for your consumers.
4. Write scenarios against the contract above — the panic watcher, evidence scrape, exit-code mapping, and the /tmp-only lifecycle come for free.
