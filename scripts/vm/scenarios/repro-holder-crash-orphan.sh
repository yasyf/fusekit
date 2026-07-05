# shellcheck shell=bash
# scripts/vm/scenarios/repro-holder-crash-orphan.sh — the release gate for
# dead-holder go-nfsv4 orphan recovery (the 2026-07-03 fleet incident: the
# holder died via SIGTRAP inside libc exit() and its orphaned servers kept
# their sockets, answering EPERM to every op on every mount).
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). Each cycle: mount under load, kill the holder in-guest
# (alternating SIGKILL / SIGTRAP — the incident signal), then per arm:
#   - default arm: the orphans survive the crash; the RESPAWNED holder (the
#     harness analog of launchd KeepAlive is the next setup's EnsureRunning)
#     must read the EPERM mount as a carcass, reap the orphans, and remount;
#     then a zero-EIO read/churn gate runs through the recovered mount.
#   - VMCTL_KILL_GROUP=1 arm (the FUSEKIT_HOLDER_KILL_GROUP ship gate): the
#     spawned servers must die WITH the holder within ORPHAN_WAIT_S — no reap
#     needed — and the holder must relaunch cleanly every cycle. This arm
#     requires all TARGET_CYCLES kill cycles inside the window; raise
#     VMCTL_RUN_TIMEOUT_MIN if they do not fit.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
MOUNT_DIR="$VMCTL_GUEST_DIR/mnt"
KILL_GROUP="${VMCTL_KILL_GROUP:-0}"
TARGET_CYCLES=10
# Bound on orphan/holder state transitions after a kill.
ORPHAN_WAIT_S=20
# Post-recovery churn length per cycle: long enough to surface EIO, short
# enough to fit TARGET_CYCLES cycles in the window.
EIO_CHECK_S=20
# Window reserved to land the final gates and a graceful stop before vmctl's
# deadline kill.
STOP_MARGIN_S=240

# guest_workload runs one workload.sh command in the guest; its output lands
# in scenario.log.
guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

# holder_pid / server_pids read guest process state ('' / empty when absent).
holder_pid() { vm_ssh "pgrep -x fusekit-holder | head -n 1" 2>/dev/null || true; }
server_pids() { vm_ssh "pgrep -x go-nfsv4" 2>/dev/null || true; }

# wait_no_servers polls until no go-nfsv4 survives, bounded by $1 seconds.
wait_no_servers() {
  local timeout="$1" t0
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    [[ -z "$(server_pids)" ]] && return 0
    sleep 1
  done
  return 1
}

# assert_recovered_mount is the zero-EIO gate: plain reads plus a bounded
# churn burst through the recovered mount, all of which fail loudly on EIO.
assert_recovered_mount() {
  vm_ssh "cat '$MOUNT_DIR/config.json' >/dev/null" || die "post-recovery read of config.json failed"
  vm_ssh "dd if='$MOUNT_DIR/mmap.dat' of=/dev/null bs=1m 2>/dev/null" || die "post-recovery bulk read of mmap.dat failed"
  guest_workload churn "$EIO_CHECK_S" || die "post-recovery churn saw errors through the mount"
}

log "pushing workload.sh into the guest"
vm_scp_to "$SCENARIO_DIR/workload.sh" "$WORKLOAD"

if [[ "$KILL_GROUP" == "1" ]]; then
  log "arm: process-group gate (FUSEKIT_HOLDER_KILL_GROUP=1 via launchctl setenv — the LaunchServices holder inherits launchd's user env, not this shell's)"
  vm_ssh "launchctl setenv FUSEKIT_HOLDER_KILL_GROUP 1"
else
  log "arm: orphan detect+reap (kill-group gate off)"
  vm_ssh "launchctl unsetenv FUSEKIT_HOLDER_KILL_GROUP || true"
fi

log "setup: vmstress serve + detached mmap readers"
guest_workload setup

vm_phase kill-cycles
cycle=0
while ((cycle < TARGET_CYCLES)) && (($(vm_seconds_left) > STOP_MARGIN_S)); do
  cycle=$((cycle + 1))
  # Alternate the two incident-shaped deaths: SIGKILL (no exit path at all)
  # and SIGTRAP (the libc-exit() crash signature).
  sig=KILL
  ((cycle % 2 == 0)) && sig=TRAP

  hpid="$(holder_pid)"
  [[ -n "$hpid" ]] || die "cycle $cycle: no fusekit-holder running before the kill"
  pre_servers="$(server_pids)"
  [[ -n "$pre_servers" ]] || die "cycle $cycle: no go-nfsv4 servers before the kill — nothing mounted?"

  log "cycle $cycle/$TARGET_CYCLES: killing holder pid $hpid with SIG$sig (servers: $(echo "$pre_servers" | tr '\n' ' '))"
  # Load stays on: the readers from setup keep hammering the mount while the
  # holder dies, exactly the incident shape.
  vm_ssh "kill -$sig $hpid" || die "cycle $cycle: kill -$sig $hpid failed"

  # The holder itself must be gone (any arm).
  t0="$(date +%s)"
  while [[ -n "$(holder_pid)" ]]; do
    (($(date +%s) - t0 < ORPHAN_WAIT_S)) || die "cycle $cycle: holder pid survived SIG$sig for ${ORPHAN_WAIT_S}s"
    sleep 1
  done

  if [[ "$KILL_GROUP" == "1" ]]; then
    # Gate arm: the servers die WITH the holder — no successor, no reap.
    wait_no_servers "$ORPHAN_WAIT_S" ||
      die "cycle $cycle: go-nfsv4 outlived the holder by ${ORPHAN_WAIT_S}s with the kill-group gate ON: $(server_pids | tr '\n' ' ')"
    log "cycle $cycle: all servers died with the holder"
  else
    # Default arm: orphans are EXPECTED to survive the crash (that is the
    # incident); note whether they did so the log shows what recovery faced.
    orphans="$(server_pids)"
    if [[ -n "$orphans" ]]; then
      log "cycle $cycle: orphaned servers survived as expected: $(echo "$orphans" | tr '\n' ' ')"
    else
      warn "cycle $cycle: no orphans survived SIG$sig (nothing to reap this cycle)"
    fi
  fi

  # Recovery: the next setup respawns the holder (the KeepAlive analog) which
  # must clear any EPERM carcass and its orphaned servers, then remount.
  log "cycle $cycle: respawning holder + remounting via setup"
  guest_workload setup || die "cycle $cycle: recovery setup failed — respawned holder could not clear the carcass and remount"

  # Every pre-kill server pid must be gone: detected as serving a carcass and
  # reaped (default arm) or dead with the holder (gate arm). The new mount's
  # own fresh server is the only go-nfsv4 allowed to remain.
  for pid in $pre_servers; do
    if vm_ssh "kill -0 $pid" 2>/dev/null; then
      die "cycle $cycle: pre-kill go-nfsv4 pid $pid still alive after recovery — orphan not reaped"
    fi
  done
  [[ -n "$(server_pids)" ]] || die "cycle $cycle: no go-nfsv4 serving the recovered mount"

  assert_recovered_mount
  log "cycle $cycle: recovered clean (zero EIO)"
done

((cycle > 0)) || die "window too short for a single kill cycle; raise VMCTL_RUN_TIMEOUT_MIN"
if [[ "$KILL_GROUP" == "1" ]] && ((cycle < TARGET_CYCLES)); then
  die "kill-group gate arm completed only $cycle/$TARGET_CYCLES cycles; raise VMCTL_RUN_TIMEOUT_MIN — the ship gate needs all $TARGET_CYCLES"
fi
((cycle == TARGET_CYCLES)) || warn "completed $cycle/$TARGET_CYCLES cycles within the window"

vm_phase final-gates
log "final gate: recovered mount serves under load"
assert_recovered_mount
guest_workload status || true
guest_workload stop
if [[ "$KILL_GROUP" == "1" ]]; then
  vm_ssh "launchctl unsetenv FUSEKIT_HOLDER_KILL_GROUP || true"
fi
log "clean window complete: $cycle kill cycle(s), orphans cleared, zero post-recovery EIO"
