# shellcheck shell=bash
# scripts/vm/scenarios/repro-panic.sh — reproduce the nfs_vinvalbuf2 kernel
# panic against the pushed holder (typically the unmitigated v0.22.1 build).
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). Phase 1 is organic claude-shaped churn only — vmstress serve, an
# open-handle churn loop with provenance-style xattr traffic, and mmap readers
# over the NFS mount. An organic panic (meta.json phase=phase1-organic) IS the
# repro.
#
# Phase 2 is OPT-IN aggravation (VMCTL_REPRO_PHASE2=1): repeated forced
# unmounts under active mmap. Its panics land in meta.json as
# phase=phase2-forced-unmount and must never be presented as organic evidence.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=panic

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
REPRO_PHASE2="${VMCTL_REPRO_PHASE2:-0}"
# Window reserved at the end for phase 2 when it is enabled.
PHASE2_RESERVE_S=600
CHURN_CHUNK_S=60

# guest_workload runs one workload.sh command in the guest; its output lands
# in scenario.log.
guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

log "pushing workload.sh into the guest"
vm_scp_to "$SCENARIO_DIR/workload.sh" "$WORKLOAD"

log "setup: vmstress serve + detached mmap readers"
guest_workload setup

vm_phase phase1-organic
floor=0
if [[ "$REPRO_PHASE2" == "1" ]]; then
  floor=$PHASE2_RESERVE_S
  log "phase 2 aggravation is enabled; reserving ${PHASE2_RESERVE_S}s of the window for it"
fi
while (($(vm_seconds_left) > floor)); do
  left="$(vm_seconds_left)"
  chunk=$((left - floor))
  if ((chunk > CHURN_CHUNK_S)); then
    chunk=$CHURN_CHUNK_S
  fi
  ((chunk > 0)) || break
  # A failed chunk (transient invalidation fallout, a wedged mount) must not
  # end the hunt: remount fresh and keep the pressure on. The staged
  # workload.sh does not survive a panic-reboot, so recovery re-pushes it
  # first. A panic fails the recovery push/setup too, and set -e hands the
  # verdict to the watcher.
  guest_workload churn "$chunk" || {
    warn "churn chunk failed; re-pushing workload.sh and re-running setup to remount and continue"
    vm_scp_to "$SCENARIO_DIR/workload.sh" "$WORKLOAD"
    guest_workload setup
  }
done

if [[ "$REPRO_PHASE2" == "1" ]]; then
  vm_phase phase2-forced-unmount
  log "phase 2: forced unmounts under active mmap (aggravation, labeled separately)"
  while (($(vm_seconds_left) > 0)); do
    guest_workload force-unmount
    sleep 15
    (($(vm_seconds_left) > 90)) || break
    guest_workload setup
    guest_workload churn 60 || {
      warn "phase-2 churn chunk failed; continuing to the next forced unmount"
    }
  done
fi

log "no panic within the window; stopping the workload"
guest_workload status || true
guest_workload stop
