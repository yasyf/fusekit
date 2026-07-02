# shellcheck shell=bash
# scripts/vm/scenarios/validate-mitigation.sh — the release gate for the
# panic-mitigated holder.
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). Runs the SAME phase-1 organic churn as repro-panic.sh, sustained
# for the full run window, and interleaves the AppleDouble release gate the
# whole way: `._` creation through the mount must fail cleanly (EACCES) while
# ordinary creates/writes keep succeeding under load.
#
# Verdicts: a panic → exit 2 (mitigation failed); any churn or gate failure →
# exit 1, loud in scenario.log (strict on purpose — on the mitigated build a
# transient workload failure is itself signal); a full clean window → exit 0.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
CHURN_CHUNK_S=60
# Window reserved to land the final gate and a graceful stop before vmctl's
# deadline kill.
STOP_MARGIN_S=300

# guest_workload runs one workload.sh command in the guest; its output lands
# in scenario.log.
guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

log "pushing workload.sh into the guest"
vm_scp_to "$SCENARIO_DIR/workload.sh" "$WORKLOAD"

log "setup: vmstress serve + detached mmap readers"
guest_workload setup

vm_phase phase1-churn
log "initial AppleDouble release gate"
guest_workload appledouble-check

while (($(vm_seconds_left) > STOP_MARGIN_S)); do
  left="$(vm_seconds_left)"
  chunk=$((left - STOP_MARGIN_S))
  if ((chunk > CHURN_CHUNK_S)); then
    chunk=$CHURN_CHUNK_S
  fi
  ((chunk > 0)) || break
  # The gate runs DURING the churn burst: ordinary creates must keep
  # succeeding and ._ creates must keep failing EACCES while the workload,
  # its xattr traffic, and the mmap readers are hot.
  guest_workload churn "$chunk" &
  churn_pid=$!
  sleep 5
  guest_workload appledouble-check
  wait "$churn_pid"
done

log "final AppleDouble release gate"
guest_workload appledouble-check
guest_workload status || true
guest_workload stop
log "clean window complete"
