# shellcheck shell=bash
# scripts/vm/scenarios/validate-sillyrename.sh — the release gate for the
# holderfs silly-rename diversion.
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). Same organic churn as validate-mitigation.sh — atomic-save
# rewrites racing self-restarting mmap readers, the exact traffic that makes
# go-nfsv4 silly-rename in-use victims — with two gates interleaved the whole
# way:
#   - the silly-litter gate: unlink-while-open diverts its placeholder into
#     PrivateRoot (shared base stays clean, readdir never lists the class,
#     the held handle keeps serving), and
#   - the AppleDouble release gate (the panic mitigations must hold with the
#     diversion in place).
#
# Verdicts: a panic → exit 2 (diversion broke the mitigation); any churn or
# gate failure → exit 1, loud in scenario.log; a full clean window → exit 0.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
CHURN_CHUNK_S=60
# Window reserved to land the final gates and a graceful stop before vmctl's
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
log "initial gates"
guest_workload appledouble-check
guest_workload sillylitter-check

while (($(vm_seconds_left) > STOP_MARGIN_S)); do
  left="$(vm_seconds_left)"
  chunk=$((left - STOP_MARGIN_S))
  if ((chunk > CHURN_CHUNK_S)); then
    chunk=$CHURN_CHUNK_S
  fi
  ((chunk > 0)) || break
  # Both gates run DURING the churn burst: rename-over-mmap-reader traffic is
  # minting silly placeholders while the checks scan base and readdir.
  guest_workload churn "$chunk" &
  churn_pid=$!
  sleep 5
  guest_workload appledouble-check
  guest_workload sillylitter-check
  wait "$churn_pid"
done

log "final gates"
guest_workload sillylitter-check
guest_workload appledouble-check
guest_workload status || true
guest_workload stop
log "clean window complete"
