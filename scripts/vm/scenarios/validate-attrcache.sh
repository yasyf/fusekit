# shellcheck shell=bash
# scripts/vm/scenarios/validate-attrcache.sh — the release gate for the
# per-mount AttrCache opt-in (fusekit MountOptions.AttrCache).
#
# Sourced on the HOST by `vmctl run` with lib.sh loaded (README: scenario
# contract). Same organic churn as validate-mitigation.sh, but the guest serve
# opts the mount INTO the go-nfsv4 attribute cache (--attrcache
# --attrcache-timeout=5s) — the configuration cc-pool wants to ship — and two
# gates interleave the whole way:
#   - the AppleDouble release gate (mitigations must hold under a cache), and
#   - the torn-read gate: every through-mount synth read must parse as a
#     complete envelope (a stale-size clamp truncates the JSON) with Gen never
#     regressing.
# A dedicated final phase runs tornread --writer: external consumer-side
# grow/shrink rewrites with a measured bound on how stale a through-mount
# read can be — the number the cc-pool opt-in decision reads.
#
# Verdicts: a panic → exit 2 (attrcache broke the mitigation); any churn,
# AppleDouble, or torn-read failure → exit 1, loud in scenario.log; a full
# clean window → exit 0.
# shellcheck disable=SC2034 # EXPECT is the contract marker vmctl greps (^EXPECT=)
EXPECT=clean

: "${VMCTL_GUEST_DIR:?scenarios only run under scripts/vm/vmctl run}"

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
CHURN_CHUNK_S=60
TORNREAD_CHUNK_S=15
# Window reserved for the dedicated --writer tornread phase, the final gates,
# and a graceful stop before vmctl's deadline kill.
STOP_MARGIN_S=300

# guest_workload runs one workload.sh command in the guest; its output lands
# in scenario.log.
guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

log "pushing workload.sh into the guest"
vm_scp_to "$SCENARIO_DIR/workload.sh" "$WORKLOAD"

log "setup: vmstress serve --attrcache --attrcache-timeout=5s + detached mmap readers"
guest_workload setup --attrcache --attrcache-timeout=5s

vm_phase phase1-churn
log "initial gates"
guest_workload appledouble-check
guest_workload tornread "$TORNREAD_CHUNK_S"

while (($(vm_seconds_left) > STOP_MARGIN_S)); do
  left="$(vm_seconds_left)"
  chunk=$((left - STOP_MARGIN_S))
  if ((chunk > CHURN_CHUNK_S)); then
    chunk=$CHURN_CHUNK_S
  fi
  ((chunk > 0)) || break
  # Both gates run DURING the churn burst: ._ creates must keep failing
  # cleanly and every synth read must stay a complete envelope while the
  # workload's atomic-save rewrites flip sizes under the enabled attr cache.
  guest_workload churn "$chunk" &
  churn_pid=$!
  sleep 5
  guest_workload appledouble-check
  guest_workload tornread "$TORNREAD_CHUNK_S"
  wait "$churn_pid"
done

vm_phase phase2-tornread-writer
log "dedicated torn-read measurement: external rewrites, churn quiet"
guest_workload tornread 45 --writer

log "final AppleDouble release gate"
guest_workload appledouble-check
guest_workload status || true
guest_workload stop
log "clean window complete"
