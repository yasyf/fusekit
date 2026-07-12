# shellcheck shell=bash
# scripts/vm/scenarios/p5-journal-legacy.sh — S5: a synthesized LEGACY journal
# (rows carrying idle_policy/carcass_policy fields) replays on a fresh holder:
# the row is served, the legacy policy fields decode away (no idle unmount, no
# force), and the rewritten journal carries no policy fields.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

L_BASE="$VMCTL_GUEST_DIR/legacy/base" L_MNT="$VMCTL_GUEST_DIR/legacy/mnt"

row_live() { holder_req '{"proto":2,"op":"list","owner":"legacy-owner"}' | grep -qF "\"dir\":\"$L_MNT\",\"base\":\"$L_BASE\",\"live\":true"; }
journal_clean() { ! vm_ssh "grep -qE 'carcass_policy|idle_policy' '$JOURNAL'"; }

vm_phase setup
p5_install
p5_reset
vm_ssh "mkdir -p '$L_BASE' '$L_MNT' '$GUEST_FUSEKIT' && echo legacy-canary >'$L_BASE/canary.txt'"

cat >"$VMCTL_RESULTS_DIR/legacy-specs.json" <<EOF
{
  "mounts": [
    {
      "base": "$L_BASE",
      "dir": "$L_MNT",
      "owner": "legacy-owner",
      "idle_policy": "unmount-when-idle",
      "carcass_policy": "force"
    }
  ],
  "bridges": []
}
EOF
vm_scp_to "$VMCTL_RESULTS_DIR/legacy-specs.json" "$JOURNAL"

vm_phase replay
holder_launch
p5_await 120 "legacy row replayed live" row_live
guest_read_ok "$L_MNT/canary.txt" || die "replayed legacy row does not serve reads"

vm_phase policy-absence
# idle_policy must be inert: the mount stays up through an idle minute.
definitely_unmounted() {
  local t
  t="$(vm_ssh mount)" || return 1
  ! grep -qF " on $L_MNT (" <<<"$t"
}
p5_never 60 "legacy idle_policy unmounted the idle row" definitely_unmounted
guest_read_ok "$L_MNT/canary.txt" || die "row stopped serving during the idle window"
# The rewritten journal must carry no policy fields.
p5_await 60 "journal rewritten without policy fields" journal_clean
journal_has_dir "$L_MNT" || die "rewritten journal lost the row itself"

vm_phase teardown
resp="$(holder_req "$(plain_unmount_json "$L_BASE" "$L_MNT" legacy-owner)" 120s)"
resp_ok "$resp" || die "cleanup unmount failed: $resp"
p5_reset
log "S5 PASS: legacy journal replayed, policy fields ignored and scrubbed on rewrite"
