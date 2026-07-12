# shellcheck shell=bash
# scripts/vm/scenarios/p5-busy-bounce.sh — S8: unmount of a lease-held dir
# bounces ClassBusy carrying the acquirer's provenance; nothing is torn down
# (kernel mount, registry row, journal row, and bridge all intact); the same
# unmount succeeds once the lease releases.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

row_live() { holder_req '{"proto":2,"op":"list","owner":"vmstress"}' | grep -qF "\"dir\":\"$MOUNT_DIR\",\"base\":\"$STATE_DIR/base\",\"live\":true"; }

vm_phase setup
p5_install
p5_reset
guest_workload setup
p5_stop_readers
LH_PID="$(p5_leasehold_start "$MOUNT_DIR" p5busy)"
p5_await 15 "session lease held on $MOUNT_DIR" p5_leasehold_held p5busy

vm_phase bounce
resp="$(holder_req "$(vmstress_unmount_json)" 120s)"
log "unmount under lease answered: $resp"
resp_ok "$resp" && die "unmount SUCCEEDED under a held session lease"
[[ "$(resp_class "$resp")" == "busy" ]] || die "expected err_class busy, got: $resp"
grep -q 'p5busy' <<<"$resp" || die "busy bounce does not carry the acquirer's provenance (owner p5busy missing): $resp"
grep -q "$LH_PID" <<<"$resp" || warn "busy provenance does not name the acquirer pid $LH_PID (owner matched): $resp"

mounted "$MOUNT_DIR" || die "bounced unmount tore the kernel mount down"
row_live || die "bounced unmount dropped the registry row"
journal_has_dir "$MOUNT_DIR" || die "bounced unmount dropped the journal row"
guest_read_ok "$MOUNT_DIR/config.json" || die "bridge no longer serves after the bounce"

vm_phase release-unmount
vm_ssh "kill $LH_PID"
released_unmount() {
  local r
  r="$(holder_req "$(vmstress_unmount_json)" 120s)" || return 1
  resp_ok "$r"
}
p5_await 60 "unmount succeeds after lease release" released_unmount
if mounted "$MOUNT_DIR"; then die "post-release unmount answered OK but the mount is still up"; fi

vm_phase teardown
guest_workload stop
p5_reset
log "S8 PASS: lease-held unmount bounced busy with provenance, nothing torn, post-release unmount clean"
