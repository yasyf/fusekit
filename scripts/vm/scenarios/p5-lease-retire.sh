# shellcheck shell=bash
# scripts/vm/scenarios/p5-lease-retire.sh — S1: a live session lease defers
# holder self-retire (holder serves normally, health surfaces the deferral);
# once the lease releases, the retire drains, the holder exits, and the
# successor replays the journal.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

health_shows_deferred() { holder_health | grep -qF "\"retire_deferred_dir\":\"$MOUNT_DIR\""; }
health_retiring() { holder_health | grep -q '"retiring":true'; }
row_live() { holder_req '{"proto":2,"op":"list","owner":"vmstress"}' | grep -qF "\"dir\":\"$MOUNT_DIR\",\"base\":\"$STATE_DIR/base\",\"live\":true"; }

vm_phase setup
p5_install
p5_reset
guest_workload setup
p5_stop_readers
LH_PID="$(p5_leasehold_start "$MOUNT_DIR" p5session)"
p5_await 15 "session lease held on $MOUNT_DIR" p5_leasehold_held p5session
log "leasehold pid $LH_PID holds $MOUNT_DIR"

vm_phase skew-deferred
p5_skew_on
p5_await 150 "retire deferred on the leased dir (health)" health_shows_deferred
health_retiring && die "holder entered retiring while the lease was held"
guest_read_ok "$MOUNT_DIR/config.json" || die "leased mount stopped serving during the deferral"

# Deferral must not degrade service: a new mount lands and unmounts fine.
TMP_BASE="$VMCTL_GUEST_DIR/p5tmp/base" TMP_MNT="$VMCTL_GUEST_DIR/p5tmp/mnt"
vm_ssh "mkdir -p '$TMP_BASE' '$TMP_MNT' && echo canary >'$TMP_BASE/c.txt'"
resp="$(holder_req "$(plain_mount_json "$TMP_BASE" "$TMP_MNT" p5tmp)" 120s)"
resp_ok "$resp" || die "new mount bounced during lease-deferred skew (must serve normally): $resp"
resp="$(holder_req "$(plain_unmount_json "$TMP_BASE" "$TMP_MNT" p5tmp)" 120s)"
resp_ok "$resp" || die "unmount of the deferral-window mount failed: $resp"

vm_phase release-drain
vm_ssh "kill $LH_PID" || die "could not release the session lease (kill $LH_PID)"
p5_await 150 "holder retired (drained + exited) after lease release" holder_gone
journal_has_dir "$MOUNT_DIR" || die "retire dropped the journaled row for $MOUNT_DIR"

vm_phase successor-replay
p5_skew_off
holder_launch
p5_await 120 "journal replay restored $MOUNT_DIR" row_live
guest_read_ok "$MOUNT_DIR/config.json" || die "replayed mount does not serve reads"

vm_phase teardown
guest_workload stop
p5_reset
log "S1 PASS: lease deferred the retire, release drained it, successor replayed the row"
