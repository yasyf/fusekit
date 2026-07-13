# shellcheck shell=bash
# scripts/vm/scenarios/p5-term-displacement.sh — S3: SIGTERM under live mounts,
# against the v1.0.1 contract (finding F-2, now FIXED): the sweep unmounts
# unleased rows cleanly and drops their journal entries; a LEASED row's journal
# entry is KEPT, its lease stays held, nothing is forced, no orphan/EPERM
# carcass remains (go-nfsv4 exits clean — an in-process-served mount cannot
# outlive its holder process), so its dir is left a healthy BARE dir. The
# successor's replay then MOUNTS that leased bare-dir row: a bare dir has no
# carcass, so the seize is SKIPPED (mirroring handleMount's not-mounted branch)
# and the row goes live UNDER the still-held lease, never forced. A consumer
# OpMount of the identical spec is then an idempotent confirm with working
# reads. (v1.0.0 DEFERRED the leased row until a consumer OpMount healed it;
# F-2 closed that replay-defers/OpMount-heals asymmetry.)
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

B_BASE="$VMCTL_GUEST_DIR/p5b/base" B_MNT="$VMCTL_GUEST_DIR/p5b/mnt"

row_live() { holder_req '{"proto":2,"op":"list","owner":"vmstress"}' | grep -qF "\"dir\":\"$MOUNT_DIR\",\"base\":\"$STATE_DIR/base\",\"live\":true"; }

vm_phase setup
p5_install
p5_reset
guest_workload setup
p5_stop_readers
LH_PID="$(p5_leasehold_start "$MOUNT_DIR" p5session)"
p5_await 15 "session lease held on $MOUNT_DIR" p5_leasehold_held p5session

vm_ssh "mkdir -p '$B_BASE' '$B_MNT' && echo canary >'$B_BASE/c.txt'"
resp="$(holder_req "$(plain_mount_json "$B_BASE" "$B_MNT" p5b)" 120s)"
resp_ok "$resp" || die "unleased mount B failed to come up: $resp"
mounted "$B_MNT" || die "mount B not in the kernel table"

vm_phase term-sweep
vm_ssh "pkill -TERM -x fusekit-holder"
p5_await 90 "holder exited after SIGTERM" holder_gone

# Unleased row: unmounted clean, journal entry dropped.
if mounted "$B_MNT"; then die "unleased quiescent mount B survived the graceful sweep"; fi
if journal_has_dir "$B_MNT"; then die "journal kept the cleanly-unmounted row for B"; fi
# Leased row: journal entry kept, lease still held, no orphan/EPERM carcass —
# the mount follows the holder process down, so the dir must be a healthy
# bare dir (answering stats), never a hung or dead-errno mountpoint.
journal_has_dir "$MOUNT_DIR" || die "journal lost the leased mount's row on TERM"
proc_alive "$LH_PID" || die "leasehold died — lease not held across the displacement"
if mounted "$MOUNT_DIR"; then die "leased dir is still a mountpoint after holder death — investigate what is serving it"; fi
[[ "$(guest_stat "$MOUNT_DIR" 3s)" == "ok" ]] || die "leased dir is not a healthy bare dir (hung or dead-errno carcass left behind)"
if [[ -n "$(vm_ssh 'pgrep -x go-nfsv4' 2>/dev/null || true)" ]]; then die "go-nfsv4 orphan survived the graceful TERM sweep"; fi

vm_phase successor-remounts
holder_launch
replay_done() { holder_health | grep -q '"replay_done":true'; }
p5_await 60 "successor finished its replay" replay_done
# The leased dir is bare (its mount followed the holder down), so replay skips
# the seize and mounts the row live UNDER the held lease. A force would have
# seized the lease and killed the leasehold, so LH_PID staying alive plus
# leases_held:1 is the no-force proof.
p5_await 30 "successor remounted the leased bare-dir row live" row_live
journal_has_dir "$MOUNT_DIR" || die "successor dropped the leased row it remounted"
proc_alive "$LH_PID" || die "leasehold died — replay must remount UNDER the held lease, never seize/force over it"
holder_health | grep -q '"leases_held":1' || die "health does not report the still-held lease after the seize-skip remount"

vm_phase consumer-confirm
# Replay already brought the row live, so a consumer OpMount of the identical
# spec is now an idempotent OK (v1.0.0 needed this call to first mount it).
resp="$(holder_req "$(vmstress_mount_json)" 150s)"
resp_ok "$resp" || die "idempotent consumer OpMount over the shared lease failed: $resp"
p5_await 30 "row still live after the idempotent consumer OpMount" row_live
guest_read_ok "$MOUNT_DIR/config.json" 15s || die "replayed mount does not serve reads"

vm_phase teardown
vm_ssh "kill $LH_PID 2>/dev/null; true" || true
guest_workload stop
p5_reset
log "S3 PASS (v1.0.1 F-2 contract): unleased swept clean, leased row kept + lease held + no carcass, replay REMOUNTED the bare-dir row live under the held lease (seize skipped, no force), consumer OpMount idempotent"
