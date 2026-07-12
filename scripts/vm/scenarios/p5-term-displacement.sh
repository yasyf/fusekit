# shellcheck shell=bash
# scripts/vm/scenarios/p5-term-displacement.sh — S3: SIGTERM under live mounts,
# against the ADJUDICATED contract (finding F-2 in the P5 RESULTS): the sweep
# unmounts unleased rows cleanly and drops their journal entries; a LEASED
# row's journal entry is KEPT, its lease stays held, nothing is forced, no
# orphan/EPERM carcass remains (go-nfsv4 exits clean — an in-process-served
# mount cannot outlive its holder process); the successor's replay DEFERS the
# leased row (never strikes, never forces); and a consumer-style OpMount of
# the identical spec heals it over the shared lease with working reads. The
# replay-defers/OpMount-heals asymmetry is tracked as open finding F-2
# (v1.0.1 improvement, not a migration blocker).
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

vm_phase successor-defers
holder_launch
replay_done() { holder_health | grep -q '"replay_done":true'; }
p5_await 60 "successor finished its replay" replay_done
journal_has_dir "$MOUNT_DIR" || die "successor's replay struck the leased row"
if row_live; then die "successor replayed the leased row live — replay seized over a held lease?"; fi
holder_health | grep -q '"leases_held":1' || die "health does not report the held lease"

vm_phase consumer-heal
resp="$(holder_req "$(vmstress_mount_json)" 150s)"
resp_ok "$resp" || die "consumer-style OpMount over the shared lease failed: $resp"
p5_await 30 "healed row live" row_live
guest_read_ok "$MOUNT_DIR/config.json" 15s || die "healed mount does not serve reads"

vm_phase teardown
vm_ssh "kill $LH_PID 2>/dev/null; true" || true
guest_workload stop
p5_reset
log "S3 PASS (adjudicated contract): unleased swept clean, leased row kept + lease held + no carcass, replay deferred, consumer OpMount healed"
