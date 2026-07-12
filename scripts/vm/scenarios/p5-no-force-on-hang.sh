# shellcheck shell=bash
# scripts/vm/scenarios/p5-no-force-on-hang.sh — S2: a wedged-but-LIVE mount
# (go-nfsv4 SIGSTOPped, uncached lookups hang) under a HELD SESSION LEASE is
# never torn down: OpMount's remount flow bounces busy, a retire tick defers,
# and — after the holder is killed and the wedge becomes a genuine orphan
# carcass — the successor's replay defers on the lease, never forces, never
# kills the server. Once the lease releases, carcass proof v2 recovers the
# dir end-to-end. The regression the holder-v2 design exists to prevent.
#
# The UNLEASED arm was exercised by attempt 3 (evidence 20260712-122302):
# OpMount answered ClassWedged after a bounded graceful unmount parked
# ("outcome unknown"), the stopped server was never killed, and the mount
# left the table via the parked GRACEFUL unmount only — no force. Unleased +
# unresponsive means the holder may gracefully recycle; leased means nothing
# moves, which is what this scenario asserts.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

assert_wedge_intact() {
  local where="$1"
  mounted "$MOUNT_DIR" || die "$where: the leased wedged mount left the kernel table"
  proc_alive "$SRV" || die "$where: the SIGSTOPped go-nfsv4 server (pid $SRV) was killed"
}

vm_phase setup
p5_install
p5_reset
guest_workload setup
p5_stop_readers
LH_PID="$(p5_leasehold_start "$MOUNT_DIR" p5wedge)"
p5_await 15 "session lease held on $MOUNT_DIR" p5_leasehold_held p5wedge
SRV="$(vm_ssh 'pgrep -x go-nfsv4 | head -n 1')"
[[ -n "$SRV" ]] || die "no go-nfsv4 server after setup"
vm_ssh "kill -STOP $SRV"
# Probe a NEVER-looked-up name: a cached path answers from the client's
# attribute cache without a server round trip; a fresh name forces a LOOKUP.
[[ "$(guest_stat "$MOUNT_DIR/wedge-probe-fresh-$$" 3s)" == "hung" ]] || die "precondition: fresh-name lookup did not hang with the server stopped"
log "wedge armed: server $SRV stopped, lease held by pid $LH_PID, uncached lookups hang"

vm_phase mount-path
resp="$(holder_req "$(vmstress_mount_json)" 150s)" || resp="(no response within 150s)"
log "OpMount on the leased wedged dir answered: $resp"
if resp_ok "$resp"; then die "OpMount claimed success on a leased wedged-but-live mount"; fi
assert_wedge_intact "mount-path"

vm_phase retire-path
p5_skew_on
retire_deferred() { holder_health | grep -qF "\"retire_deferred_dir\":\"$MOUNT_DIR\""; }
p5_await 150 "retire deferred on the leased wedged dir" retire_deferred
[[ -n "$(holder_pid)" ]] || die "retire-path: holder exited under a held lease"
assert_wedge_intact "retire-path"
p5_skew_off
journal_has_dir "$MOUNT_DIR" || die "retire-path: journal row dropped"

# The wedge phases must not have damaged the live mount: with the ORIGINAL
# holder still serving, SIGCONT restores full service (a fresh-name lookup
# answers ENOENT — a genuine round trip through holderfs — and the synth
# entry reads back).
vm_phase live-unwedge-proof
vm_ssh "kill -CONT $SRV"
live_roundtrip() { [[ "$(guest_stat "$MOUNT_DIR/live-probe-$$-$RANDOM" 5s)" == *"no such file"* ]]; }
p5_await 90 "fresh-name lookup answers ENOENT after SIGCONT" live_roundtrip
guest_read_ok "$MOUNT_DIR/config.json" 15s || die "live-unwedge-proof: synth read failed after SIGCONT"

vm_phase replay-path
vm_ssh "kill -STOP $SRV"
[[ "$(guest_stat "$MOUNT_DIR/rearm-probe-$$" 3s)" == "hung" ]] || die "replay-path: wedge did not re-arm"
vm_ssh "pkill -9 -x fusekit-holder"
p5_await 30 "holder killed" holder_gone
assert_wedge_intact "post-kill"
holder_launch
replay_done() { holder_health | grep -q '"replay_done":true'; }
p5_await 120 "successor finished (deferring) its replay" replay_done
assert_wedge_intact "replay-path"
journal_has_dir "$MOUNT_DIR" || die "replay-path: successor struck the leased wedged row from the journal"
log "successor health: $(holder_health 30s || true)"

# The deferral must be STABLE: with the wedge still armed (server stopped —
# a SIGCONT with a dead parent hands control to the orphan's own self-clean,
# out of any holder's control), nothing may move for a full window.
vm_phase deferral-stability
wedge_moved() { ! mounted "$MOUNT_DIR" || ! proc_alive "$SRV" || ! journal_has_dir "$MOUNT_DIR"; }
p5_never 30 "successor moved the leased wedge (unmount, server kill, or row drop)" wedge_moved

# Release the lease and let the orphan run: it self-cleans (or the next
# generation's carcass proof reaps it), and the following holder generation
# must remount through the still-live vmstress bridge.
vm_phase post-lease-recovery
vm_ssh "kill $LH_PID 2>/dev/null; true" || true
vm_ssh "kill -CONT $SRV 2>/dev/null; true" || true
vm_ssh "pkill -9 -x fusekit-holder"
p5_await 30 "holder killed for the recovery generation" holder_gone
holder_launch
row_live() { holder_req '{"proto":2,"op":"list","owner":"vmstress"}' | grep -qF "\"dir\":\"$MOUNT_DIR\",\"base\":\"$STATE_DIR/base\",\"live\":true"; }
p5_await 120 "row remounted after lease release (carcass cleared or self-cleaned)" row_live
guest_read_ok "$MOUNT_DIR/config.json" 15s || die "post-lease-recovery: remounted row does not serve"
orphan_gone() { ! proc_alive "$SRV"; }
p5_await 60 "gen-1 server gone (self-cleaned or reaped)" orphan_gone

vm_phase teardown
guest_workload stop
p5_reset
log "S2 PASS: leased wedge deferred on every path, mount never torn while leased, carcass proof v2 recovered after release"
