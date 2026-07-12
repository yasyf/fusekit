# shellcheck shell=bash
# scripts/vm/scenarios/p5-lease-agent.sh — S6: cc-pool's detached lease agent
# (`ccp lease-agent`, the select/env handout shape) holds the session lease
# while its watched session leader lives — the holder's unmount bounces busy
# with ccp provenance — and releases it when the leader dies, after which the
# unmount proceeds. Requires a pre-staged pure-Go ccp (v0.52.0) at
# $VM_ROOT/stage/ccp.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

CCP="$VMCTL_GUEST_DIR/bin/ccp"

vm_phase setup
p5_install
p5_install_bin ccp
p5_reset
guest_workload setup
p5_stop_readers
vm_ssh "mkdir -p '$VM_GUEST_HOME/.cc-pool/lease-agents'"

vm_phase agent-up
LEADER="$(vm_ssh "nohup sleep 900 >/dev/null 2>&1 & echo \$!")"
[[ -n "$LEADER" ]] || die "could not start the fake session leader"
START="$(vm_ssh "'$P5UTIL' starttime $LEADER")"
[[ "$START" =~ ^[0-9]+$ ]] || die "bad start-time stamp for leader $LEADER: $START"
# fd 3 is the agent's readiness-pipe contract (production wires it via
# ExtraFiles); without it the agent writes-to and CLOSES an arbitrary
# inherited fd 3 and can corrupt its own runtime — finding F-3.
vm_ssh "rm -f '$RUN_DIR/lease-agent.log'; nohup '$CCP' lease-agent --pid $LEADER --start $START --id 7 --dir '$MOUNT_DIR' >>'$RUN_DIR/lease-agent.log' 2>&1 3>/dev/null & echo \$!" >/dev/null

agent_lease_held() {
  holder_req '{"proto":2,"op":"leases","owner":"vmstress","all":true}' |
    grep -qF "\"held\":true,\"dir\":\"$MOUNT_DIR\""
}
p5_await 30 "ccp lease-agent holds the session lease" agent_lease_held

vm_phase bounce-under-lease
resp="$(holder_req "$(vmstress_unmount_json)" 120s)"
log "unmount under the agent's lease answered: $resp"
resp_ok "$resp" && die "unmount SUCCEEDED while the lease agent held the session lease"
[[ "$(resp_class "$resp")" == "busy" ]] || die "expected err_class busy under the agent's lease, got: $resp"
grep -qi 'ccp\|cc-pool' <<<"$resp" || warn "busy provenance does not name ccp: $resp"
mounted "$MOUNT_DIR" || die "bounced unmount tore the mount down"

vm_phase leader-death
vm_ssh "kill $LEADER"
agent_lease_released() { ! agent_lease_held; }
p5_await 60 "agent released the lease after the leader died" agent_lease_released

unmount_after_release() {
  local r
  r="$(holder_req "$(vmstress_unmount_json)" 120s)" || return 1
  resp_ok "$r"
}
p5_await 60 "holder unmount proceeds after the release" unmount_after_release
if mounted "$MOUNT_DIR"; then die "unmount answered OK but the mount is still up"; fi

vm_phase teardown
guest_workload stop
p5_reset
log "S6 PASS: lease agent held while the leader lived, released on leader death, unmount then proceeded"
