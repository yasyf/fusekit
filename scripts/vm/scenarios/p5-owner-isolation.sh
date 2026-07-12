# shellcheck shell=bash
# scripts/vm/scenarios/p5-owner-isolation.sh — S9: owners are scoped — list
# shows only your rows (all:true is the read-only cross-tenant view), a
# cross-owner unmount refuses with owner-mismatch even with all:true, reclaim
# sweeps only the requesting owner, and leases are owner-scoped diagnostics.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

A_BASE="$VMCTL_GUEST_DIR/alice/base" A_MNT="$VMCTL_GUEST_DIR/alice/mnt"
B_BASE="$VMCTL_GUEST_DIR/bob/base" B_MNT="$VMCTL_GUEST_DIR/bob/mnt"

vm_phase setup
p5_install
p5_reset
vm_ssh "mkdir -p '$A_BASE' '$A_MNT' '$B_BASE' '$B_MNT' && echo a >'$A_BASE/a.txt' && echo b >'$B_BASE/b.txt'"
holder_launch
resp="$(holder_req "$(plain_mount_json "$A_BASE" "$A_MNT" alice)" 120s)"
resp_ok "$resp" || die "alice mount failed: $resp"
resp="$(holder_req "$(plain_mount_json "$B_BASE" "$B_MNT" bob)" 120s)"
resp_ok "$resp" || die "bob mount failed: $resp"

vm_phase list-scoping
alist="$(holder_req '{"proto":2,"op":"list","owner":"alice"}')"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$alist" || die "alice's list misses her own row: $alist"
grep -qF "\"dir\":\"$B_MNT\"" <<<"$alist" && die "alice's list leaks bob's row: $alist"
blist="$(holder_req '{"proto":2,"op":"list","owner":"bob"}')"
grep -qF "\"dir\":\"$B_MNT\"" <<<"$blist" || die "bob's list misses his own row: $blist"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$blist" && die "bob's list leaks alice's row: $blist"
all_list="$(holder_req '{"proto":2,"op":"list","owner":"alice","all":true}')"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$all_list" && grep -qF "\"dir\":\"$B_MNT\"" <<<"$all_list" ||
  die "all:true cross-tenant view is incomplete: $all_list"

vm_phase cross-owner-refusal
resp="$(holder_req "$(plain_unmount_json "$B_BASE" "$B_MNT" alice)" 120s)"
resp_ok "$resp" && die "alice unmounted bob's row"
[[ "$(resp_class "$resp")" == "owner-mismatch" ]] || die "expected owner-mismatch, got: $resp"
resp="$(holder_req "{\"proto\":2,\"op\":\"unmount\",\"base\":\"$B_BASE\",\"dir\":\"$B_MNT\",\"owner\":\"alice\",\"all\":true}" 120s)"
resp_ok "$resp" && die "all:true let alice unmount bob's row (must stay read-only)"
mounted "$B_MNT" || die "bob's mount went away after refused cross-owner unmounts"

vm_phase lease-scoping
LH_PID="$(p5_leasehold_start "$A_MNT" alice)"
p5_await 15 "alice's diagnostic lease held" p5_leasehold_held alice
aleases="$(holder_req '{"proto":2,"op":"leases","owner":"alice"}')"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$aleases" || die "alice's leases view misses her lease: $aleases"
bleases="$(holder_req '{"proto":2,"op":"leases","owner":"bob"}')"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$bleases" && die "bob's leases view leaks alice's lease: $bleases"
all_leases="$(holder_req '{"proto":2,"op":"leases","owner":"bob","all":true}')"
grep -qF "\"dir\":\"$A_MNT\"" <<<"$all_leases" || die "all:true leases view misses alice's lease: $all_leases"
vm_ssh "kill $LH_PID"

vm_phase reclaim-scoping
resp="$(holder_req '{"proto":2,"op":"reclaim","owner":"alice"}' 180s)"
resp_ok "$resp" || die "alice's reclaim failed: $resp"
if mounted "$A_MNT"; then die "reclaim left alice's own row mounted"; fi
mounted "$B_MNT" || die "alice's reclaim tore down bob's row"
guest_read_ok "$B_MNT/b.txt" || die "bob's mount stopped serving after alice's reclaim"
resp="$(holder_req '{"proto":2,"op":"reclaim","owner":"bob"}' 180s)"
resp_ok "$resp" || die "bob's reclaim failed: $resp"

vm_phase teardown
p5_reset
log "S9 PASS: owner scoping held for list/unmount/reclaim/leases; all:true stayed read-only"
