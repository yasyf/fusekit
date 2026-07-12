# shellcheck shell=bash
# scripts/vm/scenarios/p5-content-deferral.sh — S7: a journaled cc-notes TREE
# row whose contentd socket is down DEFERS at replay (health ContentDeferred,
# journal row kept — never struck); once contentd comes back the row mounts
# and serves. Requires a pre-staged fuse-build ccn (v0.27.0) at
# $VM_ROOT/stage/ccn.
# shellcheck disable=SC2034
EXPECT=clean

SCENARIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=p5lib.sh
source "$SCENARIO_DIR/p5lib.sh"

CCN="$VMCTL_GUEST_DIR/bin/ccn"
REPO="$VMCTL_GUEST_DIR/notesrepo"
CCN_SOCK="$VM_GUEST_HOME/.fusekit/spool/cc-notes/c.sock"

contentd_up() { vm_ssh "test -S '$CCN_SOCK' && pgrep -f 'bin/ccn contentd' >/dev/null"; }
start_contentd() {
  vm_ssh "mkdir -p '$VM_GUEST_HOME/.fusekit/spool/cc-notes'; nohup '$CCN' contentd >>'$RUN_DIR/contentd.log' 2>&1 & echo \$!" >/dev/null
  p5_await 30 "ccn contentd serving its socket" contentd_up
}

vm_phase setup
p5_install
p5_install_bin ccn
p5_reset
vm_ssh "rm -rf '$REPO' '$VM_GUEST_HOME/.cc-notes' && mkdir -p '$REPO' && cd '$REPO' && git init -q . && git -c user.email=p5@vm -c user.name=p5 commit -q --allow-empty -m init"
start_contentd

vm_phase mount
MOUNT_OUT="$(vm_ssh "cd '$REPO' && '$CCN' mount 2>&1")" || die "ccn mount failed: $MOUNT_OUT"
log "ccn mount: $MOUNT_OUT"
NDIR="$(vm_ssh "readlink '$REPO/.notes'")"
[[ -n "$NDIR" ]] || die "no .notes symlink after ccn mount"
vm_ssh "ls '$NDIR/notes' >/dev/null" || die "tree mount does not serve /notes"
journal_has_dir "$NDIR" || die "tree row not journaled"
vm_ssh "grep -qE '\"content_mode\": ?\"tree\"' '$JOURNAL'" || die "journaled row is not tree-mode"

vm_phase kill-contentd-and-holder
vm_ssh "pkill -f 'bin/ccn contentd'"
sleep 2
vm_ssh "pkill -9 -x fusekit-holder"
p5_await 30 "holder killed" holder_gone

vm_phase deferred-replay
holder_launch
row_deferred() { holder_health | grep -qF "\"content_deferred\":[\"$NDIR"; }
p5_await 90 "replay deferred the tree row on the down socket" row_deferred
holder_health | grep -q '"replay_done":true' || die "replay did not finish while deferring"
# Never struck: the deferral must persist, with the journal row intact.
struck() { ! journal_has_dir "$NDIR" || ! row_deferred; }
p5_never 60 "deferred tree row was struck (journal row dropped or deferral cleared without contentd)" struck

vm_phase contentd-returns
start_contentd
row_live() { holder_req '{"proto":2,"op":"list","owner":"cc-notes"}' | grep -qF "\"dir\":\"$NDIR\",\"base\":\"$REPO\",\"live\":true"; }
p5_await 150 "deferred row mounted once contentd answered" row_live
vm_ssh "ls '$NDIR/notes' >/dev/null" || die "tree mount does not serve /notes after recovery"
deferral_cleared() { ! holder_health | grep -qF "\"content_deferred\":[\"$NDIR"; }
p5_await 60 "ContentDeferred entry cleared" deferral_cleared

vm_phase teardown
vm_ssh "cd '$REPO' && '$CCN' mount --stop '$NDIR'" || warn "ccn mount --stop failed; p5_reset will clear"
p5_reset
log "S7 PASS: tree row deferred on down contentd (never struck), mounted on contentd return"
