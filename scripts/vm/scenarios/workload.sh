#!/usr/bin/env bash
# scripts/vm/scenarios/workload.sh — shared GUEST-side workload functions.
#
# Host scenarios copy this file into the guest (vm_scp_to) and invoke one
# command per vm_ssh call: `bash ~/fusekit-vm/workload.sh <command> [args]`.
# It owns the guest process management around vmstress — the detached serve
# process, the self-restarting mmap readers, the AppleDouble release gate,
# and stop — so scenarios stay one-liner-per-step on the host.
#
# Layout (all under ~/fusekit-vm, installed by vmctl push):
#   bin/vmstress   the Go guest driver
#   stress/        serve state (base/, private/, consumer/, bridge.sock)
#   mnt/           the holder-served mountpoint
#   run/           pidfiles and logs for the detached processes
[[ "$(sysctl -n kern.hv_vmm_present 2>/dev/null)" == "1" ]] || {
  echo "workload.sh: REFUSING TO RUN: not a VM (kern.hv_vmm_present != 1); deliberate panic workloads never run on bare metal" >&2
  exit 86
}
set -euo pipefail

GUEST_DIR="$HOME/fusekit-vm"
VMSTRESS="$GUEST_DIR/bin/vmstress"
STATE_DIR="$GUEST_DIR/stress"
MOUNT_DIR="$GUEST_DIR/mnt"
RUN_DIR="$GUEST_DIR/run"
SERVE_PID="$RUN_DIR/serve.pid"
SERVE_LOG="$RUN_DIR/serve.log"

# wlog/wdie write timestamped lines; vm_ssh forwards them into scenario.log.
wlog() { printf '%s workload: %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
wdie() {
  printf '%s workload: FATAL: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2
  exit 1
}

# pid_alive reports whether the pidfile names a live process.
pid_alive() {
  local f="$1" pid
  [[ -f "$f" ]] || return 1
  pid="$(cat "$f")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# cmd_setup (re)starts the stack: stops any previous run, starts a detached
# `vmstress serve` (fresh state, holder mount), waits for the mount to come
# live, then starts the self-restarting mmap readers over the synth entry and
# the big passthrough file.
cmd_setup() {
  [[ -x "$VMSTRESS" ]] || wdie "vmstress not installed at $VMSTRESS — run vmctl push"
  cmd_stop
  mkdir -p "$RUN_DIR"
  : >"$SERVE_LOG"
  wlog "starting vmstress serve (state $STATE_DIR, mount $MOUNT_DIR)"
  nohup "$VMSTRESS" serve --state "$STATE_DIR" --dir "$MOUNT_DIR" >>"$SERVE_LOG" 2>&1 &
  echo "$!" >"$SERVE_PID"
  local t0
  t0="$(date +%s)"
  until grep -q "serving $MOUNT_DIR" "$SERVE_LOG" 2>/dev/null; do
    pid_alive "$SERVE_PID" || {
      cat "$SERVE_LOG" >&2
      wdie "vmstress serve died during startup"
    }
    if (($(date +%s) - t0 >= 120)); then
      cat "$SERVE_LOG" >&2
      wdie "mount not live within 120s"
    fi
    sleep 2
  done
  stat -f%z "$MOUNT_DIR/config.json" >/dev/null || wdie "mount live but config.json unreadable"
  start_reader config "$MOUNT_DIR/config.json"
  start_reader mmap "$MOUNT_DIR/mmap.dat"
  wlog "setup complete: serve pid $(cat "$SERVE_PID"), mmap readers up"
}

# start_reader launches a self-restarting mmap reader over one file. A shrink
# under the mapping can SIGBUS a pass; the loop restarts it, and the loop's
# pidfile lets stop reclaim it.
start_reader() {
  local tag="$1" file="$2"
  local pidf="$RUN_DIR/reader-$tag.pid" logf="$RUN_DIR/reader-$tag.log"
  : >"$logf"
  nohup bash -c "while :; do \"$VMSTRESS\" read --mmap --file \"$file\" --seconds 60 || true; sleep 1; done" >>"$logf" 2>&1 &
  echo "$!" >"$pidf"
  wlog "mmap reader '$tag' up on $file"
}

# cmd_churn drives one bounded claude-shaped churn burst through the mount.
cmd_churn() {
  local seconds="${1:?usage: workload.sh churn SECONDS}"
  pid_alive "$SERVE_PID" || wdie "vmstress serve is not running — run setup first"
  "$VMSTRESS" churn --dir "$MOUNT_DIR" --seconds "$seconds" --readers 4
}

# assert_ad_blocked fails the workload unless creating the given `._` path is
# blocked: the touch must fail AND the file must never appear. macFUSE returns
# EACCES; over NFS a blocked create also surfaces as ENOENT when the client's
# negative-lookup cache short-circuits before reaching the Create vnop under
# concurrent load (verified 40/40 EACCES serially; ENOENT only races in under
# churn). Both mean "sidecar not created" — the litter-prevention contract.
assert_ad_blocked() {
  local path="$1" err
  if err="$(touch "$path" 2>&1)"; then
    rm -f "$path"
    wdie "._ create unexpectedly SUCCEEDED ($path) — AppleDouble blocking is not active on this build"
  fi
  if [[ -e "$path" || -L "$path" ]]; then
    wdie "._ create failed but the file exists ($path) — blocked-then-created leak"
  fi
  case "$err" in
  *"Permission denied"* | *"No such file or directory"*) : ;;
  *) wdie "._ create failed with an unexpected error (want EACCES or ENOENT): $err" ;;
  esac
}

# cmd_appledouble_check is the mitigation release gate: AppleDouble `._`
# creation through the mount must be blocked (EACCES, or ENOENT under the NFS
# negative-lookup race — see assert_ad_blocked), pre-existing backing-store
# litter must be invisible, and ordinary creates/writes must keep succeeding
# alongside. It FAILS on an unmitigated holder by design — only
# validate-mitigation.sh calls it.
cmd_appledouble_check() {
  local out="$MOUNT_DIR/ad-ordinary.txt" err name

  # Ordinary create/write/read/delete must succeed.
  echo "ordinary write" >"$out" || wdie "ordinary create failed"
  [[ "$(cat "$out")" == "ordinary write" ]] || wdie "ordinary read-back mismatch"
  rm "$out" || wdie "ordinary delete failed"

  # `._` creates must be blocked, top-level and nested (never created).
  assert_ad_blocked "$MOUNT_DIR/._ad-blocked"
  mkdir -p "$MOUNT_DIR/ad-nest" || wdie "nested dir create failed"
  assert_ad_blocked "$MOUNT_DIR/ad-nest/._ad-blocked"
  rmdir "$MOUNT_DIR/ad-nest"

  # Pre-existing backing litter must be invisible: ENOENT on lookup, hidden
  # from readdir.
  touch "$STATE_DIR/base/._ad-litter"
  if err="$(ls "$MOUNT_DIR/._ad-litter" 2>&1)"; then
    wdie "._ backing litter is visible through the mount"
  fi
  [[ "$err" == *"No such file or directory"* ]] || wdie "._ lookup failed with the wrong error (want ENOENT): $err"
  local hidden
  for hidden in "$MOUNT_DIR"/._*; do
    if [[ -e "$hidden" || -L "$hidden" ]]; then
      wdie "readdir lists ._ entries: $hidden"
    fi
  done
  rm -f "$STATE_DIR/base/._ad-litter"

  # Negative space: non-AppleDouble dot names must not be caught.
  for name in .ad-dotfile ..ad-dotdot x._ad-middle; do
    echo ok >"$MOUNT_DIR/$name" || wdie "non-AppleDouble name $name was blocked"
    rm "$MOUNT_DIR/$name"
  done
  wlog "appledouble-check passed: ._ blocked (EACCES/ENOENT), litter hidden, ordinary ops fine"
}

# cmd_force_unmount is the phase-2 aggravation: forcibly unmount the live
# mount out from under the held mmaps (passwordless sudo, established by
# provision).
cmd_force_unmount() {
  mount | grep -qF " on $MOUNT_DIR (" || wdie "nothing mounted at $MOUNT_DIR"
  wlog "FORCED UNMOUNT under active mmap: sudo umount -f $MOUNT_DIR"
  sudo -n umount -f "$MOUNT_DIR"
}

# cmd_status prints serve/reader/mount state into the scenario log.
cmd_status() {
  if pid_alive "$SERVE_PID"; then
    wlog "serve: running (pid $(cat "$SERVE_PID"))"
  else
    wlog "serve: stopped"
  fi
  mount | grep -F " on $MOUNT_DIR (" || wlog "no mount at $MOUNT_DIR"
  tail -n 20 "$SERVE_LOG" 2>/dev/null || true
}

# cmd_stop reclaims everything setup started: reader wrappers first (they
# respawn otherwise), lingering vmstress workers, then a graceful serve TERM
# so the holder tears the mount down.
cmd_stop() {
  local f pid t0
  for f in "$RUN_DIR"/reader-*.pid; do
    [[ -f "$f" ]] || continue
    pid="$(cat "$f")"
    kill "$pid" 2>/dev/null || true
    rm -f "$f"
  done
  pkill -f "$VMSTRESS read" 2>/dev/null || true
  pkill -f "$VMSTRESS churn" 2>/dev/null || true
  if pid_alive "$SERVE_PID"; then
    pid="$(cat "$SERVE_PID")"
    wlog "stopping serve (pid $pid)"
    kill "$pid" 2>/dev/null || true
    t0="$(date +%s)"
    while kill -0 "$pid" 2>/dev/null; do
      if (($(date +%s) - t0 >= 60)); then
        wlog "serve did not exit within 60s; SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
        break
      fi
      sleep 1
    done
  fi
  rm -f "$SERVE_PID"
}

main() {
  local cmd="${1:-}"
  [[ -n "$cmd" ]] || wdie "usage: workload.sh <setup|churn SECONDS|appledouble-check|force-unmount|status|stop>"
  shift
  case "$cmd" in
  setup) cmd_setup "$@" ;;
  churn) cmd_churn "$@" ;;
  appledouble-check) cmd_appledouble_check "$@" ;;
  force-unmount) cmd_force_unmount "$@" ;;
  status) cmd_status "$@" ;;
  stop) cmd_stop "$@" ;;
  *) wdie "unknown command: $cmd" ;;
  esac
}

main "$@"
