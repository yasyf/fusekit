#!/usr/bin/env bash
# scripts/vm/scenarios/mux-workload.sh — GUEST-side workload for the single-mount
# multiplexing gate (validate-mux.sh).
#
# Host scenarios copy this file into the guest (vm_scp_to) and invoke one command
# per vm_ssh call: `bash ~/fusekit-vm/mux-workload.sh <command> [args]`. It owns
# the guest process management around `vmstress mux-*` — the detached mux-serve
# (one native mount, N source-mode tenants), the self-restarting per-tenant mmap
# readers, the mount-table / go-nfsv4 process assertions, the native force-unmount
# aggravation, and stop — so the host scenario stays one-liner-per-step.
#
# Layout (all under ~/fusekit-vm, installed by vmctl push):
#   bin/vmstress    the Go guest driver (mux-* subcommands live in cmd/vmstress/mux.go)
#   mux-stress/     mux instance state (base/, consumer/, private/, carve/, bridge.sock)
#   mux/            the ONE native mountpoint; subtrees mux/acct-NN
#   run/            pidfiles and logs for the detached processes
[[ "$(sysctl -n kern.hv_vmm_present 2>/dev/null)" == "1" ]] || {
  echo "mux-workload.sh: REFUSING TO RUN: not a VM (kern.hv_vmm_present != 1); deliberate panic workloads never run on bare metal" >&2
  exit 86
}
set -euo pipefail

GUEST_DIR="$HOME/fusekit-vm"
VMSTRESS="$GUEST_DIR/bin/vmstress"
MUX_STATE="$GUEST_DIR/mux-stress"
MUX_ROOT="$GUEST_DIR/mux"
RUN_DIR="$GUEST_DIR/run"
MUX_SERVE_PID="$RUN_DIR/mux-serve.pid"
MUX_SERVE_LOG="$RUN_DIR/mux-serve.log"

# MUX_TENANTS is the pool size the one native mount serves; >= 3 is the gate's
# floor. Env-overridable, but the host scenario relies on every command sharing
# this one value, so it must be identical across a whole run.
MUX_TENANTS="${MUX_TENANTS:-3}"
# Entry names must match cmd/vmstress/mux.go (privateSynthName, mmapName).
MUX_SYNTH="settings.json"
MUX_MMAP="mmap.dat"

# wlog/wdie write timestamped lines; vm_ssh forwards them into scenario.log.
wlog() { printf '%s mux-workload: %s\n' "$(date -u '+%H:%M:%S')" "$*"; }
wdie() {
  printf '%s mux-workload: FATAL: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2
  exit 1
}

# pid_alive reports whether the pidfile names a live process.
pid_alive() {
  local f="$1" pid
  [[ -f "$f" ]] || return 1
  pid="$(cat "$f")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# mux_vmstress runs one vmstress mux subcommand with the shared layout flags.
mux_vmstress() {
  local sub="$1"
  shift
  "$VMSTRESS" "$sub" "$@" --state "$MUX_STATE" --muxroot "$MUX_ROOT" --tenants "$MUX_TENANTS"
}

# mux_start_tenant_readers launches ONE tenant's two self-restarting mmap readers
# (its synth and its big passthrough file). mux_start_readers fans this across the
# whole pool; cmd_mux_fileids reuses it to bring the victim's readers back after
# the drill has quiesced them.
#
# Each reader is TWO processes: a wrapper loop (`bash -c while :; ...`) and the
# transient `vmstress read` worker it respawns every ~60s. The wrapper runs as
# its OWN process-group leader (`set -m`; macOS ships no setsid(1)), so the
# pidfile pid doubles as a pgid and stop/drain kill wrapper + in-flight worker
# atomically — a worker must never outlive its wrapper and respawn a fresh mmap
# after a quiesce (the 2026-07-03 fileids-EIO root cause, vm-repro chronology).
mux_start_tenant_readers() {
  local name="$1" file tag pidf logf
  for file in "$MUX_SYNTH" "$MUX_MMAP"; do
    tag="$name-$file"
    pidf="$RUN_DIR/mux-reader-$tag.pid"
    logf="$RUN_DIR/mux-reader-$tag.log"
    : >"$logf"
    (
      set -m
      nohup bash -c "while :; do \"$VMSTRESS\" read --mmap --file \"$MUX_ROOT/$name/$file\" --seconds 60 || true; sleep 1; done" >>"$logf" 2>&1 &
      echo "$!" >"$pidf"
    )
  done
}

# mux_start_readers launches a self-restarting mmap reader over each tenant's
# synth and big passthrough file — the held-page half of the nfs_vinvalbuf2
# surface, now across the pool on one native mount. A shrink or a logical detach
# under the mapping can SIGBUS/ENOENT a pass; the loop restarts it, and its
# pidfile lets stop/quiesce reclaim it.
mux_start_readers() {
  local i name
  for ((i = 1; i <= MUX_TENANTS; i++)); do
    name="$(printf 'acct-%02d' "$i")"
    mux_start_tenant_readers "$name"
  done
  wlog "mux mmap readers up: 2 per tenant across $MUX_TENANTS tenants"
}

# Reader argv patterns (pgrep/pkill -f, ERE), optionally scoped to one subtree
# by a "$MUX_ROOT/<name>/" prefix argument. The wrapper's argv QUOTES the
# vmstress path (`... do "$VMSTRESS" read ...`), so a worker pattern can never
# match a wrapper: sweeping only `$VMSTRESS read` kills the transient workers
# while the wrapper loops live on and respawn a fresh 60s mmap worker ~1s
# later. Exactly that orphan leak — prior-run wrappers invisible to the
# fixed-path pidfiles AND to the worker pattern — un-quiesced the fileids
# drill's victim in the 2026-07-03 EIO incident (vm-repro chronology), so the
# two shapes each get their own pattern and every sweep applies both.
mux_reader_worker_pat() { printf '%s read --mmap --file %s' "$VMSTRESS" "${1:-}"; }
mux_reader_wrapper_pat() { printf 'do "%s" read --mmap --file "%s' "$VMSTRESS" "${1:-}"; }

# mux_reap_readers kills every reader wrapper AND worker whose target file
# matches the optional subtree prefix (all readers when absent), then WAITS for
# them to actually exit — not just be signalled — escalating to SIGKILL. A
# survivor is FATAL: readers hold live mmaps, and a mapping that survives into
# the caller's next action (a victim detach, a native force-unmount) is the
# client-vnode-wedge / nfs_vinvalbuf2 surface these drills exist to prove
# absent, so proceeding would invalidate the run.
mux_reap_readers() {
  local prefix="${1:-}" wpat rpat t0
  wpat="$(mux_reader_wrapper_pat "$prefix")"
  rpat="$(mux_reader_worker_pat "$prefix")"
  pkill -f "$wpat" 2>/dev/null || true
  pkill -f "$rpat" 2>/dev/null || true
  t0="$(date +%s)"
  while pgrep -f "$wpat" >/dev/null 2>&1 || pgrep -f "$rpat" >/dev/null 2>&1; do
    if (($(date +%s) - t0 >= 10)); then
      pkill -9 -f "$wpat" 2>/dev/null || true
      pkill -9 -f "$rpat" 2>/dev/null || true
      sleep 1
      break
    fi
    sleep 1
  done
  if pgrep -f "$wpat" >/dev/null 2>&1 || pgrep -f "$rpat" >/dev/null 2>&1; then
    { pgrep -fl "$wpat" || true; pgrep -fl "$rpat" || true; } >&2
    wdie "reader(s) survived SIGKILL (scope: ${prefix:-ALL}) — quiesce did not hold; refusing to continue with live mappings"
  fi
}

# mux_stop_readers reclaims ALL reader wrappers (they respawn otherwise) and
# workers, and WAITS for them to actually exit — not just be signalled — so no
# mapped page survives into a force-unmount. The native root is only ever
# force-unmounted with no mapped pages held (the pool-idle gate the production
# design force-unmounts behind).
mux_stop_readers() {
  local f pid
  for f in "$RUN_DIR"/mux-reader-*.pid; do
    [[ -f "$f" ]] || continue
    pid="$(cat "$f")"
    # The wrapper leads its own process group (mux_start_tenant_readers), so a
    # group kill takes the in-flight worker down with it. The plain-kill
    # fallback covers a pre-group-leader pidfile left by an older script.
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    rm -f "$f"
  done
  # Pattern sweep on top of the pidfiles: pidfile paths are fixed per tag and
  # OVERWRITTEN by every run, so readers orphaned by a prior run are invisible
  # to the loop above.
  mux_reap_readers
}

# mux_victim_name is the fileids drill's victim tenant. cmd/vmstress/mux.go's
# cmdMuxFileids picks l.tenants[len-1] — the LAST tenant — as the victim, and the
# tenants are acct-01..acct-<MUX_TENANTS>, so the victim is acct-<MUX_TENANTS>.
# Keep this derivation in lockstep with that Go selection.
mux_victim_name() { printf 'acct-%02d' "$MUX_TENANTS"; }

# mux_drain_tenant_readers stops ONE tenant's readers — the per-tenant analogue of
# mux_stop_readers, scoped by the tenant's subtree path so every OTHER tenant's
# readers keep running. It kills the tenant's reader process groups by pidfile,
# then pattern-reaps that tenant's wrappers and workers (prior-run orphans
# included) and WAITS for them to actually exit (escalating to SIGKILL, fatal on
# a survivor) so no mapped page survives into the caller's next action.
mux_drain_tenant_readers() {
  local name="${1:?usage: mux_drain_tenant_readers TENANT}" f pid
  for f in "$RUN_DIR/mux-reader-$name-"*.pid; do
    [[ -f "$f" ]] || continue
    pid="$(cat "$f")"
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    rm -f "$f"
  done
  # Subtree paths are disjoint by acct-NN, so the prefix scopes the sweep to
  # THIS tenant's wrapper + worker pairs and no other tenant's.
  mux_reap_readers "$MUX_ROOT/$name/"
}

# cmd_mux_setup (re)starts the stack: stops any previous run, starts a detached
# `vmstress mux-serve` (one native mount, MUX_TENANTS source-mode tenants), waits
# for its steady-state marker, then starts the per-tenant mmap readers.
cmd_mux_setup() {
  [[ -x "$VMSTRESS" ]] || wdie "vmstress not installed at $VMSTRESS — run vmctl push"
  cmd_mux_stop
  # Fresh-run guarantee: after the graceful stop, NOTHING vmstress-shaped may
  # survive into the new stack — not stale serves, reader wrappers (their argv
  # carries the vmstress path, so this pattern reaps them too), or drill
  # workers orphaned by dead ssh sessions. Accumulated cruft of exactly this
  # kind across ~56 relaunches is what un-quiesced the fileids drill's victim
  # in the 2026-07-03 EIO incident (vm-repro chronology).
  if pgrep -f "$VMSTRESS" >/dev/null 2>&1; then
    wlog "sweeping stale vmstress-shaped process(es): $(pgrep -fl "$VMSTRESS" | tr '\n' ';')"
    pkill -9 -f "$VMSTRESS" 2>/dev/null || true
    sleep 1
    if pgrep -f "$VMSTRESS" >/dev/null 2>&1; then
      pgrep -fl "$VMSTRESS" >&2 || true
      wdie "stale vmstress process(es) survived SIGKILL — guest needs a reboot before a valid run"
    fi
  fi
  mkdir -p "$RUN_DIR"
  : >"$MUX_SERVE_LOG"
  wlog "starting vmstress mux-serve ($MUX_TENANTS tenants, state $MUX_STATE, root $MUX_ROOT)"
  nohup "$VMSTRESS" mux-serve --state "$MUX_STATE" --muxroot "$MUX_ROOT" --tenants "$MUX_TENANTS" >>"$MUX_SERVE_LOG" 2>&1 &
  echo "$!" >"$MUX_SERVE_PID"
  local t0
  t0="$(date +%s)"
  until grep -q "steady-state ready" "$MUX_SERVE_LOG" 2>/dev/null; do
    pid_alive "$MUX_SERVE_PID" || {
      cat "$MUX_SERVE_LOG" >&2
      wdie "vmstress mux-serve died during startup"
    }
    if (($(date +%s) - t0 >= 120)); then
      cat "$MUX_SERVE_LOG" >&2
      wdie "mux root not live within 120s"
    fi
    sleep 2
  done
  mux_start_readers
  wlog "mux setup complete: serve pid $(cat "$MUX_SERVE_PID"), $MUX_TENANTS tenants, readers up"
}

# cmd_mux_onemount asserts the mux's headline invariant: exactly ONE native mount
# (at the mux root, with no subtree carrying its own kernel mount) and exactly ONE
# go-nfsv4 process serving the whole pool. The process count retries briefly: a
# just-force-unmounted native root's orphaned server takes a moment to be reaped.
cmd_mux_onemount() {
  local roots subs nfs t0
  roots="$(mount | grep -F " on $MUX_ROOT (" || true)"
  [[ -n "$roots" ]] || {
    mount >&2
    wdie "no native mount at $MUX_ROOT"
  }
  if [[ "$(printf '%s\n' "$roots" | grep -c .)" != "1" ]]; then
    printf '%s\n' "$roots" >&2
    wdie "more than one native mount at $MUX_ROOT"
  fi
  subs="$(mount | grep -F " on $MUX_ROOT/" || true)"
  if [[ -n "$subs" ]]; then
    printf '%s\n' "$subs" >&2
    wdie "a subtree has its OWN kernel mount (want logical subtrees of the one native mount)"
  fi
  t0="$(date +%s)"
  while :; do
    # `|| true`: pgrep exits 1 with no match, which pipefail+set -e would turn
    # into a script abort before the count is even compared.
    nfs="$(pgrep -x go-nfsv4 | grep -c . || true)"
    [[ "$nfs" == "1" ]] && break
    if (($(date +%s) - t0 >= 20)); then
      pgrep -lx go-nfsv4 >&2 || true
      wdie "go-nfsv4 process count = $nfs, want exactly 1 for the whole pool (waited 20s for reaping)"
    fi
    sleep 2
  done
  wlog "mux-onemount OK: 1 native mount at $MUX_ROOT, 0 subtree mounts, 1 go-nfsv4 (pid $(pgrep -x go-nfsv4 | tr '\n' ' '))"
}

# cmd_mux_isolation asserts per-tenant isolation across all attached tenants:
# distinct synth bytes, distinct slot-remapped fileids, and per-tenant carve-outs.
cmd_mux_isolation() {
  pid_alive "$MUX_SERVE_PID" || wdie "mux-serve is not running — run mux-setup first"
  mux_vmstress mux-isolation
}

# cmd_mux_churn drives claude-shaped xattr/rename/stat churn across ALL tenants at
# once for SECONDS — the reproducer shape over one native mount.
cmd_mux_churn() {
  local seconds="${1:?usage: mux-workload.sh mux-churn SECONDS}"
  pid_alive "$MUX_SERVE_PID" || wdie "mux-serve is not running — run mux-setup first"
  mux_vmstress mux-churn --seconds "$seconds"
}

# cmd_mux_fileids runs the fileid identity + re-attach coherence drill:
# detach/re-attach one tenant under load; no fileid ever aliases two objects, the
# quiescent sibling's fileid holds across each detach, and each re-attach serves
# the victim's new authoritative content (go-nfsv4 is path-keyed, so the victim
# reclaiming its old fileid is expected — see cmdMuxFileids in
# cmd/vmstress/mux.go for why fileid freshness is structurally unassertable).
#
# Contract: production's per-account live-session gate quiesces a tenant before it
# is detached, so no live mapping straddles the detach; the drill mirrors that by
# draining ONLY the victim's mmap readers here (the OTHER tenants stay under load —
# cross-tenant mmap survival across a detach is still covered by assertion d). A
# victim reader left mapped across the drill's 20s detach/re-attach cycles would
# carry a stateid for the reclaimed fileid into the new incarnation and can wedge
# the vnode for the mapping's 60s life.
#
# Not hypothetical: the 2026-07-03 cycle-5 EIO (vm-repro chronology) was exactly
# this contract violated by the harness itself — reader wrappers orphaned by
# prior runs survived the drain (pidfiles overwritten; the worker-only pkill
# pattern misses the quoted wrapper argv), respawned a 60s mmap on the victim
# ~1s after the "quiesce", and the macOS NFS client wedged that one vnode
# (path-keyed fileid reclaimed + advanced change-attr + an ENOENT window +
# still-mapped pages → EIO reads for the mapping's life). The handler stayed
# fully alive serving every other path, and the same build passed 6/6 cycles
# once actually quiesced. The gate that keeps this drill valid is therefore the
# quiesce below — mux_drain_tenant_readers reaps by process group AND by
# wrapper/worker pattern, and dies loud on any survivor. The cycle cadence
# needs no slowing: the production contract the drill mirrors is "never detach
# a mapped path", not a rate limit, and back-to-back detach/attach coverage is
# the point of assertion c.
cmd_mux_fileids() {
  local cycles="${1:-6}" victim
  pid_alive "$MUX_SERVE_PID" || wdie "mux-serve is not running — run mux-setup first"
  victim="$(mux_victim_name)"
  wlog "quiescing victim $victim's readers (mirrors production's live-session gate) before the fileids detach/re-attach drill"
  mux_drain_tenant_readers "$victim"
  mux_vmstress mux-fileids --cycles "$cycles"
  wlog "restarting victim $victim's readers so detach-under-load and churn-fill keep full coverage"
  mux_start_tenant_readers "$victim"
}

# cmd_mux_detach_load runs the detach-under-load drill: hold tenant A's open file
# and mmap while tenant B detaches; A is unaffected, B goes ENOENT then re-serves.
cmd_mux_detach_load() {
  local seconds="${1:-20}"
  pid_alive "$MUX_SERVE_PID" || wdie "mux-serve is not running — run mux-setup first"
  mux_vmstress mux-detach-load --seconds "$seconds"
}

# cmd_mux_forceunmount is the native-wedge aggravation for the reassembly drill:
# quiesce every mapped-page holder first (the production design force-unmounts the
# native root ONLY when the pool is idle — no live session holds a mapping), then
# forcibly unmount the native root out from under the holder.
cmd_mux_forceunmount() {
  mount | grep -qF " on $MUX_ROOT (" || wdie "nothing mounted at $MUX_ROOT"
  wlog "quiescing readers (pool-idle gate) before the native force-unmount"
  mux_stop_readers
  wlog "FORCED UNMOUNT of the native mux root: sudo umount -f $MUX_ROOT"
  sudo -n umount -f "$MUX_ROOT"
}

# cmd_mux_reassemble re-issues every tenant's Mount RPC after a native
# force-unmount: the root remounts once and all tenants re-attach and serve. It
# then restarts the mmap readers (the pool comes back online).
cmd_mux_reassemble() {
  pid_alive "$MUX_SERVE_PID" || wdie "mux-serve is not running — run mux-setup first"
  mux_vmstress mux-reassemble
  mux_start_readers
}

# cmd_mux_status prints serve/reader/mount/go-nfsv4 state into the scenario log.
cmd_mux_status() {
  if pid_alive "$MUX_SERVE_PID"; then
    wlog "mux-serve: running (pid $(cat "$MUX_SERVE_PID"))"
  else
    wlog "mux-serve: stopped"
  fi
  mount | grep -F " on $MUX_ROOT" || wlog "no mount under $MUX_ROOT"
  wlog "go-nfsv4: $(pgrep -x go-nfsv4 | tr '\n' ' ')"
  tail -n 20 "$MUX_SERVE_LOG" 2>/dev/null || true
}

# cmd_mux_stop reclaims everything setup started: readers first (they respawn
# otherwise), lingering churn workers, then a graceful serve TERM so the holder
# detaches every tenant and the last detach unmounts the native root.
cmd_mux_stop() {
  local pid t0
  mux_stop_readers
  pkill -f "$VMSTRESS mux-churn" 2>/dev/null || true
  if pid_alive "$MUX_SERVE_PID"; then
    pid="$(cat "$MUX_SERVE_PID")"
    wlog "stopping mux-serve (pid $pid) — detaches all tenants, unmounts the native root"
    kill "$pid" 2>/dev/null || true
    t0="$(date +%s)"
    while kill -0 "$pid" 2>/dev/null; do
      if (($(date +%s) - t0 >= 60)); then
        wlog "mux-serve did not exit within 60s; SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
        break
      fi
      sleep 1
    done
  fi
  rm -f "$MUX_SERVE_PID"
  # The pidfile only names THIS run's serve; serves orphaned by prior runs
  # (their pidfile was overwritten) survive the graceful path above and
  # accumulate — 5 stale mux-serves were part of the polluted-guest state
  # behind the 2026-07-03 fileids EIO. TERM first so each detaches its tenants
  # and unmounts its root, then escalate.
  if pgrep -f "$VMSTRESS mux-serve" >/dev/null 2>&1; then
    wlog "sweeping stale mux-serve process(es): $(pgrep -f "$VMSTRESS mux-serve" | tr '\n' ' ')"
    pkill -f "$VMSTRESS mux-serve" 2>/dev/null || true
    t0="$(date +%s)"
    while pgrep -f "$VMSTRESS mux-serve" >/dev/null 2>&1; do
      if (($(date +%s) - t0 >= 60)); then
        wlog "stale mux-serve did not exit within 60s; SIGKILL"
        pkill -9 -f "$VMSTRESS mux-serve" 2>/dev/null || true
        sleep 1
        break
      fi
      sleep 1
    done
  fi
}

main() {
  local cmd="${1:-}"
  [[ -n "$cmd" ]] || wdie "usage: mux-workload.sh <mux-setup|mux-onemount|mux-isolation|mux-churn SECONDS|mux-fileids [CYCLES]|mux-detach-load [SECONDS]|mux-forceunmount|mux-reassemble|mux-status|mux-stop>"
  shift
  case "$cmd" in
  mux-setup) cmd_mux_setup "$@" ;;
  mux-onemount) cmd_mux_onemount "$@" ;;
  mux-isolation) cmd_mux_isolation "$@" ;;
  mux-churn) cmd_mux_churn "$@" ;;
  mux-fileids) cmd_mux_fileids "$@" ;;
  mux-detach-load) cmd_mux_detach_load "$@" ;;
  mux-forceunmount) cmd_mux_forceunmount "$@" ;;
  mux-reassemble) cmd_mux_reassemble "$@" ;;
  mux-status) cmd_mux_status "$@" ;;
  mux-stop) cmd_mux_stop "$@" ;;
  *) wdie "unknown command: $cmd" ;;
  esac
}

main "$@"
