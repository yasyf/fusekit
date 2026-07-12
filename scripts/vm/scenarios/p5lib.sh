# shellcheck shell=bash
# scripts/vm/scenarios/p5lib.sh — shared host-side helpers for the Phase-5
# holder-v2 validation scenarios (p5-*.sh). Sourced after lib.sh.

: "${VMCTL_GUEST_DIR:?p5 scenarios only run under scripts/vm/vmctl run}"

P5_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
P5_REPO_ROOT="$(cd "$P5_DIR/../../.." && pwd)"
WORKLOAD="$VMCTL_GUEST_DIR/workload.sh"
P5RESET="$VMCTL_GUEST_DIR/p5reset.sh"
P5UTIL="$VMCTL_GUEST_DIR/bin/p5util"
RUN_DIR="$VMCTL_GUEST_DIR/run"
MOUNT_DIR="$VMCTL_GUEST_DIR/mnt"
STATE_DIR="$VMCTL_GUEST_DIR/stress"
GUEST_FUSEKIT="$VM_GUEST_HOME/.fusekit"
JOURNAL="$GUEST_FUSEKIT/holder-specs.json"
STRIKES="$GUEST_FUSEKIT/holder-retires.json"
PLIST="$VMCTL_GUEST_HOLDER_APP/Contents/Info.plist"

guest_workload() { vm_ssh "bash '$WORKLOAD' $*"; }

# p5_install stages workload.sh, p5reset.sh, and p5util into the guest.
p5_install() {
  vm_scp_to "$P5_DIR/workload.sh" "$WORKLOAD"
  vm_scp_to "$P5_DIR/p5reset.sh" "$P5RESET"
  local stage="$VM_ROOT/stage"
  mkdir -p "$stage"
  if [[ ! -x "$stage/p5util" ]]; then
    log "building p5util (pure Go)"
    (cd "$P5_REPO_ROOT" && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -o "$stage/p5util" ./scripts/vm/p5util)
  fi
  vm_ssh "mkdir -p '$VMCTL_GUEST_DIR/bin' '$RUN_DIR'"
  vm_scp_to "$stage/p5util" "$P5UTIL"
  vm_ssh "chmod +x '$P5UTIL'"
}

# p5_install_bin ships a pre-staged consumer binary (ccp, ccn) into the guest.
p5_install_bin() {
  local name="$1"
  [[ -x "$VM_ROOT/stage/$name" ]] || die "$name not staged at $VM_ROOT/stage/$name — build it before this run (see p5-vm RESULTS.md)"
  vm_scp_to "$VM_ROOT/stage/$name" "$VMCTL_GUEST_DIR/bin/$name"
  vm_ssh "chmod +x '$VMCTL_GUEST_DIR/bin/$name'"
}

# p5_reset returns the guest to a clean pre-scenario state (everything down,
# fusekit state wiped, plist skew reverted).
p5_reset() {
  vm_ssh "bash '$P5RESET'" || die "p5reset.sh failed in the guest"
}

# --- holder protocol ------------------------------------------------------------

# holder_req sends one raw proto-2 JSON request; prints the response line.
holder_req() { vm_ssh "'$P5UTIL' req --timeout '${2:-60s}' '$1'"; }
holder_health() { holder_req '{"proto":2,"op":"health"}' "${1:-15s}"; }
holder_pid() { vm_ssh "pgrep -x fusekit-holder | head -n 1" 2>/dev/null || true; }
holder_up() { holder_health 2>/dev/null | grep -q '"ok":true'; }
holder_gone() { [[ -z "$(holder_pid)" ]]; }

# holder_launch retries open -g: a single shot fired seconds after a kill -9
# can silently no-op while LaunchServices still holds the dead instance's
# record (production relaunch paths — launchd KeepAlive, EnsureRunning — retry).
holder_launch() {
  local round t0
  for round in 1 2 3 4; do
    vm_ssh "open -g '$VMCTL_GUEST_HOLDER_APP'" || true
    t0="$(date +%s)"
    while (($(date +%s) - t0 < 15)); do
      if holder_up; then
        log "ready: holder serving its socket (open round $round)"
        return 0
      fi
      sleep 2
    done
  done
  die "holder not serving after 4 open -g rounds"
}

resp_ok() { grep -q '"ok":true' <<<"$1"; }
resp_class() { sed -n 's/.*"err_class":"\([^"]*\)".*/\1/p' <<<"$1"; }

# --- polling --------------------------------------------------------------------

# p5_await polls "$@" (a host-side function) until true, else dies at timeout.
p5_await() {
  local timeout="$1" what="$2" t0
  shift 2
  t0="$(date +%s)"
  until "$@"; do
    (($(date +%s) - t0 < timeout)) || die "timed out (${timeout}s) waiting for: $what"
    sleep 2
  done
  log "ready: $what"
}

# p5_never asserts "$@" stays false for the whole window.
p5_never() {
  local seconds="$1" what="$2" t0
  shift 2
  t0="$(date +%s)"
  while (($(date +%s) - t0 < seconds)); do
    if "$@"; then die "forbidden state observed: $what"; fi
    sleep 2
  done
}

# --- guest state probes ---------------------------------------------------------

mounted() { vm_ssh "mount | grep -qF ' on $1 ('"; }
# The journal is MarshalIndent-pretty ("dir": "..."); tolerate the space.
journal_has_dir() { vm_ssh "grep -qE '\"dir\": ?\"$1\"' '$JOURNAL'"; }
proc_alive() { vm_ssh "kill -0 $1" 2>/dev/null; }

# guest_read_ok asserts a bounded full read of $1 answers ok.
guest_read_ok() { vm_ssh "'$P5UTIL' readfile --timeout '${2:-10s}' '$1'" | grep -qx ok; }
# guest_stat prints the bounded-stat verdict for $1: ok | hung | errno:...
guest_stat() { vm_ssh "'$P5UTIL' stat --timeout '${2:-3s}' '$1'"; }

# --- vmstress spec (must mirror cmd/vmstress newPaths + spec) ---------------------

vmstress_mount_json() {
  printf '{"proto":2,"op":"mount","base":"%s","dir":"%s","owner":"vmstress","content_socket":"%s","domain":"vmstress","private_root":"%s","content_mode":"source","probe_path":"/.stress-probe","private_prefixes":["settings.json","credentials.json"]}' \
    "$STATE_DIR/base" "$MOUNT_DIR" "$STATE_DIR/bridge.sock" "$STATE_DIR/private"
}
vmstress_unmount_json() {
  printf '{"proto":2,"op":"unmount","base":"%s","dir":"%s","owner":"vmstress"}' "$STATE_DIR/base" "$MOUNT_DIR"
}

# plain_mount_json BASE DIR OWNER — a bare passthrough row.
plain_mount_json() { printf '{"proto":2,"op":"mount","base":"%s","dir":"%s","owner":"%s"}' "$1" "$2" "$3"; }
plain_unmount_json() { printf '{"proto":2,"op":"unmount","base":"%s","dir":"%s","owner":"%s"}' "$1" "$2" "$3"; }

# --- reader/lease process management ---------------------------------------------

# p5_stop_readers kills setup's mmap readers but leaves serve + the mount up,
# so graceful unmounts see no EBUSY from held maps.
p5_stop_readers() {
  vm_ssh "for f in '$RUN_DIR'/reader-*.pid; do [ -f \"\$f\" ] && kill \"\$(cat \"\$f\")\" 2>/dev/null; rm -f \"\$f\"; done; pkill -f 'vmstress read' 2>/dev/null; true" || true
  sleep 2
}

# p5_leasehold_start DIR OWNER — detached p5util leasehold; prints its guest pid.
p5_leasehold_start() {
  vm_ssh "rm -f '$RUN_DIR/leasehold-$2.log'; nohup '$P5UTIL' leasehold --dir '$1' --owner '$2' >>'$RUN_DIR/leasehold-$2.log' 2>&1 & echo \$!"
}
p5_leasehold_held() { vm_ssh "grep -q '^held ' '$RUN_DIR/leasehold-$1.log'"; }

# --- retire skew -----------------------------------------------------------------

p5_skew_on() {
  vm_ssh "cp '$PLIST' '$PLIST.p5bak' && /usr/bin/sed -i '' 's|<key>CFBundleShortVersionString</key><string>[^<]*</string>|<key>CFBundleShortVersionString</key><string>vm-skewed</string>|' '$PLIST'"
}
p5_skew_off() { vm_ssh "[ -f '$PLIST.p5bak' ] && mv '$PLIST.p5bak' '$PLIST'; true" || true; }
