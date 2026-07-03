# shellcheck shell=bash
# scripts/vm/lib.sh — shared plumbing for the disposable tart VM harness.
#
# Sourced by vmctl and push.sh; `vmctl run` sources scenarios with this lib
# already loaded. Every function here executes on the HOST; guest work goes
# through vm_ssh/vm_sudo/vm_scp_to/vm_scp_from. All mutable state lives under
# /tmp/fusekit-vm — vm_tart pins TART_HOME there on every invocation, so tart
# never touches ~/.tart.
#
# Safety: this harness exists because fuse workloads kernel-panic machines.
# Nothing here mounts or runs the holder on the host, and vm_assert_guest
# refuses any target that is not a VM (kern.hv_vmm_present != 1).

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "lib.sh is a library: source it (see scripts/vm/vmctl)" >&2
  exit 64
fi

# --- Tunables (env-overridable) ----------------------------------------------

export VMCTL_NAME="${VMCTL_NAME:-fusekit-test}"
export VMCTL_IMAGE="${VMCTL_IMAGE:-ghcr.io/cirruslabs/macos-tahoe-base:latest}"
export VMCTL_CPUS="${VMCTL_CPUS:-4}"
export VMCTL_MEMORY_MB="${VMCTL_MEMORY_MB:-8192}"
export VMCTL_DISK_GB="${VMCTL_DISK_GB:-60}"
# 10 min is the standing validation window: the unmitigated build panics in
# ~2 s, so a clean 10 min run (hundreds of times the failure latency) is a
# decisive pass. Raise it per-invocation for a longer soak.
export VMCTL_RUN_TIMEOUT_MIN="${VMCTL_RUN_TIMEOUT_MIN:-10}"
export VMCTL_GRAPHICS="${VMCTL_GRAPHICS:-0}"
# Space-separated TCC "Network Volumes" grantees: a bundle id, or an absolute
# path (stored as client_type 1). sshd-keygen-wrapper is the TCC responsible
# process for everything run over ssh; the holder app is its own responsible
# process because LaunchServices (`open -g`) launches it.
export VMCTL_TCC_CLIENTS="${VMCTL_TCC_CLIENTS:-com.apple.sshd-keygen-wrapper com.yasyf.fusekit-holder}"

# --- Fixed layout: ALL state under /tmp/fusekit-vm ----------------------------

export VM_ROOT="/tmp/fusekit-vm"
export VM_TART_HOME="$VM_ROOT/tart"
# Belt and braces: exported once here AND pinned per-invocation in vm_tart /
# vm_start, so no code path can reach ~/.tart.
export TART_HOME="$VM_TART_HOME"
VM_SSH_DIR="$VM_ROOT/ssh"
export VM_SSH_KEY="$VM_SSH_DIR/id_ed25519"
export VM_RESULTS_ROOT="$VM_ROOT/results"
VM_LOG_DIR="$VM_ROOT/logs"
export VM_STATE_DIR="$VM_ROOT/state"

export VM_GUEST_USER="admin"
export VM_GUEST_PASS="admin"
export VM_GUEST_HOME="/Users/$VM_GUEST_USER"
export VM_GUEST_MARKER="$VM_GUEST_HOME/.vmctl-run-marker"

# Guest install layout (written by push.sh, consumed by scenarios/vmstress).
# The holder sits at the production cask path so mountd.HolderApp/HolderExe
# and DefaultHolderSocket() work unmodified inside the guest.
export VMCTL_GUEST_DIR="$VM_GUEST_HOME/fusekit-vm"
export VMCTL_GUEST_VMSTRESS="$VMCTL_GUEST_DIR/bin/vmstress"
export VMCTL_GUEST_HOLDER_APP="/Applications/fusekit-holder.app"

# --- Logging -------------------------------------------------------------------

# log writes a timestamped progress line to stderr.
log() { printf '%s vmctl: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

# warn writes a timestamped warning to stderr.
warn() { printf '%s vmctl: WARN: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2; }

# die writes a fatal message and exits 1 (the harness-wide infra-failure code).
die() {
  printf '%s vmctl: FATAL: %s\n' "$(date -u '+%H:%M:%S')" "$*" >&2
  exit 1
}

# require_cmd dies unless $1 is on PATH; $2 is an optional install hint.
require_cmd() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1${2:+ ($2)}"; }

# vm_require_tart dies unless tart is installed (create installs it, loudly).
vm_require_tart() {
  command -v tart >/dev/null 2>&1 || die "tart is not installed; run: vmctl create (installs it via brew, loudly)"
}

# vm_ensure_dirs creates the /tmp/fusekit-vm tree.
vm_ensure_dirs() {
  mkdir -p "$VM_TART_HOME" "$VM_SSH_DIR" "$VM_RESULTS_ROOT" "$VM_LOG_DIR" "$VM_STATE_DIR"
}

# --- tart ----------------------------------------------------------------------

# vm_tart runs tart with TART_HOME pinned inside /tmp/fusekit-vm.
vm_tart() { TART_HOME="$VM_TART_HOME" command tart "$@"; }

# vm_list_field prints one header-named column of the VM's `tart list` row,
# or nothing when the VM has no row. Column positions are resolved from the
# header line, so a tart version reordering or inserting columns cannot
# silently repoint the parse. The LAST header column is read from the row's
# END ($NF): tart 2.32's `tart list` has a multi-word "Accessed" column
# ("4 seconds ago") that shifts every field after it, which made a
# field-indexed State read "seconds" — never "running" — and sent cmd_run's
# watcher into a one-a-second tart-relaunch storm over a perfectly live VM
# (the storm behind the 2026-07-03 polluted-guest diagnosis runs).
vm_list_field() {
  vm_tart list 2>/dev/null | awk -v n="$VMCTL_NAME" -v f="$1" '
    NR == 1 {
      for (i = 1; i <= NF; i++) col[$i] = i
      last = $NF
      next
    }
    col["Name"] && col[f] && $col["Name"] == n { print(f == last ? $NF : $col[f]) }'
}

# vm_exists reports whether the VM has been cloned.
vm_exists() { [[ -n "$(vm_list_field Name)" ]]; }

# vm_is_running reports whether tart itself considers the VM running. This is
# the authoritative liveness signal: the pidfile can point at a relaunched
# `tart run` that lost the race to the surviving owner and exited "already
# running", so keying liveness off the pidfile desyncs and storms.
vm_is_running() { [[ "$(vm_list_field State)" == "running" ]]; }

# vm_start launches `tart run` detached (nohup, pidfile). Mode "headless"
# forces --no-graphics; the default "auto" honors VMCTL_GRAPHICS=1, the
# one-time window used for the TCC click-Allow path.
vm_start() {
  local mode="${1:-auto}" logf
  vm_ensure_dirs
  # tart refuses a second `run` of a VM that is already up ("VM is already
  # running!"); that competitor exits instantly. Adopt the running VM instead
  # of racing it. tart's own run-state (vm_is_running) is the single liveness
  # signal — there is no pidfile to desync.
  if vm_is_running; then
    log "VM $VMCTL_NAME already running; adopting it"
    return 0
  fi
  local args=("run" "$VMCTL_NAME")
  if [[ "$mode" == "headless" || "$VMCTL_GRAPHICS" != "1" ]]; then
    args+=("--no-graphics")
  fi
  logf="$VM_LOG_DIR/tart-run-$(date +%Y%m%d-%H%M%S).log"
  log "starting: tart ${args[*]} (log: $logf)"
  TART_HOME="$VM_TART_HOME" nohup tart "${args[@]}" >>"$logf" 2>&1 &
  disown
}

# --- Guest reachability ---------------------------------------------------------

# vm_ip prints the guest IP, cached per vmctl process; vm_ip_forget drops the
# cache (call it whenever the guest drops, the lease can change across reboots).
vm_ip() {
  if [[ -n "${VM_IP_CACHE:-}" ]]; then
    printf '%s\n' "$VM_IP_CACHE"
    return 0
  fi
  local ip
  ip="$(vm_tart ip "$VMCTL_NAME" 2>/dev/null)" || return 1
  [[ -n "$ip" ]] || return 1
  VM_IP_CACHE="$ip"
  printf '%s\n' "$ip"
}

# vm_ip_forget invalidates the cached guest IP.
vm_ip_forget() { VM_IP_CACHE=""; }

VM_SSH_OPTS=(
  -i "$VM_SSH_KEY"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ConnectTimeout=8
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=3
  -o BatchMode=yes
)

# vm_ssh runs its arguments as a command in the guest (key auth only). Returns
# nonzero when the guest is unreachable — callers own the reaction, the panic
# watcher polls through this. Non-interactive sshd commands get the bare
# /usr/bin:/bin PATH, so Homebrew's prefix is prepended for every command.
vm_ssh() {
  local ip
  ip="$(vm_ip)" || return 1
  # shellcheck disable=SC2029 # helpers take remote command strings built host-side by design
  ssh "${VM_SSH_OPTS[@]}" "$VM_GUEST_USER@$ip" "export PATH=/opt/homebrew/bin:/opt/homebrew/sbin:\$PATH; $*"
}

# vm_ssh_ok reports whether key-based ssh into the guest works right now.
vm_ssh_ok() { vm_ssh true >/dev/null 2>&1; }

# vm_sudo runs a single remote command string as root. Provision established
# passwordless sudo, so -n either works or fails loudly.
vm_sudo() { vm_ssh "sudo -n -- $*"; }

# vm_scp_to copies a local file or directory into the guest: vm_scp_to <local> <remote>.
vm_scp_to() {
  local ip
  ip="$(vm_ip)" || return 1
  scp -q "${VM_SSH_OPTS[@]}" -r "$1" "$VM_GUEST_USER@$ip:$2"
}

# vm_scp_from copies a guest file or directory out: vm_scp_from <remote> <local>.
vm_scp_from() {
  local ip
  ip="$(vm_ip)" || return 1
  scp -q "${VM_SSH_OPTS[@]}" -r "$VM_GUEST_USER@$ip:$1" "$2"
}

# vm_wait_port22 waits up to $1 seconds for the guest to accept TCP on 22.
vm_wait_port22() {
  local timeout="$1" t0 ip
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    vm_ip_forget
    if ip="$(vm_ip)" && nc -z -G 4 "$ip" 22 >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  return 1
}

# vm_wait_ssh waits up to $1 seconds for key-based ssh to work.
vm_wait_ssh() {
  local timeout="$1" t0
  t0="$(date +%s)"
  while (($(date +%s) - t0 < timeout)); do
    if vm_ssh_ok; then
      return 0
    fi
    vm_ip_forget
    sleep 5
  done
  return 1
}

# vm_ensure_running makes sure the VM process is up and key-ssh works, starting
# tart if needed. Fails (rather than creates) when the VM does not exist.
vm_ensure_running() {
  local timeout="${1:-300}"
  if vm_ssh_ok; then
    return 0
  fi
  vm_exists || die "VM $VMCTL_NAME does not exist — run: vmctl create && vmctl provision"
  if ! vm_is_running; then
    vm_start auto
  fi
  vm_wait_ssh "$timeout"
}

# --- One-time guest bootstrap ---------------------------------------------------

# vm_authorize_ssh_key installs the harness public key into the guest's
# authorized_keys. First contact drives ssh-copy-id's password prompt with
# /usr/bin/expect (ships with macOS; sshpass is not a host dependency); every
# later connection is key-only.
vm_authorize_ssh_key() {
  vm_ensure_dirs
  if [[ ! -f "$VM_SSH_KEY" ]]; then
    log "generating harness ssh key: $VM_SSH_KEY"
    ssh-keygen -q -t ed25519 -N "" -C "fusekit-vmctl" -f "$VM_SSH_KEY"
  fi
  if vm_ssh_ok; then
    log "ssh key already authorized in guest"
    return 0
  fi
  local ip
  ip="$(vm_ip)" || die "cannot resolve guest IP for the ssh-key bootstrap"
  log "installing ssh key via password auth (expect, one time; creds $VM_GUEST_USER/$VM_GUEST_PASS)"
  /usr/bin/expect <<EOF || die "ssh key bootstrap failed (see expect output)"
set timeout 90
spawn /usr/bin/ssh-copy-id -i "$VM_SSH_KEY.pub" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o PubkeyAuthentication=no "$VM_GUEST_USER@$ip"
expect {
    -re {[Pp]assword:} { send -- "$VM_GUEST_PASS\r"; exp_continue }
    timeout { exit 92 }
    eof {}
}
catch wait result
exit [lindex \$result 3]
EOF
  vm_ssh_ok || die "key installed but key-based ssh still fails"
  log "ssh key authorized"
}

# vm_ensure_sudo makes guest sudo passwordless (cirrus images usually ship this
# way; when they do not, install a NOPASSWD rule once — the guest is disposable).
vm_ensure_sudo() {
  if vm_ssh "sudo -n true" >/dev/null 2>&1; then
    return 0
  fi
  log "guest sudo wants a password; installing a NOPASSWD rule (one time, guest-only)"
  # sudo -S consumes line 1 (the password); tee writes line 2 into sudoers.d.
  printf '%s\n%s\n' "$VM_GUEST_PASS" "$VM_GUEST_USER ALL=(ALL) NOPASSWD: ALL" |
    vm_ssh "sudo -S -p '' tee /etc/sudoers.d/vmctl-nopasswd >/dev/null"
  vm_ssh "sudo -n chmod 440 /etc/sudoers.d/vmctl-nopasswd"
  vm_ssh "sudo -n true" >/dev/null 2>&1 || die "could not establish passwordless sudo in the guest"
}

# --- VM-only guard ---------------------------------------------------------------

# vm_assert_guest dies unless the ssh target is a virtual machine. This is the
# structural rail that keeps deliberate panic workloads off bare metal.
vm_assert_guest() {
  local v
  v="$(vm_ssh sysctl -n kern.hv_vmm_present 2>/dev/null)" || die "cannot read kern.hv_vmm_present from the guest"
  v="${v//[^0-9]/}"
  [[ "$v" == "1" ]] || die "ssh target is NOT a VM (kern.hv_vmm_present=$v); refusing to drive fuse workloads"
}

# --- Panic detection and evidence -------------------------------------------------

# vm_boottime prints the guest kern.boottime seconds, or returns 1 when the
# guest is unreachable. A change against the run's baseline means the guest
# rebooted underneath us — on this harness, that is a kernel panic signal.
vm_boottime() {
  local out
  out="$(vm_ssh sysctl -n kern.boottime 2>/dev/null)" || return 1
  # sysctl prints "{ sec = N, usec = N } <date>". Anchor on the opening brace:
  # a bare ".*sec = " greedily matches the trailing "usec = N" field and
  # records microseconds instead of the boot epoch seconds.
  out="$(printf '%s\n' "$out" | sed -nE 's/.*\{ *sec = ([0-9]+).*/\1/p' | head -n 1)"
  [[ -n "$out" ]] || return 1
  printf '%s\n' "$out"
}

# vm_mark_run_start (re)creates the guest-side marker file that timestamps the
# run; vm_new_panic_count counts .panic reports newer than it. The marker lives
# in the guest home, which survives reboots (unlike guest /tmp).
vm_mark_run_start() { vm_ssh "rm -f '$VM_GUEST_MARKER' && touch '$VM_GUEST_MARKER'"; }

# vm_new_panic_count prints how many guest .panic reports are newer than the
# run-start marker; prints nothing and returns 1 when the probe cannot run.
vm_new_panic_count() {
  local n
  # shellcheck disable=SC2029 # host-side expansion of the marker path is intended
  n="$(vm_sudo "find /Library/Logs/DiagnosticReports /Library/Logs/DiagnosticReports/Retired -maxdepth 1 -name '*.panic' -newer '$VM_GUEST_MARKER' 2>/dev/null | wc -l" 2>/dev/null)" || return 1
  n="$(printf '%s' "$n" | tr -d '[:space:]')"
  [[ "$n" =~ ^[0-9]+$ ]] || return 1
  printf '%s\n' "$n"
}

# vm_scrape_panics copies every guest .panic report (current and Retired) into
# <dest>/panics, staging through a root-readable guest dir.
vm_scrape_panics() {
  local dest="$1"
  mkdir -p "$dest"
  vm_sudo "rm -rf /tmp/vmctl-panics && mkdir -p /tmp/vmctl-panics && find /Library/Logs/DiagnosticReports /Library/Logs/DiagnosticReports/Retired -maxdepth 1 -name '*.panic' -exec cp {} /tmp/vmctl-panics/ ';' 2>/dev/null; chmod -R a+rX /tmp/vmctl-panics" || return 1
  rm -rf "$dest/panics"
  vm_scp_from "/tmp/vmctl-panics" "$dest" || return 1
  mv "$dest/vmctl-panics" "$dest/panics"
  log "panic reports in $dest/panics: $(find "$dest/panics" -name '*.panic' | wc -l | tr -d ' ')"
}

# --- Scenario helpers --------------------------------------------------------------

# vm_phase records the scenario's active workload phase; the label lands in
# meta.json verbatim, so it is restricted to [A-Za-z0-9._-]+.
vm_phase() {
  local label="$1"
  [[ "$label" =~ ^[A-Za-z0-9._-]+$ ]] || die "vm_phase: label must match [A-Za-z0-9._-]+: $label"
  [[ -n "${VMCTL_PHASE_FILE:-}" ]] || die "vm_phase: only valid inside vmctl run"
  printf '%s\n' "$label" >"$VMCTL_PHASE_FILE"
  log "workload phase: $label"
}

# vm_seconds_left prints the seconds remaining before the run deadline (0 when
# past it); scenarios loop on this to fill the bounded window.
vm_seconds_left() {
  [[ -n "${VMCTL_DEADLINE_EPOCH:-}" ]] || die "vm_seconds_left: only valid inside vmctl run"
  local left=$((VMCTL_DEADLINE_EPOCH - $(date +%s)))
  ((left > 0)) || left=0
  printf '%s\n' "$left"
}
