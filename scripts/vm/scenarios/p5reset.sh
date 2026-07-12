#!/usr/bin/env bash
# scripts/vm/scenarios/p5reset.sh — GUEST-side reset between p5 scenarios:
# stop consumers/holder/servers, clear leftover mounts under $HOME, wipe
# fusekit state, revert any plist skew.
[[ "$(sysctl -n kern.hv_vmm_present 2>/dev/null)" == "1" ]] || {
  echo "p5reset.sh: REFUSING TO RUN: not a VM" >&2
  exit 86
}
set -u

GUEST_DIR="$HOME/fusekit-vm"

pkill -f 'bin/ccn' 2>/dev/null
pkill -f 'lease-agent' 2>/dev/null
pkill -f 'p5util leasehold' 2>/dev/null
pkill -f "$GUEST_DIR/bin/vmstress" 2>/dev/null
pkill -x sleep 2>/dev/null

# WAIT for vmstress to be fully gone: a dying serve's graceful teardown
# unlinks its bridge socket seconds after TERM, which would yank the path out
# from under the next scenario's freshly-bound bridge.
for _ in $(seq 1 30); do
  pgrep -f "$GUEST_DIR/bin/vmstress" >/dev/null || break
  sleep 1
done
pkill -9 -f "$GUEST_DIR/bin/vmstress" 2>/dev/null

pkill -TERM -x fusekit-holder 2>/dev/null
for _ in $(seq 1 30); do
  pgrep -x fusekit-holder >/dev/null || break
  sleep 1
done
pkill -9 -x fusekit-holder 2>/dev/null

# Resume any SIGSTOP-wedged server before killing, so unmounts can drain.
pkill -CONT -x go-nfsv4 2>/dev/null
pkill -x go-nfsv4 2>/dev/null
sleep 2
pkill -9 -x go-nfsv4 2>/dev/null

# Dead-server leftovers: graceful umount first, force as last resort (the
# harness's accepted reset primitive; no load, no dirty maps at this point).
mount | sed -n "s|.* on \($HOME/[^(]*\) (.*|\1|p" | while read -r m; do
  m="${m% }"
  umount "$m" 2>/dev/null || sudo -n umount -f "$m" 2>/dev/null
done

rm -f "$HOME/.fusekit/holder-specs.json" "$HOME/.fusekit/holder-retires.json"
rm -rf "$HOME/.fusekit/leases"
rm -rf "$GUEST_DIR/run"
mkdir -p "$GUEST_DIR/run" "$GUEST_DIR/bin"

PLIST="/Applications/fusekit-holder.app/Contents/Info.plist"
[[ -f "$PLIST.p5bak" ]] && mv "$PLIST.p5bak" "$PLIST"

exit 0
