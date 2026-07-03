#!/usr/bin/env bash
# Regenerate docs/assets/demo.png: a REAL captured run of the kill -9 survival
# sequence, rendered with freeze. Needs macOS with fuse-t installed and the
# freeze CLI (brew install charmbracelet/tap/freeze).
#
# SAFETY: the run is fully isolated under /tmp/fusekit-demo — its own scratch
# root, socket, and holder binary. It never touches ~/.fusekit or a live
# holder. kill -9 lands only on the demo daemon (safe by design: that is the
# point); the holder itself is retired gracefully on every exit path, with a
# force-unmount as the belt.
set -euo pipefail

repo="$(cd "$(dirname "$0")/../.." && pwd)"
root=/tmp/fusekit-demo
out="$repo/docs/assets/demo.png"

cleanup() {
  if [ -x "$root/daemon" ]; then
    "$root/daemon" -cleanup -root "$root" >/dev/null 2>&1 || true
  fi
  /sbin/umount -f "$root/mnt" >/dev/null 2>&1 || true
  rm -rf "$root"
}
trap cleanup EXIT

# A stale mount from an interrupted prior run must come down before rm -rf.
if mount | grep -q "$root/mnt"; then
  /sbin/umount -f "$root/mnt"
fi
rm -rf "$root"
mkdir -p "$root/src" "$root/mnt"
echo "all mounts alive" > "$root/src/report.txt"

(cd "$repo" && go build -tags fuse -o "$root/fusekit-holder" ./cmd/holder)
(cd "$repo" && go build -o "$root/daemon" docs/scripts/demo-daemon.go)

prompt=$'\e[38;5;42m$\e[0m'
ts="$root/transcript.ansi"

{
  printf '%s ./daemon -root /tmp/fusekit-demo &\n' "$prompt"
  "$root/daemon" -root "$root" > "$root/daemon.out" 2>&1 &
  for _ in $(seq 1 120); do
    grep -q "serving" "$root/daemon.out" 2>/dev/null && break
    sleep 0.5
  done
  cat "$root/daemon.out"
  dpid="$(sed -n 's/^daemon\[\([0-9]*\)\].*/\1/p' "$root/daemon.out")"
  [ -n "$dpid" ] || { echo "demo daemon never came up; see $root/holder.log" >&2; exit 1; }

  printf '\n%s cat /tmp/fusekit-demo/mnt/report.txt\n' "$prompt"
  cat "$root/mnt/report.txt"

  printf '\n%s kill -9 %s\n' "$prompt" "$dpid"
  kill -9 "$dpid"
  sleep 1

  printf '\n%s cat /tmp/fusekit-demo/mnt/report.txt   # the daemon is gone; the mount is not\n' "$prompt"
  cat "$root/mnt/report.txt"

  printf '\n%s mount | grep fusekit-demo\n' "$prompt"
  mount | grep fusekit-demo
} > "$ts"

freeze "$ts" --language ansi --theme github-dark --background "#0d1117" \
  --window --padding 24 --font.size 28 -o "$out"
echo "wrote $out"
