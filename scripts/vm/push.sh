#!/usr/bin/env bash
# scripts/vm/push.sh — the fusekit-specific seam of the VM harness.
#
# Host-builds the fusekit-holder app bundle (cgo, -tags fuse — BUILD ONLY, the
# holder is NEVER executed on the host) and vmstress (pure Go), installs both
# into the guest, and proves the stack end-to-end with an in-guest
# `vmstress selftest`. The holder lands at the production cask path
# (/Applications/fusekit-holder.app) so mountd.HolderApp/HolderExe and
# DefaultHolderSocket() hold inside the guest.
#
# BUILD_REV (env-overridable; defaults to the repo's short HEAD, "-dirty"
# appended when the tree is not clean) is recorded in the guest and host state
# so `vmctl run` can prove in meta.json which build a verdict applies to.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

main() {
  require_cmd go
  require_cmd git
  require_cmd codesign
  vm_require_tart
  vm_ensure_dirs
  vm_ensure_running 600 || die "VM unreachable — run vmctl provision first"
  vm_assert_guest

  local rev
  if [[ -n "${BUILD_REV:-}" ]]; then
    rev="$BUILD_REV"
  else
    rev="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
    if [[ -n "$(git -C "$REPO_ROOT" status --porcelain)" ]]; then
      rev="$rev-dirty"
    fi
  fi

  local stage="$VM_ROOT/stage"
  rm -rf "$stage"
  mkdir -p "$stage/bin"

  local build_serial
  build_serial="$(date +%s)"
  if [[ -f "$VM_STATE_DIR/build-serial" ]]; then
    local previous_serial
    previous_serial="$(<"$VM_STATE_DIR/build-serial")"
    if ((build_serial <= previous_serial)); then
      build_serial=$((previous_serial + 1))
    fi
  fi
  local build_version="9999.$build_serial.0-dev"
  local ldflags="-X main.buildVersion=$build_version -X main.buildCommit=$rev"

  log "building vmstress (pure Go, BUILD_REV=$rev)"
  (cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "$ldflags" -o "$stage/bin/vmstress" ./cmd/vmstress)

  local app="$stage/fusekit-holder.app"
  log "building fusekit-holder.app (cgo, -tags fuse) — BUILD ONLY, never run on this host"
  mkdir -p "$app/Contents/MacOS"
  (cd "$REPO_ROOT" && CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -trimpath -tags fuse -ldflags "$ldflags" -o "$app/Contents/MacOS/fusekit-holder" ./cmd/holder)

  # Mirror the release workflow's Info.plist (arm64-only, ad-hoc signed: the
  # guest runs unquarantined local builds, so no notarization is needed).
  cat >"$app/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleIdentifier</key><string>com.yasyf.fusekit-holder</string>
  <key>CFBundleName</key><string>fusekit-holder</string>
  <key>CFBundleExecutable</key><string>fusekit-holder</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>$build_version</string>
  <key>CFBundleVersion</key><string>$(git -C "$REPO_ROOT" rev-list --count HEAD)</string>
  <key>LSBackgroundOnly</key><true/>
  <key>LSMinimumSystemVersion</key><string>12.0</string>
</dict></plist>
PLIST
  codesign --force -s - "$app/Contents/MacOS/fusekit-holder"
  codesign --force -s - "$app"
  printf '%s\n' "$rev" >"$stage/BUILD_REV"

  # A holder already serving DefaultHolderSocket would survive the install and
  # keep serving the OLD build — kill it so the next mount spawns this one.
  log "stopping any running guest holder"
  vm_ssh "pkill -x fusekit-holder 2>/dev/null || true"

  log "installing into the guest: $VMCTL_GUEST_DIR and $VMCTL_GUEST_HOLDER_APP"
  # shellcheck disable=SC2029 # host-side path expansion is intended
  vm_ssh "rm -rf '$VMCTL_GUEST_DIR' '$VMCTL_GUEST_HOLDER_APP' && mkdir -p '$VMCTL_GUEST_DIR'"
  # tar over ssh preserves the .app structure and permissions in one trip.
  # shellcheck disable=SC2029
  tar -C "$stage" -cf - . | vm_ssh "tar -xf - -C '$VMCTL_GUEST_DIR'"
  # /Applications is admin-group writable on macOS; no sudo needed.
  # shellcheck disable=SC2029
  vm_ssh "mv '$VMCTL_GUEST_DIR/fusekit-holder.app' '$VMCTL_GUEST_HOLDER_APP'"
  printf '%s\n' "$rev" >"$VM_STATE_DIR/build-rev"
  printf '%s\n' "$build_serial" >"$VM_STATE_DIR/build-serial"

  log "guest selftest: fuse mount + TCC end-to-end"
  # shellcheck disable=SC2029
  vm_ssh "'$VMCTL_GUEST_VMSTRESS' selftest" ||
    die "vmstress selftest failed — a TCC denial means the Network Volumes grant is missing (README: TCC section)"
  log "pushed BUILD_REV=$rev"
}

main "$@"
