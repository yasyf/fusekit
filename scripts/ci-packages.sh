#!/usr/bin/env bash
# Print one half of the CI test-host partition.
#
#   scripts/ci-packages.sh linux-tested   # the linux portable job's test set
#   scripts/ci-packages.sh macos-tested   # the macOS shard job's test set
#
# Test-host routing is NOT daemonkit-coupling. ci/portable.txt is a BUILD
# invariant — the packages that compile and vet without daemonkit, gated by
# scripts/portable-gate.sh. Whether a portable package's TESTS run on linux is a
# separate question: a package can be genuinely daemonkit-free yet have a suite
# too slow for the linux smoke job or one that only builds/means anything on
# darwin. Those are listed in ci/macos-tested.txt and their TEST execution routes
# to the macOS shard job, while portable-gate.sh still proves they BUILD+VET on
# linux. Conflating "free" with "linux-tested" is the bug this split avoids.
#
#   linux-tested = portable.txt  MINUS  macos-tested.txt
#   macos-tested = everything else (the daemonkit-coupled packages, plus the
#                  portable-but-macOS-tested ones)
#
# Both halves partition every package `go list ./...` reports, so nothing falls
# out of CI. A package added to the module joins macos-tested by default;
# portable-gate.sh is what lets it move to the linux half, and ci/macos-tested.txt
# is what pulls a portable-but-heavy/darwin-leaning package back to macOS.
#
# macOS ships bash 3.2, so nothing here may use mapfile or any other bash 4
# builtin.

set -euo pipefail

half="${1:-}"
case "$half" in
  linux-tested | macos-tested) ;;
  *)
    echo "usage: ci-packages.sh <linux-tested|macos-tested>" >&2
    exit 2
    ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# grep exits 1 on a manifest with no entries, a legal state.
portable="$(grep -Ev '^[[:space:]]*(#|$)' ci/portable.txt || (($? == 1)))"
macos_only="$(grep -Ev '^[[:space:]]*(#|$)' ci/macos-tested.txt || (($? == 1)))"

module="$(go list -m)"
# Enumerated under GOOS=darwin: a darwin-tagged package drops out of
# `go list ./...` entirely under any other GOOS, and the linux half of this
# partition is consumed on a linux runner.
packages="$(GOOS=darwin GOARCH=arm64 go list ./...)"

while read -r pkg; do
  dir="${pkg#"$module"}"
  dir="${dir#/}"
  key="${dir:-.}"
  if grep -qxF "$key" <<<"$portable" && ! grep -qxF "$key" <<<"$macos_only"; then
    found=linux-tested
  else
    found=macos-tested
  fi
  if [[ "$found" == "$half" ]]; then
    echo "./${dir}"
  fi
done <<<"$packages"
