#!/usr/bin/env bash
# Print one half of the CI package partition.
#
#   scripts/ci-packages.sh free       # the linux runner's packages
#   scripts/ci-packages.sh coupled    # the macOS runners' packages
#
# Both halves are derived from the single manifest that scripts/portable-gate.sh
# validates, so every package `go list ./...` reports lands in exactly one of
# them. A package added to the module joins the coupled half by default and is
# tested on macOS; the gate is what decides whether it may move to the other
# half. Nothing can fall out of CI by being named in neither list.
#
# macOS ships bash 3.2, so nothing here may use mapfile or any other bash 4
# builtin.

set -euo pipefail

half="${1:-}"
case "$half" in
  free | coupled) ;;
  *)
    echo "usage: ci-packages.sh <free|coupled>" >&2
    exit 2
    ;;
esac

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

# grep exits 1 on a manifest with no entries, a legal state.
declared="$(grep -Ev '^[[:space:]]*(#|$)' ci/portable.txt || (($? == 1)))"

module="$(go list -m)"
# Enumerated under GOOS=darwin: a darwin-tagged package drops out of
# `go list ./...` entirely under any other GOOS, and the free half of this
# partition is consumed on a linux runner.
packages="$(GOOS=darwin GOARCH=arm64 go list ./...)"

while read -r pkg; do
  dir="${pkg#"$module"}"
  dir="${dir#/}"
  if grep -qxF "${dir:-.}" <<<"$declared"; then
    found=free
  else
    found=coupled
  fi
  if [[ "$found" == "$half" ]]; then
    echo "./${dir}"
  fi
done <<<"$packages"
