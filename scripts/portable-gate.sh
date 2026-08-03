#!/usr/bin/env bash
# Gate the module's daemonkit-free subset against ci/portable.txt.
#
#   scripts/portable-gate.sh            # check; exits non-zero on drift either way
#   scripts/portable-gate.sh --write    # record an undeclared gain in the manifest
#
# FuseKit's core is macOS-only: catalog reaches for daemonkit/proc, and every
# package downstream of it inherits that. daemonkit does not compile off darwin,
# so the packages listed here are exactly the ones CI may still build, vet, and
# test on a linux runner. The manifest is the declared boundary, and the gate
# fails in both directions: a declared package that has gained a daemonkit edge
# is a regression, and an undeclared package that is free of one is a boundary
# nobody reviewed.
#
# Membership is decided by imports, not by whether a linux build happens to
# succeed today. A daemonkit release that still compiles for linux would let
# every package pass a build-only probe, and the manifest would then collapse
# the moment daemonkit went darwin-only. The import test gives the same answer
# before and after that cut. The linux build and vet run as a second, weaker
# check that the declared subset is genuinely portable.
#
# Test dependencies count. These packages are tested on the linux runner, so a
# package whose test binary reaches daemonkit is coupled even when its library
# is not.
#
# The two directions do not share a remedy. --write records a gain; it refuses
# while anything is regressed, because regenerating the manifest over a
# regression drops the regressed package from it and launders a broken boundary
# into an approved one in a single command. A regression is fixed in the
# package, or given up in a separate change whose diff says so.
#
# The package set is enumerated under GOOS=darwin: a darwin-tagged package drops
# out of `go list ./...` entirely under any other GOOS, and this gate runs on a
# linux runner.

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

manifest="ci/portable.txt"
coupled_module="github.com/yasyf/daemonkit"

usage() {
  echo "usage: portable-gate.sh [--write]" >&2
  exit 2
}

# free prints the module-relative directory of every package that imports no
# daemonkit package — its own test binary included — and that builds and vets
# for linux, sorted. `-o /dev/null` keeps a main package from dropping a binary
# into the tree.
#
# The dependency list is captured before it is matched, never piped into a
# `grep -q`: grep exits at the first match and the SIGPIPE that kills `go list`
# makes `pipefail` fail the whole pipeline, so a matched — that is, coupled —
# package reads as an unmatched one whenever the write loses that race. A build
# or vet failure is swallowed on purpose, since most of the module is coupled
# and fails under GOOS=linux by design, but a `go list` that cannot answer is a
# broken gate rather than a free package, and stops the run.
free() {
  local module packages pkg dir deps
  module="$(go list -m)"
  packages="$(GOOS=darwin GOARCH=arm64 go list ./...)"
  while read -r pkg; do
    dir="${pkg#"$module"}"
    dir="${dir#/}"
    deps="$(GOOS=darwin GOARCH=arm64 go list -deps -test "$pkg")"
    grep -qE "^${coupled_module}(/|\$)" <<<"$deps" && continue
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null "$pkg" >/dev/null 2>&1 || continue
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go vet "$pkg" >/dev/null 2>&1 || continue
    echo "${dir:-.}"
  done <<<"$packages" | LC_ALL=C sort
}

# declared reads the manifest. grep exits 1 on a manifest with no entries, which
# is a legal state — bootstrapping the gate, or giving up the last package — so
# only a real grep failure propagates.
declared() {
  local status=0 lines
  lines="$(grep -Ev '^[[:space:]]*(#|$)' "$manifest")" || status=$?
  ((status <= 1)) || return "$status"
  [[ -n "$lines" ]] || return 0
  printf '%s\n' "$lines" | LC_ALL=C sort
}

section() {
  local label="$1" sign="$2" lines="$3"
  [[ -n "$lines" ]] || return 0
  printf 'portable gate: %s (%d):\n' "$label" "$(printf '%s\n' "$lines" | wc -l | tr -d ' ')" >&2
  printf '%s\n' "$lines" | sed "s/^/  $sign /" >&2
}

# explain names the daemonkit edge each regressed package gained, then re-runs
# its linux build and vet with the output shown. Discovery has to swallow that
# output — most of the module is daemonkit-coupled by design and fails there on
# purpose — so without this a regression reaches CI as a bare package name with
# no diagnostic and no way in.
explain() {
  local module dir pkg
  module="$(go list -m)"
  while read -r dir; do
    if [[ "$dir" == "." ]]; then
      pkg="$module"
    else
      pkg="$module/$dir"
    fi
    printf 'portable gate: %s reaches daemonkit through:\n' "$pkg" >&2
    GOOS=darwin GOARCH=arm64 go list -deps -test "$pkg" 2>/dev/null |
      grep -E "^${coupled_module}(/|\$)" | sed 's/^/  /' >&2 || true
    printf 'portable gate: %s under GOOS=linux:\n' "$pkg" >&2
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /dev/null "$pkg" >&2 || true
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go vet "$pkg" >&2 || true
    printf "  reproduce: go list -deps -test %s | grep %s\n" "$pkg" "$coupled_module" >&2
  done <<<"$1"
}

case "${1:-}" in
  --write) [[ "$#" == 1 ]] || usage ;;
  "") ;;
  *) usage ;;
esac

got="$(mktemp)"
want="$(mktemp)"
trap 'rm -f "$got" "$want"' EXIT
free >"$got"
declared >"$want"

regressed="$(comm -13 "$got" "$want")"
undeclared="$(comm -23 "$got" "$want")"

if [[ -n "$regressed" ]]; then
  section "declared daemonkit-free, but no longer is (or no longer builds for linux)" "-" "$regressed"
  section "daemonkit-free, but not declared" "+" "$undeclared"
  explain "$regressed"
  printf 'portable gate: a regression is fixed in the package, not recorded — --write refuses while one stands. Giving the package up is a separate change that drops its line from %s and says why.\n' \
    "$manifest" >&2
  exit 1
fi

if [[ "${1:-}" == "--write" ]]; then
  cp "$got" "$manifest"
  printf 'portable gate: %d packages recorded in %s\n' \
    "$(wc -l <"$manifest" | tr -d ' ')" "$manifest" >&2
  exit 0
fi

if [[ -n "$undeclared" ]]; then
  section "daemonkit-free, but not declared" "+" "$undeclared"
  echo "portable gate: an undeclared gain needs review. Once approved: scripts/portable-gate.sh --write" >&2
  exit 1
fi

printf 'portable gate: %d packages are daemonkit-free and build for linux, %s matches\n' \
  "$(wc -l <"$got" | tr -d ' ')" "$manifest"
