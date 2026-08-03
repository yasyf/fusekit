#!/usr/bin/env bash
# Run one shard of a race suite spread across several packages.
#
#   scripts/test-race-shard.sh <shard> <shards> <package>...
#
# Tests are enumerated per package, paired with the package they came from,
# globally sorted, and dealt round-robin. A shard's cost therefore tracks the
# whole suite rather than one package: FuseKit's daemonkit-coupled tree is
# dominated by ./catalog, so dealing whole packages to shards would pin the
# slowest shard to that one package's runtime.
#
# macOS ships bash 3.2, so nothing here may use mapfile or any other bash 4
# builtin — this runs on the macOS runners.

set -euo pipefail

if (($# < 3)); then
  echo "usage: $0 <shard> <shards> <package>..." >&2
  exit 2
fi

shard="$1"
shards="$2"
shift 2

if [[ ! "$shard" =~ ^[0-9]+$ || ! "$shards" =~ ^[1-9][0-9]*$ ]] ||
  ((shard >= shards)); then
  echo "invalid shard $shard/$shards" >&2
  exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

pairs=()
for package in "$@"; do
  while IFS= read -r name; do
    pairs+=("${package}"$'\t'"${name}")
  done < <(
    ./scripts/test.sh -race -run '^$' -list '^(Test|Example|Fuzz)' "$package" |
      awk 'NF == 1 && /^(Test|Example|Fuzz)/ { print }'
  )
done
((${#pairs[@]} > 0))

sorted=()
while IFS= read -r pair; do
  sorted+=("$pair")
done < <(printf '%s\n' "${pairs[@]}" | LC_ALL=C sort)

selected=()
for index in "${!sorted[@]}"; do
  if ((index % shards == shard)); then
    selected+=("${sorted[$index]}")
  fi
done
((${#selected[@]} > 0))
printf 'shard %d/%d: %d of %d tests\n' \
  "$shard" "$shards" "${#selected[@]}" "${#sorted[@]}" >&2
printf '%s\n' "${selected[@]}" >&2

# One `go test` per package holding selected tests. A failure is recorded rather
# than thrown, so a red package still lets the rest of the shard report.
status=0
for package in "$@"; do
  tests=()
  for pair in "${selected[@]}"; do
    [[ "${pair%%$'\t'*}" == "$package" ]] || continue
    tests+=("${pair#*$'\t'}")
  done
  ((${#tests[@]} > 0)) || continue
  regex="$(
    IFS='|'
    printf '^(%s)$' "${tests[*]}"
  )"
  ./scripts/test.sh -race -count=1 -timeout="${TEST_SHARD_TIMEOUT:-600s}" \
    -run "$regex" "$package" || status=1
done
exit "$status"
