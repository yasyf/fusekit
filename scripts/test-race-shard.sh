#!/usr/bin/env bash
set -euo pipefail

if (( $# != 3 )); then
  echo "usage: $0 <package> <shard> <shards>" >&2
  exit 2
fi

package="$1"
shard="$2"
shards="$3"
if [[ ! "$shard" =~ ^[0-9]+$ || ! "$shards" =~ ^[1-9][0-9]*$ ]] ||
  (( shard >= shards )); then
  echo "invalid shard $shard/$shards" >&2
  exit 2
fi

test_list="$(
  ./scripts/test.sh -race -run '^$' -list '^(Test|Example|Fuzz)' "$package"
)"
selected_tests="$(
  awk 'NF == 1 && /^(Test|Example|Fuzz)/ { print }' <<< "$test_list" |
    LC_ALL=C sort
)"
[[ -n "$selected_tests" ]]
mapfile -t tests <<< "$selected_tests"

selected=()
for index in "${!tests[@]}"; do
  if (( index % shards == shard )); then
    selected+=("${tests[$index]}")
  fi
done
(( ${#selected[@]} > 0 ))

printf '%s\n' "${selected[@]}"
regex="$(IFS='|'; printf '^(%s)$' "${selected[*]}")"
exec ./scripts/test.sh -race -count=1 -timeout="${TEST_SHARD_TIMEOUT:-180s}" \
  -run "$regex" "$package"
