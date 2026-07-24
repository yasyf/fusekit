#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
subject="$root/scripts/verify-release-tag.sh"
trusted="F3299DE3FE0F6C3CF2B66BFBF7ECDD88A700D73A"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin"

cat >"$scratch/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-} ${2:-}" in
  "cat-file -t")
    [[ "${FAKE_TAG_EXISTS:-1}" == 1 ]] || exit 1
    printf '%s\n' "${FAKE_TAG_TYPE:-tag}"
    ;;
  "verify-tag --raw")
    printf '%s\n' "${FAKE_VERIFY_STATUS:-}" >&2
    exit "${FAKE_VERIFY_EXIT:-0}"
    ;;
  *)
    exit 99
    ;;
esac
EOF
cat >"$scratch/bin/gpg" <<'EOF'
#!/usr/bin/env bash
exit "${FAKE_IMPORT_EXIT:-0}"
EOF
chmod +x "$scratch/bin/git" "$scratch/bin/gpg"

run_check() {
  PATH="$scratch/bin:$PATH" "$subject" v1.13.0 2>&1
}

expect_failure() {
  local expected="$1"
  shift
  local output
  if output="$(env "$@" bash -c 'run_check' 2>&1)"; then
    echo "verify-release-tag-test: expected failure containing $expected" >&2
    exit 1
  fi
  grep -Fq "$expected" <<<"$output" || {
    echo "verify-release-tag-test: output does not contain $expected: $output" >&2
    exit 1
  }
}

export -f run_check
export PATH subject scratch

expect_failure "does not exist" FAKE_TAG_EXISTS=0
expect_failure "must be annotated" FAKE_TAG_TYPE=commit
expect_failure "signature verification failed" FAKE_VERIFY_EXIT=1
expect_failure "is not signed by trusted key" \
  FAKE_VERIFY_STATUS="[GNUPG:] VALIDSIG 1111111111111111111111111111111111111111 0 0 0 0 0 0 0 0 2222222222222222222222222222222222222222"

FAKE_VERIFY_STATUS="[GNUPG:] VALIDSIG $trusted 0 0 0 0 0 0 0 0 $trusted" run_check >/dev/null
FAKE_VERIFY_STATUS="[GNUPG:] VALIDSIG A2CCA0EF125FF155ABB22068AE62FF59AFC36043 0 0 0 0 0 0 0 0 $trusted" run_check >/dev/null
