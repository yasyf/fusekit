#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
trusted_key="$root/.github/keys/release-signing-key.asc"
trusted_fingerprint="F3299DE3FE0F6C3CF2B66BFBF7ECDD88A700D73A"
tag="${1:-}"

fail() {
  echo "verify-release-tag: $*" >&2
  exit 1
}

[[ "$#" == 1 && "$tag" =~ ^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
  fail "usage: verify-release-tag.sh v1.X.Y"
[[ -s "$trusted_key" ]] || fail "trusted release public key is missing"

tag_type="$(git cat-file -t "refs/tags/$tag" 2>/dev/null)" ||
  fail "tag $tag does not exist"
[[ "$tag_type" == "tag" ]] || fail "tag $tag must be annotated"

keyring="$(mktemp -d)"
trap 'rm -rf "$keyring"' EXIT
chmod 700 "$keyring"
gpg --batch --homedir "$keyring" --import "$trusted_key" >/dev/null 2>&1 ||
  fail "trusted release public key cannot be imported"

status="$(GNUPGHOME="$keyring" git verify-tag --raw "$tag" 2>&1)" ||
  fail "tag $tag signature verification failed"
valid_signatures="$(
  awk '$1 == "[GNUPG:]" && $2 == "VALIDSIG" { print $3 " " $NF }' <<<"$status"
)"
signature_count="$(grep -c . <<<"$valid_signatures" || true)"
[[ "$signature_count" == 1 ]] ||
  fail "tag $tag must contain exactly one valid signature"
read -r signing_fingerprint primary_fingerprint <<<"$valid_signatures"
if [[ "$signing_fingerprint" != "$trusted_fingerprint" &&
      "$primary_fingerprint" != "$trusted_fingerprint" ]]; then
  fail "tag $tag is not signed by trusted key $trusted_fingerprint"
fi
