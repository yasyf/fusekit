#!/usr/bin/env bash

set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail() {
  echo "release-check: $*" >&2
  exit 1
}

require_source_tree() {
  [[ "$(sed -n 's/^module //p' go.mod)" == "github.com/yasyf/fusekit" ]] ||
    fail "go.mod must declare the v1 module path github.com/yasyf/fusekit"
  grep -Fq 'name: "fusekit"' Package.swift || fail "Package.swift must declare package fusekit"
  grep -Fq '.library(name: "FuseKit"' Package.swift || fail "Package.swift must export the FuseKit library product"
  [[ "$(grep -Ec '^[[:space:]]*\.library\(' Package.swift)" == "1" ]] ||
    fail "Package.swift must export exactly one library product"

  local residue
  local -a forbidden=(
    cmd/holder
    .github/cask
    .goreleaser.yml
    .goreleaser.yaml
    mountd
    mountset
    holderfs
    overlay
    fileproviderd
    live
    vmstress
  )
  for residue in "${forbidden[@]}"; do
    if [[ -n "$(git ls-files -- "$residue" "$residue/**")" ]]; then
      fail "legacy release residue remains tracked at $residue"
    fi
  done
  if grep -Eq 'cmd/holder|fusekit-holder|sign-notarize|render-formula|MACOS_(SIGN|NOTARY)|HOMEBREW_TAP_TOKEN' \
    .github/workflows/release.yml; then
    fail "FuseKit releases must remain source-only; signing and app packaging belong to consumers"
  fi
}

require_released_dependencies() {
  grep -Eq '^[[:space:]]*github\.com/yasyf/daemonkit v0\.2\.0$' go.mod ||
    fail "go.mod must pin the published daemonkit v0.2.0 tag before FuseKit is tagged"
  grep -Fq '.package(url: "https://github.com/yasyf/daemonkit.git", exact: "0.2.0")' Package.swift ||
    fail "Package.swift must pin the published daemonkit 0.2.0 tag before FuseKit is tagged"
  grep -Fq '"version" : "0.2.0"' Package.resolved ||
    fail "Package.resolved must resolve the published daemonkit 0.2.0 tag"
  if grep -Eq '^(replace|exclude)[[:space:]]' go.mod; then
    fail "release go.mod cannot contain replace or exclude directives"
  fi
}

require_release_tag() {
  local tag="$1"
  [[ "$tag" =~ ^v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] ||
    fail "tag must be a stable v1 semantic version"
  local version="${tag#v}"
  local latest
  latest="$(awk '/^## \[[0-9]+\.[0-9]+\.[0-9]+\] - / {sub(/^## \[/, ""); sub(/\].*$/, ""); print; exit}' CHANGELOG.md)"
  [[ "$latest" == "$version" ]] || fail "tag $tag does not match latest changelog release $latest"

  local heading_count
  heading_count="$(grep -Ec "^## \\[$version\\] - [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md)"
  [[ "$heading_count" == "1" ]] || fail "CHANGELOG.md must contain one dated $version release heading"
  awk '
    /^## \[Unreleased\]$/ {unreleased = 1; next}
    unreleased && /^## \[/ {exit}
    unreleased && NF {exit 1}
  ' CHANGELOG.md || fail "the Unreleased section must be empty when cutting $tag"
  grep -Fqx "[Unreleased]: https://github.com/yasyf/fusekit/compare/$tag...HEAD" CHANGELOG.md ||
    fail "Unreleased comparison link must start at $tag"
  grep -Eq "^\[$version\]: https://github.com/yasyf/fusekit/(compare/[^ ]+\.\.\.$tag|releases/tag/$tag)$" CHANGELOG.md ||
    fail "CHANGELOG.md is missing the exact $tag release link"
  require_released_dependencies
}

write_notes() {
  local tag="$1"
  local output="$2"
  local version="${tag#v}"
  awk -v heading="## [$version] - " '
    index($0, heading) == 1 {inside = 1; next}
    inside && /^## \[/ {exit}
    inside {print}
  ' CHANGELOG.md >"$output"
  [[ -s "$output" ]] || fail "release notes for $tag are empty"
}

require_source_tree

case "${1:-}" in
  --tree)
    [[ "$#" == 1 ]] || fail "usage: release-check.sh --tree"
    ;;
  --notes)
    [[ "$#" == 3 ]] || fail "usage: release-check.sh --notes OUTPUT v1.X.Y"
    require_release_tag "$3"
    write_notes "$3" "$2"
    ;;
  "")
    fail "usage: release-check.sh --tree | [--notes OUTPUT] v1.X.Y"
    ;;
  *)
    [[ "$#" == 1 ]] || fail "usage: release-check.sh v1.X.Y"
    require_release_tag "$1"
    ;;
esac
