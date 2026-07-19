# fusekit Style Guide

The concrete style rules for this repository.

## Core principles

1. **Fail fast, fail loud.** No fallbacks, shims, compatibility layers, silent
   defaults, or guards for impossible states. Delete unused surfaces.
2. **Make invalid states unrepresentable.** Use branded primitives, immutable
   generation contracts, closed enums, and required fields.
3. **One owner per concern.** daemonkit owns lifecycle and transport; FuseKit
   owns filesystem identity and convergence; consumers own product policy and
   their signed application deployment identity.
4. **Match surrounding code.** Follow this guide, then the package being
   changed. Repair violations in code already in scope.

## Go rules

- `gofmt` and `go vet` own formatting and mechanical checks.
- Wrap an error once per layer with `%w`. Match exported sentinels with
  `errors.Is` and typed failures with `errors.As`; never log and return the same
  failure.
- `ctx context.Context` is the first parameter of every operation that blocks,
  performs I/O, or spawns. Every goroutine and process has a defined exit.
- Locks protect in-memory state only. No lock spans filesystem, socket,
  subprocess, Keychain, or File Provider I/O.
- Document every exported symbol. Body comments are reserved for load-bearing
  invariants, non-obvious workarounds, and TODOs.
- Prefer early returns. Nesting deeper than three levels is a design smell.
- Pure files must build with `CGO_ENABLED=0`. Native FUSE callbacks remain
  behind `darwin && cgo && fuse`; platform operations use explicit files and
  build tags.

## Protocols

Transport uses daemonkit's generated length-framed session protocol. Catalog
and mount messages are generated from the Go schemas and share exact build
identities with Swift.

- Exact protocol/build equality is required before admission.
- Unknown fields, operations, enum values, trailing data, and old builds fail
  before mutation.
- Request IDs, cancellation, terminal settlement, streaming, bounded queues,
  and peer identity are part of one session contract.
- There is no feature negotiation, additive skew policy, legacy decoder, or
  one-request-per-connection path.
- A schema change regenerates Go and Swift artifacts and updates golden tests in
  the same commit.

## Filesystem invariants

- Object IDs never encode paths, names, backing locations, or classifications.
- Replace-over-target preserves the source ID and tombstones the target in one
  transaction.
- Open handles pin snapshots until close; compaction respects handles and valid
  anchors.
- File Provider delta enumeration never performs root snapshots, content reads,
  or per-name classification.
- daemonkit transport backpressure never substitutes for FuseKit's tenant and
  mutation lanes.
- Potentially wedging native calls run only in a disposable supervised child.

## Testing

Use `scripts/test.sh`; never invoke bare `go test` on a real host. Every change
runs the race suite, `go vet`, and the pure build. Schema changes also run all
three generators in check mode plus Swift build/test. Fuse-tag compilation runs
with its provider installed. Live mount, kill/reap, File Provider, and TCC tests
run only in isolated VMs.

Tests assert exact values and negative behavior. Use the real SQLite catalog in
a temporary directory. Fake only external process, socket, clock, and platform
boundaries.
