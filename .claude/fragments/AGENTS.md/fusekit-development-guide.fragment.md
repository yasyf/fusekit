# fusekit Development Guide

Revisioned catalog, tenant convergence, native mount, and File Provider runtime.

## Repository structure

```text
fusekit/
├── catalog/            # SQLite WAL object identity, namespace, snapshots, changes, state
├── tenant/             # generation-fenced actors and PrepareTenant convergence
├── mountmux/           # one native root, route pins, CatalogFS, signed child callbacks
├── holder/             # composed daemonkit process runtime
├── catalogservice/     # exact persistent-session catalog protocol
├── mountservice/       # exact persistent-session tenant/native protocol
├── convergence/        # demand-aware targeting, coalescing, acknowledgements, quarantine
├── Sources/FuseKit/    # Swift File Provider runtime, domain controller, signed broker
├── fuset/              # low-level FUSE-T facts used by the signed child
├── docs/reports/       # rewrite evidence ledger
└── .github/workflows/  # Go, fuse-tag, Swift, schema, guide, and release gates
```

The root package exports stable tenant aliases only. There is no standalone
holder application: every consumer embeds `holder.Runtime` in its own fixed
signed app, and the same executable handles
`mountmux.ParseNativeChildArguments` before normal startup.

The v1.6 line is a hard cut. `mountd`, `mountset`, `MountSpec`, legacy holder and
live probes, content bridge/tree APIs, `holderfs`, overlay selection, old File
Provider control, lease/journal/strike/retirement state, and compatibility wire
surfaces do not return.

## Testing

Run `scripts/test.sh`, never bare `go test` on a real machine. The standard gate
is:

```sh
scripts/test.sh -race -count=1 ./...
go vet ./...
CGO_ENABLED=0 go build ./...
go run ./catalogproto/gen -check
go run ./mountproto/gen -check
go run ./transportproto/gen -check
swift build
swift test
```

Fuse-tag compilation is safe with its provider installed; live mount,
process-kill, File Provider, and TCC exercises are VM-only.
