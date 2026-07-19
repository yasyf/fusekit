## Style

**Comments are terse and used sparingly.** Exported symbols and packages receive
godoc that starts with the identifier. Body comments are only for TODOs,
non-obvious workarounds, and load-bearing invariants.

@STYLEGUIDE.md

## General Rules

**No compatibility code.** Old protocol readers, feature negotiation, state
migrations, aliases for deleted APIs, and silent fallbacks are bugs. Consumers
migrate manually at the hard cut.

**Keep ownership exact.** daemonkit owns lifecycle, listeners, transport,
process identity, reaping, trust primitives, and App Group resolution. FuseKit
owns object identity, catalog revisions, tenant lanes, materialization,
presentations, and convergence. Product policy stays in consumers.

**Search before writing.** Query with `ccx code search` or `ccx code symbol`
before creating a helper. Reuse current catalog, tenant, and daemonkit seams.

**Observe, don't infer.** Inspect fixtures, runtime state, and exact protocol
bytes before reasoning. Reproduce the smallest failure before editing.

**Verify before asserting.** Do not report success until the relevant test,
build, runtime probe, or residue scan has completed.

**Testing.** Always use `scripts/test.sh`. The per-commit gate is
`scripts/test.sh -race -count=1 ./...`, `go vet ./...`, and
`CGO_ENABLED=0 go build ./...`. Protocol changes run every generator in check
mode, `swift build`, and `swift test`. Fuse-tag compilation needs FUSE-T or
libfuse; live mount, kill/reap, File Provider, and TCC tests run only in isolated
VMs.

**Writing docs.** Keep examples on the current hard API only. Do not document
removed behavior as an alternative or migration path.
