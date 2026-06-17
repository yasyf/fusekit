# fusekit Style Guide

The concrete style rules for this repository.

## Core Principles

1. **Fail fast, fail loud.** No defensive coding: no fallbacks, shims, or
   backwards-compat layers, and no guards against impossible states. No sentinel
   values, no silent defaults. If unused, delete it. Crash on the unexpected.
2. **Make invalid states unrepresentable.** Branded/newtype primitives, immutable
   data structures, required fields over optionals.
3. **Minimal changes.** Stay within scope. Make the test pass, then stop. Improve
   only the code you touch.
4. **Match surrounding code.** Follow this guide first, then the file you're in,
   then the module. If surrounding code violates this guide, fix it.

## Go rules

- **`gofmt` + `go vet` own formatting and mechanical issues** — never hand-flag them in review; fix only what needs judgment (logic, concurrency, edge cases).
- **Errors wrap once per layer with `%w`**; define sentinels and match with `errors.Is`/`errors.As`. Never log-and-return the same error (one or the other). Keep the fallible call adjacent to its `if err != nil`.

  ```go
  // Good
  if err := h.Unmount(); err != nil {
      return fmt.Errorf("teardown %s: %w", dir, err)
  }
  // Bad — log AND return; caller logs again
  if err := h.Unmount(); err != nil {
      log.Printf("unmount failed: %v", err)
      return err
  }
  ```

- **Sentinel identity is load-bearing.** Consumers alias fusekit's sentinels (`var ErrMountNotLive = fusekit.ErrMountNotLive`); never re-`errors.New` an equivalent — it silently breaks every `errors.Is` across the module boundary.
- **`ctx context.Context` is the first parameter** of any function that blocks, does I/O, or spawns. Every goroutine has a defined exit; never hold a lock across I/O.
- **godoc on every exported symbol.** Inside function bodies, comments only for TODOs, non-obvious workarounds, or load-bearing invariants (e.g. *why* `noattrcache` is forced on darwin) — never to restate the code.
- **Flat over nested.** Early returns; nesting deeper than three is a smell.
- **Build tags & platform splits.** Pure files carry no tags and must build `CGO_ENABLED=0` on every GOOS. The fuse host is `//go:build fuse && cgo` (not darwin-restricted). Platform syscalls live in `_darwin.go`/`_other.go` behind one signature — a darwin-only syscall in a shared file must fail the Linux `-tags fuse` build, not compile by luck.

## Wire-protocol freeze

`mountd`'s protocol is **frozen**: field/op/class JSON strings, one-JSON-per-line framing, the `<socket>.lock` held-for-life, and the timeout ladder are a compatibility contract with already-deployed holders. fusekit's own module version must never appear on the wire — `Response.Version` is fed only by the consumer-injected `Server.Version`. Changes are additive-only; pin both directions with golden-bytes tests.

## Error Handling

Keep error-handling blocks minimal: only the operation that can fail belongs
inside. No catch-all handlers that swallow everything; use dedicated error types.
Read required configuration so a missing key fails at startup. No sentinel return
values; raise, or return a typed result.

## Code Organization

Order each module: imports, constants, type aliases, helpers, classes, then
functions. Constants sit immediately after imports, before any class or function.
Use the language's export-control mechanism instead of underscore/naming
conventions to hide internals.

## Comments & Docstrings

Code documents itself through names, types, and organization. No comments except
TODOs, non-obvious workarounds, or disabled code. Document the public API only;
a doc comment that restates the signature is clutter to delete.

## Testing

Write strict assertions against specific expected values; a test that can't fail
uncovers nothing. Mock the boundaries your code talks to, such as the network,
filesystem, and clock, and leave the function under test real. A database (or any
stateful service) is not a mock boundary: when a test needs one, start a real
ephemeral instance with testcontainers rather than mocking the driver or using an
in-memory fake. Parameterize repeated test bodies, giving each case a descriptive
id and its own expected values.
