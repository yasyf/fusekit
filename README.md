# ![fusekit](docs/assets/readme-banner.webp)

FuseKit is a revisioned filesystem runtime for signed macOS applications. One
catalog owns object identity, namespace transactions, immutable content
snapshots, and change history. Mount and File Provider are two presentations of
that same state.

[![CI](https://github.com/yasyf/fusekit/actions/workflows/ci.yml/badge.svg)](https://github.com/yasyf/fusekit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/yasyf/fusekit)](https://github.com/yasyf/fusekit/releases)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm--Noncommercial--1.0.0-blue)](LICENSE)

## Invariants

- Object IDs are opaque and never contain a path, storage role, or content
  classification.
- A replace is one catalog transaction: the source keeps its ID and the target
  becomes one tombstone.
- Open handles read the exact snapshot they opened, even after replacement.
- `PrepareTenant` coalesces all waiters for one tenant revision. Account or
  product locks never span filesystem work.
- One signed consumer executable owns one native mount root. The unsigned Go
  daemon never owns an App Group endpoint or protected filesystem access.
- File Provider enumerates immutable snapshot pages and revision deltas. It
  never reconstructs the root to answer `enumerateChanges`.
- Every wire session uses daemonkit framing, exact build equality, cancellation,
  bounded queues, and authenticated peer identity. There is no feature
  negotiation or compatibility decoder.
- Private source-publisher and tenant-owner traffic is authorized by product
  policy after daemonkit's same-UID socket floor. Broker and native traffic
  additionally requires the holder plan's exact signed-app requirement.

## Install

Go consumers use the runtime and catalog packages:

```sh
go get github.com/yasyf/fusekit@latest
```

Signed apps and File Provider extensions add the Swift package and link the
`FuseKit` library product.

## Tenant model

A tenant generation is a complete immutable contract:

```go
spec := fusekit.TenantSpec{
    OwnerID:          fusekit.OwnerID("com.example.product"),
    ID:               catalog.TenantID("account-instance-42"),
    PresentationRoot: "/Users/me/Library/Application Support/Example/tenants/42",
    Backing:          fusekit.BackingSpec{Root: "/Users/me/.example/accounts/42"},
    Content:          fusekit.ContentSource{ID: "example-config"},
    Traits: fusekit.TenantTraits{
        Access:          fusekit.ReadWrite,
        CaseSensitivity: catalog.CaseInsensitive,
        Presentations:   fusekit.PresentMount | fusekit.PresentFileProvider,
    },
    Generation: 1,
}
```

Provision, replace, and remove operations are generation-fenced. Calling
`PrepareTenant(ctx, tenant, revision)` converges catalog mutations,
materialization, verification, and mount lifecycle outside product bookkeeping
locks. Disposable daemonkit workers contain context-unaware filesystem calls;
a timed-out worker is terminated, reaped, and cannot retain its semantic lane.

Each source publication assigns an opaque root key per tenant. Before an
external namespace mutation starts, the catalog durably resolves the affected
object, target, and parent to `SourceLocator` values containing the exact
source authority, opaque key, and causal revision. `SourceMutationPlanner`
receives those locators plus tenant ID and generation, never a backing path or
catalog handle. A create reserves the authority key returned by product policy
before its disposable worker starts, so replay and a subsequent atomic replace
retain one source identity.

## Signed holder runtime

`holder.Runtime` composes the daemon listener, SQLite catalog, tenant actors,
disposable workers, exact transport, and one native mount root. The consumer
supplies product policy through `tenant.SourceMutationPlanner`,
`mountservice.Authorizer`, and `catalogservice.Authorizer`.

`holder.NewPlan` validates the consumer-owned app path, bundle and signing
identities, Team ID, entitlements, and private runtime directory. The resulting
`holder.Plan` derives the exact executable, runtime paths, peer requirement, and
daemonkit KeepAlive service; `holder.Config` requires that plan. The same signed
executable handles the exact child mode before starting its normal UI or daemon
entry point:

```go
config, child, err := mountmux.ParseNativeChildArguments(os.Args[1:])
if err != nil {
    return err
}
if child {
    return mountmux.RunNativeChild(ctx, config)
}
```

The parent records the child before execution, then accepts readiness only from
the same daemonkit session and exact PID, process start time, and boot identity.
Session loss or deadline expiry stops and reaps the child before the native
operation lane is released.

## File Provider and TCC

The Swift runtime supplies generic `NSFileProviderReplicatedExtension`
enumeration, lookup, content fetch, mutation, domain lifecycle, and convergence
signaling. A consumer extension subclasses `CatalogReplicatedExtension` and
provides only its domain-to-runtime binding.

Protected traffic has one fixed topology:

```text
File Provider extension
        | App Group socket
        v
signed consumer app broker
        | persistent outbound daemonkit session
        v
0600 product daemon socket
```

`CatalogBroker` resolves and binds the App Group socket in the signed app. It
pins the extension Team ID, signing identifier, entitlement, and hardened
runtime before forwarding traffic. The Go daemon neither resolves nor traverses
the Group Container.

## Packages

| Package | Responsibility |
| --- | --- |
| `fusekit` | Stable tenant API aliases |
| `catalog` | SQLite WAL object catalog, transactions, snapshots, changes, interests, and convergence state |
| `tenant` | Per-tenant actors, generation leases, preparation coalescing, quarantine, and worker recovery |
| `mountmux` | One native mount root, route pins, CatalogFS, and the signed native child |
| `holder` | Composed daemonkit-backed process runtime |
| `catalogservice`, `mountservice` | Exact persistent-session filesystem protocols |
| `convergence`, `causal` | Demand-aware notification targeting and causal change identity |
| Swift `FuseKit` | File Provider runtime, domain controller, and signed App Group broker |
| `fuset` | The small set of FUSE-T install/runtime facts used by the signed child |

## Hard cut

FuseKit v1.6 intentionally has no reader or adapter for the previous mountd,
content bridge, holderfs, overlay, File Provider control, journal, lease, or
feature-negotiated protocols. Consumers migrate source-of-truth data and rebuild
derived runtime state. Old clients fail the exact handshake before mutation.

## Verify

```sh
scripts/test.sh -race -count=1 ./...
go vet ./...
CGO_ENABLED=0 go build ./...
swift build
swift test
```

Fuse-tag compile checks run on CI with FUSE-T/libfuse installed. Live mount,
process-kill, File Provider, and TCC tests run only in isolated macOS VMs.
