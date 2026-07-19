# Source observation hard-cut design

## Decision

Recursive source observation, its durable cursor, physical-path index, repair
state, source-publication sequencing, and preparation barrier belong to
FuseKit. A consumer supplies only product policy: which source roots exist,
how an observed path maps to logical keys and tenant projections, and how to
produce the effective content for those keys.

For cc-pool, the fixed signed `CCPoolStatus.app` holder runs this FuseKit
runtime. The unsigned account daemon no longer watches `~/.claude`, owns a
source-publication SQLite database, or turns vnode notifications into fleet
work. This keeps filesystem access on the stable signed identity and removes
the daemon-to-every-domain fan-out seam.

This is a hard replacement. There is no compatibility watcher, polling
fallback, old source-state reader, or remote publication adapter retained for
cc-pool.

## Why the current seam cannot route deltas safely

cc-pool's watcher accepts only a `dirtyCause`, not a path-bearing event
(`internal/daemon/events_darwin.go:30#sz22`). Its `claude-dir` subscription
marks every directory wake as structural (`internal/daemon/events_darwin.go:103#z32w`),
while exact file watches separately mark canonical and settings changes.
Directory vnode events neither identify the changed child nor form a recursive,
durable journal. An atomic `settings.json` replacement therefore looks like
both an exact settings change and an unknown structural change, and nested
shared-tree changes cannot be reconstructed from the wake alone.

The dormant partial machinery is useful but is downstream of that missing
fact. `ClaudeSource.Delta` can read a fail-closed selection
(`internal/tenantfs/claude_source.go:154#bxfq`), `SourceDelta` carries a complete
fleet fence (`internal/tenantfs/model.go:129#fy14`), and `PublishDelta` durably
sequences the resulting update (`internal/tenantfs/publisher.go:46#k5v1`). None
of those APIs can convert an ambiguous wake into a proven path set.

FuseKit already has the correct sink: `catalog.SourcePublication` distinguishes
snapshot from predecessor-fenced delta (`catalog/source.go:64#a25g`),
`Catalog.ApplySource` commits source state and convergence outbox together
(`catalog/source.go:95#qv4t`), and the source watermark rejects gaps
(`catalog/source.go:323#pqpe`). The missing layer is the authoritative producer
in front of that sink.

## Ownership

| Layer | Owns |
|---|---|
| daemonkit | Signed process lifecycle, persistent session framing, peer identity, cancellation, bounded queues, and reaping. It has no path, source-key, or tenant semantics. |
| FuseKit | Recursive path event stream, durable cursor/inbox, physical metadata index, opaque source-key allocation, snapshot repair state, source revisions, publication spool, catalog apply, convergence outbox, and the `PrepareTenant` source barrier. |
| cc-pool policy | Claude root declarations, shared/private/synthetic logical-key mapping, merge/split rules, effective bytes and fingerprints, and account-instance-to-tenant projection. It cannot notify domains or choose retries. |
| Signed consumer app | The concrete entitlements and stable executable that hosts the FuseKit observer/runtime. |

Paths are locators in the physical index, never object identifiers. cc-pool may
name a logical role such as `effective-settings` or map an account-private
source object, but FuseKit allocates and retains the opaque source key and
catalog `ObjectID` across rename, replacement, materialization, and
presentation changes.

## New FuseKit source-authority runtime

Add a `sourceauthority` package with these top-level contracts:

```go
type RootSpec struct {
    Authority  causal.SourceAuthorityID
    ID         RootID
    Path       string
    Recursive  bool
    Generation uint64
}

type PathEvent struct {
    Root       RootID
    Relative   string
    Kind       EventKind
    FileID     FileIdentity
    PriorPath  string
}

type EventBatch struct {
    Stream     StreamIdentity
    Predecessor EventCursor
    Cursor     EventCursor
    RootEpoch  RootEpoch
    Events     []PathEvent
}

type Policy interface {
    Roots(context.Context, []tenant.TenantSpec) ([]RootSpec, error)
    PlanDelta(context.Context, IndexView, EventBatch) (DeltaPlan, error)
    PlanSnapshot(context.Context, SnapshotView) (SnapshotPlan, error)
    Materialize(context.Context, MaterializationRequest) (Materialization, error)
}
```

The exact concrete shapes can be generated from these invariants, but the
boundary is fixed:

- `EventBatch` contains physical facts only. It has no account or Claude
  semantics.
- `DeltaPlan` names logical roles, affected tenant generations, exact deletes,
  and exact reads. It cannot carry File Provider domains or notifications.
- `Materialization` returns complete metadata, bytes or link target, and an
  effective fingerprint for every upsert.
- FuseKit turns the policy plan into `catalog.SourcePublication`; policy code
  cannot select a source revision, predecessor, change ID, operation ID, or
  catalog object ID.

### Durable state

Extend the single holder catalog in `catalog/schema.go:17#kfjr` with:

- `source_observer_roots`: authority, opaque root ID, root generation, volume
  identity, stable root identity, and root-set digest;
- `source_observer_streams`: stream identity, last durably received event ID,
  last applied event ID, and state (`snapshot_required` or `incremental`);
- `source_observer_inbox`: ordered immutable event batches and their digest;
- `source_physical_index`: authority/root/relative locator, physical identity,
  kind, metadata fingerprint, content fingerprint, and opaque logical binding;
- `source_publication_spools`: exact staged publication bytes and content-stage
  references until `Catalog.ApplySource` returns its idempotent result.

The existing `source_watermarks` and `source_operations` tables
(`catalog/schema.go:140#xzmg`) remain the only source-revision truth. cc-pool's
`authority_state`, pending spool, manifest, and per-tenant commit mirror are
deleted after the runtime and preparation barrier consume the catalog's own
watermark/applicable-commit view.

### Event acquisition

The Darwin backend uses one file-event FSEvents stream covering the normalized
root set. It is created before the initial scan and records volume/root
identity plus the persistent event ID. The callback performs no reads and no
policy work: it validates/bounds paths, appends an immutable batch to the
durable inbox, and wakes the source actor.

`MustScanSubDirs`, user/kernel drops, wrapped IDs, changed root identity, and
stream/root-set discontinuity are state transitions to `snapshot_required`,
not synthetic path events. The actor is single-writer per source authority and
processes inbox batches in cursor order. Bounded ingestion applies backpressure;
it never discards an event and continues incrementally.

This semantic runtime is composed into `holder.Config` and `holder.Runtime`
(`holder/runtime.go:33#zrg3`, `holder/runtime.go:60#gvzq`). It starts after
catalog/tenant recovery and before daemon admission. During drain it stops new
observer intake, settles every durable inbox/publication record, then closes
before the catalog and daemon listener. It does not create another lifecycle
or transport implementation.

### Incremental transaction

For one continuous event batch:

1. Claim the oldest durable inbox record for the authority.
2. Re-stat only its named paths and required parents against the prior physical
   index. Collapse repeated events to their final state while retaining
   identity evidence for moves.
3. Invoke the consumer `PlanDelta` and reject any plan outside the batch/root/
   fleet fence.
4. Materialize only the plan's exact reads. Stable-probe each opened object and
   compute the effective fingerprint.
5. Suppress unchanged logical values. Allocate opaque source keys only for new
   logical identities; a path or private/computed role transition cannot
   allocate a replacement key.
6. Persist the exact publication spool before dispatch.
7. Apply one predecessor-fenced `catalog.SourceDelta`. Catalog apply, source
   watermark, per-tenant commits, causal change, and convergence outbox remain
   one transaction.
8. Atomically update the physical index, settle the inbox record/publication
   spool, and advance the applied event cursor. A crash before settlement
   replays the same operation ID and exact bytes.

Multiple queued batches may be coalesced only before materialization and only
when their cursor range is continuous. Latest-wins is an optimization over a
proven range, never a substitute for missing events.

### Authoritative snapshot transaction

Snapshot repair is one explicit runtime state, not an incremental error
fallback:

1. Arm/flush the event stream and capture a start fence.
2. Recursively scan declared roots into a new physical index while building
   the policy's complete authority fleet.
3. Flush again. If any event in the fenced interval intersects the scanned
   roots, discard the candidate and retry with a new fence; never publish a
   mixed-time tree.
4. Persist and apply one complete `catalog.SourceSnapshot` covering the exact
   desired tenant fleet.
5. Replace the physical index, settle inbox records covered by the fence, store
   the stream cursor/root epoch, and enter `incremental` state.

Repeated churn is bounded and quarantines the source authority with precise
cursor/root evidence. It does not publish a partial snapshot or silently switch
to polling.

## Exact snapshot-required conditions

A complete snapshot is mandatory only when one of these facts is present:

1. no committed observer baseline exists;
2. the desired tenant/root fleet or any tenant/root generation changed;
3. FSEvents reports `MustScanSubDirs`, `UserDropped`, `KernelDropped`,
   `EventIdsWrapped`, `RootChanged`, or an event ID/predecessor gap;
4. the stream, volume, root identity, or root-set digest differs from the
   durable baseline;
5. the durable physical index or inbox digest fails validation;
6. policy cannot map a proven path without inspecting an unindexed subtree;
7. FuseKit returns `ErrSourceRequiresSnapshot` or the catalog watermark is not
   the runtime's exact predecessor.

`ErrSourceChanged` while reading a named object does not force a snapshot. The
batch remains pending and retries from the same cursor after bounded backoff.
An ordinary transport failure likewise replays its durable publication bytes
before observing newer input.

## `PrepareTenant` barrier

Preparation must no longer trust a source tuple mirrored by the caller. The
current request carries catalog/source revision, change ID, and operation ID
(`catalogproto/messages_gen.go:597#cjd1`), and the adapter prepares that supplied
tuple (`catalogservice/adapters.go:476#d052`). Replace it with a request that
contains only tenant, domain, and expected generation.

The FuseKit preparation adapter then:

1. asks `sourceauthority.Runtime.Barrier(tenant, generation)` to flush the
   observer stream and settle every continuous event through the barrier;
2. reads the tenant's latest applicable commit directly from the catalog;
3. calls the tenant actor and convergence engine with that exact tuple;
4. returns the catalog/domain observation proof.

Concurrent callers coalesce on the same authority cursor and tenant revision.
Cancellation removes only the waiter; it cannot abandon the source actor,
publication spool, tenant lane, or account reservation. This is the source-side
half of eliminating `acct-18 content catch-up: context deadline exceeded`.

## cc-pool migration

Retain and reshape only the Claude policy now represented by
`internal/tenantfs/claude_source.go` and the external-mutation semantics in
`internal/tenantfs/source_mutation.go:52#ekx1`:

- declare canonical `.claude.json`, shared `~/.claude`, and per-account private
  roots from the current desired tenant fleet;
- map exact observed paths to shared, account-private, or synthetic logical
  roles;
- recompute `.claude.json` and `settings.json` only for tenants whose effective
  inputs intersect the event batch;
- preserve Claude merge/split/classification policy and effective fingerprints;
- emit `external_unattributed` for FSEvents observations and retain authenticated
  provider-mutation causal identity for catalog mutations.

Delete:

- `internal/daemon/events_darwin.go`, `events_other.go`, the source dirty queue,
  retry loop, and daemon-owned snapshot coordinator;
- cc-pool `AuthorityStore`, `Publisher`, publication spool, manifest, and
  per-tenant source-commit mirror;
- cc-pool calls to remote `SourceReconcile` and caller-supplied preparation
  tuples;
- the external `catalog.source_reconcile` protocol/client/server path once all
  consumers embed the source-authority runtime. `Catalog.ApplySource` remains
  an internal FuseKit transaction, not a consumer wire API.

Account provisioning/replacement/removal remains the only fleet-control input.
The source runtime observes the durable desired-tenant generation and performs
the required snapshot transition. cc-pool never directly signals a File
Provider domain.

## Verification

### Deterministic source-runtime tests

- Nested create, write, delete, and cross-directory rename yield exact path
  batches and no root traversal.
- Atomic temp-write/rename-over-`settings.json` yields one synthetic settings
  logical update, stable source/object identity, and no structural fleet work.
- A shared change touches only tenants whose effective fingerprint changed; a
  private change touches exactly its account tenant.
- Unrelated paths perform zero content reads and produce zero catalog commits or
  notifications.
- Repeated writes within one continuous cursor range coalesce to the final
  value and one source revision.
- Crash injection after inbox append, path restat, body stage, publication
  spool, catalog commit, index update, and cursor settlement recovers to one
  exact result with no lost or duplicated revision.
- Cursor gap/drop/wrap/root replacement and desired-fleet generation changes
  publish no delta and require one stable complete snapshot.
- Reads changing during materialization retain the same batch/cursor and retry;
  they never advance the watermark.
- Corrupt inbox/index/spool state fails closed with source-authority quarantine.
- Two concurrent preparation waiters share one barrier/publication and receive
  the same applicable tuple; canceling either waiter retains forward progress.

### Scale and integration gates

- A 10,000-object initial scan is bounded/paged; a one-object edit stats and
  reads only the named path and necessary parents.
- With 100 registered domains, 10 live, and 3 materialized, one shared change
  notifies exactly the three effective/materialized domains, with no inactive
  launch and one later on-demand catch-up.
- The reported 14-domain/9-active workload produces zero `itemCollision`,
  `ESTALE`, `itemDocTrackedButNotOnDisk`, delayed continuation, full-root
  incremental enumeration, or repeated post-ack launch.
- Clean signed VM tests cover install, reboot, app upgrade, observer cursor
  recovery, root replacement, FSEvents drop injection, and process death at
  every durable transition. The Go account daemon is proven not to access the
  App Group or own the source watcher.

The standard FuseKit race/vet/pure-build/protocol/Swift gates remain required;
live FSEvents, File Provider, process-kill, and TCC cases run only in the signed
VM harness.
