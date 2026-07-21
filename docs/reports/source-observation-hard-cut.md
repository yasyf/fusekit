# Source observation hard-cut architecture

## Decision

Recursive source observation, its durable cursor, physical-path index, repair
state, source-publication sequencing, and preparation barrier belong to
FuseKit. A consumer supplies only product policy: which source roots exist,
how an observed path maps to logical keys and tenant projections, and how to
produce the effective content for those keys.

The fixed signed consumer app runs this FuseKit runtime. An unsigned product
daemon does not watch protected source trees, own a parallel source-publication
database, or turn vnode notifications into fleet work. This keeps filesystem
access on the stable signed identity and removes the daemon-to-every-domain
fan-out seam.

This is a hard replacement. There is no compatibility watcher, polling
fallback, old source-state reader, or remote publication adapter.

## Failure evidence that required the hard cut

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

FuseKit already has the correct durable sinks. Complete snapshots are appended
as bounded `SourceSnapshotPublicationPage` values and atomically installed by
`Catalog.PromoteSourceSnapshot`; predecessor-fenced deltas are appended as
bounded `SourcePublicationStagePage` values and atomically installed by
`Catalog.CommitSourcePublicationStage`. Both paths advance the source watermark
only with the observer settlement that proves their source fence. The
`sourceauthority` runtime is the authoritative producer in front of those
sinks.

## Ownership

| Layer | Owns |
|---|---|
| daemonkit | Signed process lifecycle, persistent session framing, peer identity, cancellation, bounded queues, and reaping. It has no path, source-key, or tenant semantics. |
| FuseKit | Recursive path event stream, durable cursor/inbox, physical metadata index, opaque source-key allocation, snapshot repair state, source revisions, normalized publication stages, setwise catalog commit, convergence outbox, and the `PrepareTenant` source barrier. |
| cc-pool policy | Claude root declarations, shared/private/synthetic logical-key mapping, merge/split rules, effective bytes and fingerprints, and account-instance-to-tenant projection. It cannot notify domains or choose retries. |
| Signed consumer app | The concrete entitlements and stable executable that hosts the FuseKit observer/runtime. |

Paths are locators in the physical index, never object identifiers. cc-pool may
name a logical role such as `effective-settings` or map an account-private
source object, but FuseKit allocates and retains the opaque source key and
catalog `ObjectID` across rename, replacement, materialization, and
presentation changes.

## Landed FuseKit source-authority runtime

The `sourceauthority` package exposes the exact hard-cut policy boundary:

```go
type RootSpec struct {
    Authority        causal.SourceAuthorityID
    ID               RootID
    Path             string
    Kind             RootKind
    Generation       uint64
    ExpectedIdentity FileIdentity
}

type EventBatch struct {
    Stream      StreamIdentity
    Predecessor EventID
    Cursor      EventID
    RootEpoch   RootEpoch
    Events      []PathEvent
}

type Policy interface {
    Roots(context.Context, []tenant.TenantSpec) ([]RootSpec, error)
    PlanDelta(context.Context, IndexView, EventBatch) (DeltaPlan, error)
    PlanSnapshot(context.Context, SnapshotView, SnapshotPlanCursor, int) (SnapshotPlanPage, error)
    PlanMutation(context.Context, MutationRequest) (MutationPlan, error)
}

type Materializer interface {
    Materialize(context.Context, MaterializerTask) (Materialization, error)
}

type AuthorityPolicy interface {
    Policy
    Materializer
}
```

Desired source topology is durable catalog state. `holder.Config` receives its
fleet owner and one immutable `holder.DriverFactories` registry keyed by
durable driver ID. A physical factory resolves an `AuthorityPolicy`; a semantic
factory resolves a `sourcedriver.Driver`. `holder.RunChild` receives the same
registry and exposes only the exact child role requested by FuseKit. The
boundary enforces these invariants:

- `EventBatch` contains physical facts only. It has no account or Claude
  semantics.
- `DeltaPlan` names logical roles, affected tenant generations, exact deletes,
  and exact reads. It cannot carry File Provider domains or notifications.
- `Materialization` returns complete projections with bounded content streams
  or link targets and one effective fingerprint for the logical value.
- FuseKit turns the policy plan into bounded snapshot or predecessor-fenced
  publication-stage pages; policy code cannot select a source revision,
  predecessor, change ID, operation ID, or catalog object ID.

### Durable state

The single holder catalog now contains:

- `source_observer_roots`: authority, opaque root ID, root generation, volume
  identity, stable root identity, and root-set digest;
- `source_observer_streams`: stream identity, last durably received event ID,
  last applied event ID, and state (`snapshot_required` or `incremental`);
- `source_observer_inbox`: ordered immutable event batches and their digest;
- `source_physical_index`: authority/root/relative locator, physical identity,
  kind, metadata fingerprint, content fingerprint, and opaque logical binding;
- `source_publication_stages`, normalized page rows, and terminal receipts:
  bounded publication fragments, validated cumulative proofs, and compact
  exact replay results until the authority floor permits compaction.

The `source_watermarks` and `source_operations` tables remain the only
source-revision truth. Consumer-owned authority state, publication staging,
manifest, and per-tenant commit mirrors are outside the hard-cut API.

### Event acquisition

The Darwin backend uses one file-event FSEvents stream covering the normalized
root set. It is created before the initial scan and records volume/root
identity plus the persistent event ID. The callback performs no reads and no
policy work: it validates/bounds paths, appends an immutable batch to the
durable inbox, and wakes the source actor.

`FlagMustScanSubDirs`, `FlagUserDropped`, `FlagKernelDropped`,
`FlagEventIDsWrapped`, `FlagRootChanged`, and stream/root-set discontinuity are
state transitions to `snapshot_required`, not synthetic path events. The actor
is single-writer per source authority and processes inbox batches in cursor
order. Bounded ingestion applies backpressure; it never discards an event and
continues incrementally.

This semantic runtime is composed by `holder.Runtime` from catalog-owned desired
topology, `holder.Config.Owner`, and `holder.Config.Drivers`. The fixed child
dispatcher receives the same registry through `holder.ChildConfig.Drivers`; no
product daemon owns a parallel source runtime. It starts after catalog/tenant
recovery and before daemon admission. During drain it stops new observer
intake, settles every durable inbox/publication record, then closes before the
catalog and daemon listener. It does not create another lifecycle or transport
implementation.

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
6. Append bounded normalized pages to one predecessor-fenced catalog-owned
   publication stage. The stage owns its content references and exact rolling
   proof.
7. Commit the complete stage setwise. Object state, source watermark,
   per-tenant commits, causal change, convergence outbox, and observer
   settlement remain one transaction.
8. Atomically update the physical index, settle the inbox record, persist the
   compact terminal receipt, and advance the applied event cursor. A lost
   response replays the same receipt without retaining the stage payload.

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
4. Append bounded `catalog.SourceSnapshotPublicationPage` values for the exact
   desired tenant fleet and atomically install the complete staged snapshot
   with `Catalog.PromoteSourceSnapshot`.
5. Replace the physical index, settle inbox records covered by the fence, store
   the stream cursor/root epoch, and enter `incremental` state.

Repeated churn is bounded and quarantines the source authority with precise
cursor/root evidence. It does not publish a partial snapshot or silently switch
to polling. The policy never supplies a source revision or predecessor: FuseKit
allocates the exact next source revision internally, including when a native
cursor discontinuity forces snapshot repair.

## Exact snapshot-required conditions

A complete snapshot is mandatory only when one of these facts is present:

1. no committed observer baseline exists;
2. the desired tenant/root fleet or any tenant/root generation changed;
3. FSEvents reports `FlagMustScanSubDirs`, `FlagUserDropped`,
   `FlagKernelDropped`, `FlagEventIDsWrapped`, `FlagRootChanged`, or an event
   ID/predecessor gap;
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

The legacy caller-supplied source tuple is gone. The v1
`catalogproto.PrepareTenantRequest` contains only protocol, domain ID, and
expected generation; the tenant is the routed request identity. No client can
choose a catalog revision, source revision, change ID, or operation ID.

The FuseKit preparation adapter then:

1. asks `sourceauthority.Runtime.Barrier(tenant, generation)` to flush the
   observer stream and settle every continuous event through the barrier;
2. reads the tenant's latest applicable commit directly from the catalog;
3. calls the tenant actor and convergence engine with that exact tuple;
4. returns the catalog/domain observation proof.

Concurrent callers coalesce on the same authority cursor and tenant revision.
Cancellation removes only the waiter; it cannot abandon the source actor,
publication stage, tenant lane, or account reservation. This is the source-side
half of eliminating `acct-18 content catch-up: context deadline exceeded`.

## Consumer boundary

A consumer declares desired tenant and source topology and supplies immutable
physical or semantic driver factories. Product-specific path classification,
content synthesis, and effective fingerprints remain policy. Observation,
publication staging, revision allocation, preparation barriers, convergence,
and File Provider targeting remain exclusively in FuseKit. A consumer cannot
directly signal a File Provider domain or mirror FuseKit source state.

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
  stage append, catalog commit, index update, and cursor settlement recovers to one
  exact result with no lost or duplicated revision.
- Cursor gap/drop/wrap/root replacement and desired-fleet generation changes
  publish no delta and require one stable complete snapshot.
- Reads changing during materialization retain the same batch/cursor and retry;
  they never advance the watermark.
- Corrupt inbox/index/stage state fails closed with source-authority quarantine.
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
