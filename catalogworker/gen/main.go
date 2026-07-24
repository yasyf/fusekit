package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/yasyf/fusekit/catalog"
)

type field struct {
	name, typeName, jsonName string
}

type operation struct {
	name, wire        string
	request, response []field
}

var operations = []operation{
	{name: "Head", wire: "head", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Revision", "catalog.Revision", "revision"}}},
	{name: "CompactionFloor", wire: "compaction-floor", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Revision", "catalog.Revision", "revision"}}},
	{name: "Tenant", wire: "tenant", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Metadata", "catalog.TenantMetadata", "metadata"}}},
	{name: "Root", wire: "root", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "Lookup", wire: "lookup", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"ID", "catalog.ObjectID", "id"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "PrivateMutationObject", wire: "private-mutation-object", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"ID", "catalog.ObjectID", "id"}, {"Origin", "catalog.CausalOrigin", "origin"}}, response: []field{{"Result", "catalog.PrivateMutationResult", "result"}}},
	{name: "LookupAt", wire: "lookup-at", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"ID", "catalog.ObjectID", "id"}, {"Revision", "catalog.Revision", "revision"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "LookupName", wire: "lookup-name", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"Parent", "catalog.ObjectID", "parent"}, {"Name", "string", "name"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "Inspect", wire: "inspect", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"ID", "catalog.ObjectID", "id"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "Snapshot", wire: "snapshot", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Scope", "catalog.EnumerationScope", "scope"}, {"Revision", "catalog.Revision", "revision"}, {"Cursor", "catalog.SnapshotCursor", "cursor"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SnapshotPage", "page"}}},
	{name: "ChangesSince", wire: "changes-since", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Scope", "catalog.EnumerationScope", "scope"}, {"Cursor", "catalog.ChangeCursor", "cursor"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.ChangePage", "page"}}},
	{name: "StageContent", wire: "stage-content", response: []field{{"Ref", "catalog.ContentRef", "ref"}}},
	{name: "ReleaseUnclaimedContent", wire: "release-unclaimed-content", request: []field{{"Refs", "[]catalog.ContentRef", "refs"}}},
	{name: "OpenAt", wire: "open-at", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"Generation", "catalog.Generation", "generation"}, {"ID", "catalog.ObjectID", "id"}, {"Revision", "catalog.Revision", "revision"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "OpenSnapshotAt", wire: "open-snapshot-at", request: []field{{"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"Generation", "catalog.Generation", "generation"}, {"ID", "catalog.ObjectID", "id"}, {"Revision", "catalog.Revision", "revision"}}, response: []field{{"Token", "string", "token"}, {"Object", "catalog.Object", "object"}}},
	{name: "ReadSnapshotAt", wire: "read-snapshot-at", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}, {"Offset", "int64", "offset"}, {"Limit", "int", "limit"}}, response: []field{{"Data", "[]byte", "data"}, {"EOF", "bool", "eof"}}},
	{name: "CloseSnapshot", wire: "close-snapshot", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}},
	{name: "ForgetSnapshot", wire: "forget-snapshot", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}},
	{name: "OpenWriteAt", wire: "open-write-at", request: []field{{"Token", "string", "token"}, {"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Presentation", "catalog.Presentation", "presentation"}, {"Generation", "catalog.Generation", "generation"}, {"ID", "catalog.ObjectID", "id"}, {"Revision", "catalog.Revision", "revision"}}, response: []field{{"Object", "catalog.Object", "object"}}},
	{name: "ReadWriteAt", wire: "read-write-at", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}, {"Offset", "int64", "offset"}, {"Limit", "int", "limit"}}, response: []field{{"Data", "[]byte", "data"}, {"EOF", "bool", "eof"}}},
	{name: "WriteAt", wire: "write-at", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}, {"Offset", "int64", "offset"}, {"Data", "[]byte", "data"}}, response: []field{{"Written", "int", "written"}}},
	{name: "TruncateWrite", wire: "truncate-write", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}, {"Size", "int64", "size"}}},
	{name: "SyncWrite", wire: "sync-write", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}},
	{name: "SealAndBeginWrite", wire: "seal-and-begin-write", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}, response: []field{{"Result", "nativeWriteSealResult", "result"}}},
	{name: "ResolveCommittedWrite", wire: "resolve-committed-write", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}, response: []field{{"Operation", "catalog.MutationID", "operation"}, {"Object", "catalog.Object", "object"}}},
	{name: "AbortWrite", wire: "abort-write", request: []field{{"Owner", "string", "owner"}, {"Token", "string", "token"}}},
	{name: "CloseNativeSession", wire: "close-native-session", request: []field{{"Owner", "string", "owner"}}},
	{name: "BeginFileProviderMaterializationSnapshot", wire: "begin-file-provider-materialization-snapshot", request: []field{{"Identity", "catalog.FileProviderMaterializationIdentity", "identity"}}, response: []field{{"Epoch", "uint64", "epoch"}}},
	{name: "SuspendFileProviderMaterialization", wire: "suspend-file-provider-materialization", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Domain", "causal.DomainID", "domain"}, {"Generation", "catalog.Generation", "generation"}}},
	{name: "StageFileProviderMaterializationPage", wire: "stage-file-provider-materialization-page", request: []field{{"Page", "catalog.FileProviderMaterializationPage", "page"}}},
	{name: "CommitFileProviderMaterializationSnapshot", wire: "commit-file-provider-materialization-snapshot", request: []field{{"Commit", "catalog.FileProviderMaterializationCommit", "commit"}}, response: []field{{"Result", "catalog.FileProviderMaterializationResult", "result"}}},
	{name: "ResolveCriticalObjects", wire: "resolve-critical-objects", request: []field{{"Request", "catalog.CriticalObjectResolutionRequest", "request"}}, response: []field{{"Resolution", "catalog.CriticalObjectResolution", "resolution"}}},
	{name: "PendingMutation", wire: "pending-mutation", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Mutation", "*catalog.PreparedMutation", "mutation"}}},
	{name: "PreparedMutation", wire: "prepared-mutation", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"ID", "catalog.MutationID", "id"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "BeginMutation", wire: "begin-mutation", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Expected", "catalog.Revision", "expected"}, {"Intent", "catalog.MutationIntent", "intent"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "Mutation", wire: "mutation", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"ID", "catalog.MutationID", "id"}}, response: []field{{"Mutation", "catalog.MutationRecord", "mutation"}}},
	{name: "OpenMutationContent", wire: "open-mutation-content", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"ID", "catalog.MutationID", "id"}}},
	{name: "OpenPrivateContent", wire: "open-private-content", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}, {"ID", "catalog.ObjectID", "id"}, {"Creator", "catalog.MutationID", "creator"}, {"Origin", "catalog.CausalOrigin", "origin"}}},
	{name: "ClaimMutation", wire: "claim-mutation", request: []field{{"ID", "catalog.MutationID", "id"}, {"Owner", "catalog.MutationOwnerID", "owner"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "PrepareMutationSource", wire: "prepare-mutation-source", request: []field{{"ID", "catalog.MutationID", "id"}, {"Claim", "catalog.MutationClaim", "claim"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "SetMutationSourceResult", wire: "set-mutation-source-result", request: []field{{"ID", "catalog.MutationID", "id"}, {"Claim", "catalog.MutationClaim", "claim"}, {"Locator", "catalog.SourceLocator", "locator"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "ReclaimMutation", wire: "reclaim-mutation", request: []field{{"ID", "catalog.MutationID", "id"}, {"Stale", "catalog.MutationClaim", "stale"}, {"Owner", "catalog.MutationOwnerID", "owner"}}, response: []field{{"Mutation", "catalog.PreparedMutation", "mutation"}}},
	{name: "TopologyHead", wire: "topology-head", request: []field{{"Owner", "catalog.SourceAuthorityFleetOwnerID", "owner"}}, response: []field{{"Head", "catalog.TopologyHeadState", "head"}}},
	{name: "TopologySnapshot", wire: "topology-snapshot", request: []field{{"Request", "catalog.TopologySnapshotRequest", "request"}}, response: []field{{"Page", "catalog.TopologySnapshotPage", "page"}}},
	{name: "TopologyChangesSince", wire: "topology-changes-since", request: []field{{"Request", "catalog.TopologyChangesRequest", "request"}}, response: []field{{"Page", "catalog.TopologyChangePage", "page"}}},
	{name: "WaitTopologyChanges", wire: "wait-topology-changes", request: []field{{"Request", "catalog.TopologyChangesRequest", "request"}}, response: []field{{"Page", "catalog.TopologyChangePage", "page"}}},
	{name: "LoadTenantState", wire: "load-tenant-state", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"State", "catalog.TenantStateRecord", "state"}}},
	{name: "ProvisionTenant", wire: "provision-tenant", request: []field{{"Provision", "catalog.TenantProvision", "provision"}}, response: []field{{"Provision", "catalog.TenantProvision", "provision"}}},
	{name: "ReplaceTenantProvision", wire: "replace-tenant-provision", request: []field{{"Expected", "catalog.Generation", "expected"}, {"Next", "catalog.TenantProvision", "next"}}, response: []field{{"Provision", "catalog.TenantProvision", "provision"}}},
	{name: "RemoveTenantProvision", wire: "remove-tenant-provision", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}},
	{name: "ProveTenantRetired", wire: "prove-tenant-retired", request: []field{{"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}, response: []field{{"Proof", "catalog.TenantRetirementProof", "proof"}}},
	{name: "SaveTenantState", wire: "save-tenant-state", request: []field{{"Expected", "catalog.StateVersion", "expected"}, {"State", "catalog.TenantStateRecord", "state"}}, response: []field{{"State", "catalog.TenantStateRecord", "state"}}},
	{name: "PageFileProviderDomains", wire: "page-file-provider-domains", request: []field{{"After", "catalog.TenantID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.FileProviderDomainPage", "page"}}},
	{name: "FileProviderDomainForTenant", wire: "file-provider-domain-for-tenant", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Domain", "catalog.FileProviderDomain", "domain"}, {"Found", "bool", "found"}}},
	{name: "FileProviderDemand", wire: "file-provider-demand", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Domain", "causal.DomainID", "domain"}, {"Generation", "catalog.Generation", "generation"}, {"Now", "time.Time", "now"}}, response: []field{{"Leases", "uint32", "leases"}, {"Interests", "uint32", "interests"}}},
	{name: "PrepareFileProviderLease", wire: "prepare-file-provider-lease", request: []field{{"Lease", "catalog.FileProviderLease", "lease"}}, response: []field{{"Lease", "catalog.FileProviderLease", "lease"}}},
	{name: "CommitFileProviderLease", wire: "commit-file-provider-lease", request: []field{{"Lease", "catalog.FileProviderLease", "lease"}}, response: []field{{"Lease", "catalog.FileProviderLease", "lease"}}},
	{name: "RenewFileProviderLease", wire: "renew-file-provider-lease", request: []field{{"Lease", "catalog.FileProviderLease", "lease"}}, response: []field{{"Lease", "catalog.FileProviderLease", "lease"}}},
	{name: "ReleaseFileProviderLease", wire: "release-file-provider-lease", request: []field{{"Lease", "catalog.FileProviderLease", "lease"}}, response: []field{{"Lease", "catalog.FileProviderLease", "lease"}}},
	{name: "FileProviderContentPolicy", wire: "file-provider-content-policy", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Domain", "causal.DomainID", "domain"}, {"Generation", "catalog.Generation", "generation"}, {"Object", "catalog.ObjectID", "object"}, {"Now", "time.Time", "now"}}, response: []field{{"EagerKeep", "bool", "eager_keep"}}},
	{name: "BeginFileProviderDomainRemoval", wire: "begin-file-provider-domain-removal", request: []field{{"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}, response: []field{{"Removal", "catalog.FileProviderDomainRemoval", "removal"}}},
	{name: "FileProviderDomainRemovalState", wire: "file-provider-domain-removal-state", request: []field{{"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}, response: []field{{"Removal", "catalog.FileProviderDomainRemoval", "removal"}}},
	{name: "PageFileProviderDomainRemovals", wire: "page-file-provider-domain-removals", request: []field{{"After", "catalog.TenantID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.FileProviderDomainRemovalPage", "page"}}},
	{name: "ConfirmFileProviderDomainRemoval", wire: "confirm-file-provider-domain-removal", request: []field{{"Removal", "catalog.FileProviderDomainRemoval", "removal"}}},
	{name: "ConfirmFileProviderDomain", wire: "confirm-file-provider-domain", request: []field{{"Domain", "catalog.FileProviderDomain", "domain"}}},
	{name: "InvalidateFileProviderDomain", wire: "invalidate-file-provider-domain", request: []field{{"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}},
	{name: "ConfirmFileProviderDomainAbsent", wire: "confirm-file-provider-domain-absent", request: []field{{"Domain", "causal.DomainID", "domain"}}},
	{name: "NextBrokerCommandID", wire: "next-broker-command-id", response: []field{{"ID", "uint64", "id"}}},
	{name: "BeginBrokerCommandAttempt", wire: "begin-broker-command-attempt", request: []field{{"Attempt", "catalog.BrokerCommandAttempt", "attempt"}}, response: []field{{"Attempt", "catalog.BrokerCommandAttempt", "attempt"}, {"Created", "bool", "created"}}},
	{name: "TransitionBrokerCommandAttempt", wire: "transition-broker-command-attempt", request: []field{{"Attempt", "catalog.BrokerCommandAttempt", "attempt"}, {"Next", "catalog.BrokerCommandAttemptState", "next"}}, response: []field{{"Attempt", "catalog.BrokerCommandAttempt", "attempt"}}},
	{name: "AbandonBrokerCommandAttempt", wire: "abandon-broker-command-attempt", request: []field{{"Attempt", "catalog.BrokerCommandAttempt", "attempt"}}},
	{name: "RecoverReapedBrokerCommandAttempts", wire: "recover-reaped-broker-command-attempts", request: []field{{"Process", "catalog.BrokerProcessIdentity", "process"}}},
	{name: "RecoverBrokerCommandAttempts", wire: "recover-broker-command-attempts"},
	{name: "QuarantineSourceObserver", wire: "quarantine-source-observer", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Detail", "string", "detail"}}},
	{name: "SourceObserverStream", wire: "source-observer-stream", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Record", "catalog.SourceObserverStreamRecord", "record"}}},
	{name: "BeginSourceObserverConfiguration", wire: "begin-source-observer-configuration", request: []field{{"Identity", "catalog.SourceObserverConfigurationIdentity", "identity"}}},
	{name: "AppendSourceObserverConfigurationRoots", wire: "append-source-observer-configuration-roots", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}, {"Page", "catalog.SourceObserverRootAppendPage", "page"}}, response: []field{{"Ref", "catalog.SourceObserverConfigurationRef", "ref"}}},
	{name: "AppendSourceObserverConfigurationCheckpoints", wire: "append-source-observer-configuration-checkpoints", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}, {"Page", "catalog.SourceObserverCheckpointAppendPage", "page"}}, response: []field{{"Ref", "catalog.SourceObserverConfigurationRef", "ref"}}},
	{name: "CommitSourceObserverConfiguration", wire: "commit-source-observer-configuration", request: []field{{"Ref", "catalog.SourceObserverConfigurationRef", "ref"}}, response: []field{{"Record", "catalog.SourceObserverStreamRecord", "record"}}},
	{name: "AcknowledgeSourceObserverConfiguration", wire: "acknowledge-source-observer-configuration", request: []field{{"Ref", "catalog.SourceObserverConfigurationRef", "ref"}}},
	{name: "AbortSourceObserverConfiguration", wire: "abort-source-observer-configuration", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}}},
	{name: "SourceObserverRootsPage", wire: "source-observer-roots-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"After", "string", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceObserverRootPage", "page"}}},
	{name: "SourceObserverCheckpointsPage", wire: "source-observer-checkpoints-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"After", "string", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceObserverCheckpointPage", "page"}}},
	{name: "SourceObserverNextInbox", wire: "source-observer-next-inbox", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"After", "uint64", "after"}}, response: []field{{"Record", "*catalog.SourceObserverInboxRecord", "record"}}},
	{name: "SourceObserverInboxPage", wire: "source-observer-inbox-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"AfterExclusive", "uint64", "after_exclusive"}, {"ThroughInclusive", "uint64", "through_inclusive"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceObserverInboxPage", "page"}}},
	{name: "RequireSourceObserverSnapshot", wire: "require-source-observer-snapshot", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}},
	{name: "SourceMutationExpectation", wire: "source-mutation-expectation", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "catalog.MutationID", "operation"}}, response: []field{{"Record", "catalog.SourceMutationExpectationRecord", "record"}}},
	{name: "SourceMutationExpectationsPage", wire: "source-mutation-expectations-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"After", "catalog.MutationID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceMutationExpectationPage", "page"}}},
	{name: "CompleteSourceMutationRepair", wire: "complete-source-mutation-repair", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "catalog.MutationID", "operation"}}},
	{name: "BeginSourceSnapshotStage", wire: "begin-source-snapshot-stage", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Snapshot", "string", "snapshot"}}},
	{name: "AbortSourceSnapshotStage", wire: "abort-source-snapshot-stage", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Snapshot", "string", "snapshot"}}},
	{name: "AppendSourceSnapshotStagePage", wire: "append-source-snapshot-stage-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Snapshot", "string", "snapshot"}, {"Page", "catalog.SourceSnapshotPage", "page"}}},
	{name: "SourceSnapshotStagePage", wire: "source-snapshot-stage-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Snapshot", "string", "snapshot"}, {"After", "catalog.SourceIndexLocator", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceSnapshotPage", "page"}}},
	{name: "SourceSnapshotStageLookup", wire: "source-snapshot-stage-lookup", request: []field{{"Request", "catalog.SourceSnapshotPhysicalLookupRequest", "request"}}, response: []field{{"Page", "catalog.SourceSnapshotPhysicalLookupPage", "page"}}},
	{name: "SourceWatermark", wire: "source-watermark", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Revision", "causal.Revision", "revision"}}},
	{name: "BeginSourceSnapshotPublication", wire: "begin-source-snapshot-publication", request: []field{{"Identity", "catalog.SourceSnapshotIdentity", "identity"}}},
	{name: "SourceSnapshotRootLookup", wire: "source-snapshot-root-lookup", request: []field{{"Request", "catalog.SourceSnapshotRootLookupRequest", "request"}}, response: []field{{"Page", "catalog.SourceSnapshotRootLookupPage", "page"}}},
	{name: "AppendSourceSnapshotPublication", wire: "append-source-snapshot-publication", request: []field{{"Identity", "catalog.SourceSnapshotIdentity", "identity"}, {"Page", "catalog.SourceSnapshotPublicationPage", "page"}}, response: []field{{"Ref", "catalog.SourceSnapshotStageRef", "ref"}}},
	{name: "PromoteSourceSnapshot", wire: "promote-source-snapshot", request: []field{{"Ref", "catalog.SourceSnapshotStageRef", "ref"}, {"Settlement", "catalog.SourceSnapshotSettlement", "settlement"}}, response: []field{{"Result", "catalog.SourceResult", "result"}}},
	{name: "SourceAuthorityBindingLookup", wire: "source-authority-binding-lookup", request: []field{{"Request", "catalog.SourceAuthorityBindingLookupRequest", "request"}}, response: []field{{"Page", "catalog.SourceAuthorityBindingLookupPage", "page"}}},
	{name: "SourceObserverBindingForKey", wire: "source-observer-binding-for-key", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Key", "catalog.SourceObjectKey", "key"}}, response: []field{{"Binding", "catalog.SourceAuthorityBindingRecord", "binding"}}},
	{name: "SourceObserverBindingIndexPage", wire: "source-observer-binding-index-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Key", "catalog.SourceObjectKey", "key"}, {"After", "catalog.SourceIndexLocator", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourcePhysicalIndexPage", "page"}}},
	{name: "SourcePhysicalIndexLookup", wire: "source-physical-index-lookup", request: []field{{"Request", "catalog.SourcePhysicalIndexLookupRequest", "request"}}, response: []field{{"Page", "catalog.SourcePhysicalIndexLookupPage", "page"}}},
	{name: "SourcePhysicalIndexRecordsPage", wire: "source-physical-index-records-page", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"After", "catalog.SourceIndexLocator", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourcePhysicalIndexPage", "page"}}},
	{name: "SourcePhysicalIndexRecordByIdentity", wire: "source-physical-index-record-by-identity", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Identity", "[]byte", "identity"}}, response: []field{{"Record", "catalog.SourcePhysicalIndexRecord", "record"}}},
	{name: "ReserveSourceAuthorityBinding", wire: "reserve-source-authority-binding", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Logical", "string", "logical"}, {"Key", "catalog.SourceObjectKey", "key"}}, response: []field{{"Record", "catalog.SourceAuthorityBindingRecord", "record"}}},
	{name: "SettleSourceObserver", wire: "settle-source-observer", request: []field{{"Settlement", "catalog.SourceObserverSettlement", "settlement"}}},
	{name: "AcknowledgeSourceObserverSettlement", wire: "acknowledge-source-observer-settlement", request: []field{{"Ref", "catalog.SourcePublicationStageRef", "ref"}}},
	{name: "EnsureTenantNamespace", wire: "ensure-tenant-namespace", request: []field{{"Request", "catalog.EnsureTenantNamespaceRequest", "request"}}, response: []field{{"Namespace", "catalog.TenantNamespace", "namespace"}}},
	{name: "SetTenantPresent", wire: "set-tenant-present", request: []field{{"Mutation", "catalog.TenantMutation", "mutation"}, {"Definition", "catalog.TenantProvision", "definition"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "SetTenantAbsent", wire: "set-tenant-absent", request: []field{{"Mutation", "catalog.TenantMutation", "mutation"}, {"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "StageApplication", wire: "stage-application", request: []field{{"Request", "catalog.StageApplicationRequest", "request"}}, response: []field{{"Lease", "catalog.StagedViewLease", "lease"}, {"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "RecordPresentation", wire: "record-presentation", request: []field{{"Receipt", "catalog.PresentationReceipt", "receipt"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "ActivateTenant", wire: "activate-tenant", request: []field{{"Request", "catalog.ActivateTenantRequest", "request"}}, response: []field{{"Result", "catalog.TenantActivationResult", "result"}}},
	{name: "RecoverTenantPreparations", wire: "recover-tenant-preparations", request: []field{{"Request", "catalog.TenantPreparationRecoveryRequest", "request"}}, response: []field{{"Result", "catalog.TenantPreparationRecoveryResult", "result"}}},
	{name: "TenantTargetingRevision", wire: "tenant-targeting-revision", request: []field{{"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"Revision", "uint64", "revision"}}},
	{name: "RetirePresentation", wire: "retire-presentation", request: []field{{"Request", "catalog.RetirementRequest", "request"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "RetireApplication", wire: "retire-application", request: []field{{"Request", "catalog.RetirementRequest", "request"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "ClearTenantActivation", wire: "clear-tenant-activation", request: []field{{"Request", "catalog.RetirementRequest", "request"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "TenantLifecycle", wire: "tenant-lifecycle", request: []field{{"Owner", "string", "owner"}, {"Tenant", "catalog.TenantID", "tenant"}}, response: []field{{"State", "catalog.TenantLifecycleState", "state"}}},
	{name: "AppendSourceObserverInbox", wire: "append-source-observer-inbox", request: []field{{"Record", "catalog.SourceObserverInboxRecord", "record"}}, response: []field{{"Sequence", "uint64", "sequence"}}},
	{name: "PutSourceMutationExpectation", wire: "put-source-mutation-expectation", request: []field{{"Record", "catalog.SourceMutationExpectationRecord", "record"}}},
	{name: "CompleteSourceMutationExpectation", wire: "complete-source-mutation-expectation", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "catalog.MutationID", "operation"}, {"Receipt", "[]byte", "receipt"}}},
	{name: "RecoverSourceMutationExpectationReceipt", wire: "recover-source-mutation-expectation-receipt", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "catalog.MutationID", "operation"}, {"Receipt", "[]byte", "receipt"}}},
	{name: "RecoverDeliveries", wire: "recover-deliveries", request: []field{{"RuntimeGeneration", "string", "runtime_generation"}, {"Now", "time.Time", "now"}}},
	{name: "ClaimDelivery", wire: "claim-delivery", request: []field{{"Request", "convergence.ClaimRequest", "request"}}, response: []field{{"Claim", "*convergence.DeliveryClaim", "claim"}}},
	{name: "RecordDelivery", wire: "record-delivery", request: []field{{"Delivery", "convergence.DeliveryResult", "delivery"}}},
	{name: "AcknowledgeDelivery", wire: "acknowledge-delivery", request: []field{{"Acknowledgement", "causal.ActivationAck", "acknowledgement"}}},
	{name: "QuarantineExpired", wire: "quarantine-expired", request: []field{{"Now", "time.Time", "now"}}},
	{name: "ActivationPresentationTarget", wire: "activation-presentation-target", request: []field{{"Key", "causal.ActivationKey", "key"}}, response: []field{{"Target", "catalog.TenantPresentationTarget", "target"}}},
	{name: "PendingSourcePublicationStage", wire: "pending-source-publication-stage", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Ref", "*catalog.SourcePublicationStageRef", "ref"}}},
	{name: "BeginSourcePublicationStage", wire: "begin-source-publication-stage", request: []field{{"Identity", "catalog.SourcePublicationStageIdentity", "identity"}}},
	{name: "AppendSourcePublicationStage", wire: "append-source-publication-stage", request: []field{{"Identity", "catalog.SourcePublicationStageIdentity", "identity"}, {"Page", "catalog.SourcePublicationStagePage", "page"}}, response: []field{{"Ref", "catalog.SourcePublicationStageRef", "ref"}}},
	{name: "CommitSourcePublicationStage", wire: "commit-source-publication-stage", request: []field{{"Ref", "catalog.SourcePublicationStageRef", "ref"}}, response: []field{{"Result", "catalog.SourcePublicationStageResult", "result"}}},
	{name: "AbortSourcePublicationStage", wire: "abort-source-publication-stage", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}}},
	{name: "SourceDriverCheckpoint", wire: "source-driver-checkpoint", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Checkpoint", "catalog.SourceDriverCheckpoint", "checkpoint"}}},
	{name: "SourceDriverTargetCheckpoint", wire: "source-driver-target-checkpoint", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Tenant", "catalog.TenantID", "tenant"}, {"Generation", "catalog.Generation", "generation"}}, response: []field{{"Checkpoint", "catalog.SourceDriverTargetCheckpoint", "checkpoint"}}},
	{name: "SourceDriverCommittedTargetCheckpoints", wire: "source-driver-committed-target-checkpoints", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}, {"After", "catalog.TenantID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceDriverTargetCheckpointPage", "page"}}},
	{name: "PendingSourceDriverStage", wire: "pending-source-driver-stage", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"State", "*catalog.SourceDriverStageState", "state"}}},
	{name: "ValidateSourceDriverTargetEpoch", wire: "validate-source-driver-target-epoch", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"TargetEpoch", "uint64", "target_epoch"}}},
	{name: "SourceDriverTargetEpoch", wire: "source-driver-target-epoch", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"TargetEpoch", "uint64", "target_epoch"}}},
	{name: "RequireSourceDriverSnapshot", wire: "require-source-driver-snapshot", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Token", "string", "token"}, {"Reason", "catalog.SourceDriverSnapshotReason", "reason"}}, response: []field{{"Checkpoint", "catalog.SourceDriverCheckpoint", "checkpoint"}}},
	{name: "RebindSourceDriverCheckpoint", wire: "rebind-source-driver-checkpoint", request: []field{{"Request", "catalog.SourceDriverCheckpointRebind", "request"}}, response: []field{{"Checkpoint", "catalog.SourceDriverCheckpoint", "checkpoint"}}},
	{name: "ReserveSourceDriverMutation", wire: "reserve-source-driver-mutation", request: []field{{"Request", "catalog.SourceDriverMutationReservationRequest", "request"}}, response: []field{{"Reservation", "catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "SourceDriverMutationReservation", wire: "source-driver-mutation-reservation", request: []field{{"Mutation", "catalog.MutationID", "mutation"}}, response: []field{{"Reservation", "catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "ActiveSourceDriverMutationReservation", wire: "active-source-driver-mutation-reservation", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Reservation", "*catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "PrepareSourceDriverMutationReservationBatch", wire: "prepare-source-driver-mutation-reservation-batch", request: []field{{"Mutation", "catalog.MutationID", "mutation"}, {"Claim", "catalog.MutationClaim", "claim"}}, response: []field{{"Reservation", "catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "SourceDriverMutationReservationTargets", wire: "source-driver-mutation-reservation-targets", request: []field{{"Mutation", "catalog.MutationID", "mutation"}, {"After", "catalog.TenantID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceDriverTargetPage", "page"}}},
	{name: "BindSourceDriverMutationRequest", wire: "bind-source-driver-mutation-request", request: []field{{"Mutation", "catalog.MutationID", "mutation"}, {"Claim", "catalog.MutationClaim", "claim"}, {"Digest", "[32]byte", "digest"}}, response: []field{{"Reservation", "catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "RecordSourceDriverMutationReceipt", wire: "record-source-driver-mutation-receipt", request: []field{{"Mutation", "catalog.MutationID", "mutation"}, {"Claim", "catalog.MutationClaim", "claim"}, {"Proof", "catalog.SourceDriverMutationReceiptProof", "proof"}}, response: []field{{"Reservation", "catalog.SourceDriverMutationReservation", "reservation"}}},
	{name: "ReleaseUnboundSourceDriverMutationReservation", wire: "release-unbound-source-driver-mutation-reservation", request: []field{{"Mutation", "catalog.MutationID", "mutation"}, {"Claim", "catalog.MutationClaim", "claim"}, {"TargetEpoch", "uint64", "target_epoch"}}},
	{name: "BeginSourceDriverStage", wire: "begin-source-driver-stage", request: []field{{"Identity", "catalog.SourceDriverStageIdentity", "identity"}}},
	{name: "PrepareSourceDriverTargetDeclarationBatch", wire: "prepare-source-driver-target-declaration-batch", request: []field{{"Identity", "catalog.SourceDriverStageIdentity", "identity"}}, response: []field{{"State", "catalog.SourceDriverTargetDeclarationState", "state"}}},
	{name: "SourceDriverStageTargets", wire: "source-driver-stage-targets", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Operation", "causal.OperationID", "operation"}, {"After", "catalog.TenantID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Targets", "[]catalog.SourceDriverTarget", "targets"}}},
	{name: "AppendSourceDriverStage", wire: "append-source-driver-stage", request: []field{{"Identity", "catalog.SourceDriverStageIdentity", "identity"}, {"Page", "catalog.SourceDriverStagePage", "page"}}, response: []field{{"State", "catalog.SourceDriverStageState", "state"}}},
	{name: "PrepareSourceDriverPublicationBatch", wire: "prepare-source-driver-publication-batch", request: []field{{"Identity", "catalog.SourceDriverStageIdentity", "identity"}}, response: []field{{"State", "catalog.SourceDriverPreparationState", "state"}}},
	{name: "CommitSourceDriverStage", wire: "commit-source-driver-stage", request: []field{{"State", "catalog.SourceDriverStageState", "state"}}, response: []field{{"Result", "catalog.SourceDriverStageResult", "result"}}},
	{name: "CommitSourceDriverMutation", wire: "commit-source-driver-mutation", request: []field{{"State", "catalog.SourceDriverStageState", "state"}}, response: []field{{"Result", "catalog.SourceDriverStageResult", "result"}}},
	{name: "AbortSourceDriverStage", wire: "abort-source-driver-stage", request: []field{{"Identity", "catalog.SourceDriverStageIdentity", "identity"}}},
	{name: "PendingSourceDriverCommittedReceipt", wire: "pending-source-driver-committed-receipt", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}}, response: []field{{"Receipt", "*catalog.SourceDriverCommittedReceipt", "receipt"}}},
	{name: "PendingSourceDriverReceiptAuthorities", wire: "pending-source-driver-receipt-authorities", request: []field{{"After", "causal.SourceAuthorityID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.SourceDriverReceiptAuthorityPage", "page"}}},
	{name: "CommittedSourceDriverMutation", wire: "committed-source-driver-mutation", request: []field{{"Authority", "causal.SourceAuthorityID", "authority"}, {"Mutation", "catalog.MutationID", "mutation"}}, response: []field{{"Receipt", "*catalog.SourceDriverCommittedReceipt", "receipt"}}},
	{name: "AcknowledgeSourceDriverCommittedReceipt", wire: "acknowledge-source-driver-committed-receipt", request: []field{{"Result", "catalog.SourceDriverStageResult", "result"}}},
	{name: "ForgetSourceDriverCommittedReceipt", wire: "forget-source-driver-committed-receipt", request: []field{{"Result", "catalog.SourceDriverStageResult", "result"}}},
	{name: "PublishDesiredSourceFleet", wire: "publish-desired-source-fleet", request: []field{{"Request", "catalog.PublishDesiredSourceFleetRequest", "request"}}, response: []field{{"State", "catalog.DesiredSourceAuthorityFleetState", "state"}}},
	{name: "DesiredSourceFleetPage", wire: "desired-source-fleet-page", request: []field{{"Request", "catalog.DesiredSourceFleetPageRequest", "request"}}, response: []field{{"Page", "catalog.DesiredSourceFleetPage", "page"}}},
	{name: "SourceAuthorityFleetHead", wire: "source-authority-fleet-head", request: []field{{"Owner", "catalog.SourceAuthorityFleetOwnerID", "owner"}}, response: []field{{"Status", "catalog.SourceAuthorityFleetStatus", "status"}}},
	{name: "SourceAuthorityFleetPage", wire: "source-authority-fleet-page", request: []field{{"Request", "catalog.SourceAuthorityFleetPageRequest", "request"}}, response: []field{{"Page", "catalog.SourceAuthorityFleetPage", "page"}}},
	{name: "ReconcileSourceAuthorityFleet", wire: "reconcile-source-authority-fleet", request: []field{{"Request", "catalog.SourceAuthorityFleetReconcileRequest", "request"}}, response: []field{{"State", "catalog.SourceAuthorityFleetReconcileState", "state"}}},
	{name: "AbortSourceAuthorityFleet", wire: "abort-source-authority-fleet", request: []field{{"Request", "catalog.SourceAuthorityFleetAbortRequest", "request"}}, response: []field{{"Receipt", "catalog.SourceAuthorityFleetAbortReceipt", "receipt"}}},
	{name: "RetireSourceAuthority", wire: "retire-source-authority", request: []field{{"Request", "catalog.SourceAuthorityRetireRequest", "request"}}, response: []field{{"Receipt", "catalog.SourceAuthorityRetirementReceipt", "receipt"}}},
	{name: "AcknowledgeSourceAuthorityFleet", wire: "acknowledge-source-authority-fleet", request: []field{{"Acknowledgement", "catalog.SourceAuthorityFleetAcknowledgement", "acknowledgement"}}, response: []field{{"State", "catalog.SourceAuthorityFleetState", "state"}}},
	{name: "SourceAuthorityRuntimeStatus", wire: "source-authority-runtime-status", request: []field{{"Ref", "catalog.SourceAuthorityRuntimeRef", "ref"}}, response: []field{{"State", "catalog.SourceAuthorityRuntimeState", "state"}}},
	{name: "TakeoverSourceAuthorityRuntime", wire: "takeover-source-authority-runtime", request: []field{{"Takeover", "catalog.SourceAuthorityRuntimeTakeover", "takeover"}}},
	{name: "OpenSourceAuthorityRuntime", wire: "open-source-authority-runtime", request: []field{{"Fence", "catalog.SourceAuthorityRuntimeFence", "fence"}}},
	{name: "CloseSourceAuthorityRuntime", wire: "close-source-authority-runtime", request: []field{{"Fence", "catalog.SourceAuthorityRuntimeFence", "fence"}}},
	{name: "BeginRecoverReapedSourceAuthorityRuntimes", wire: "begin-recover-reaped-source-authority-runtimes", request: []field{{"Receipt", "proc.ReapReceipt", "receipt"}}, response: []field{{"Summary", "catalog.SourceAuthorityRuntimeRecoverySummary", "summary"}}},
	{name: "AcknowledgeSourceAuthorityRuntimeRecovery", wire: "acknowledge-source-authority-runtime-recovery", request: []field{{"Floor", "proc.ReapReceiptFloor", "floor"}}},
	{name: "SourceAuthorityRuntimeRecoveryPage", wire: "source-authority-runtime-recovery-page", request: []field{{"Request", "catalog.SourceAuthorityRuntimeRecoveryPageRequest", "request"}}, response: []field{{"Page", "catalog.SourceAuthorityRuntimeRecoveryPage", "page"}}},
	{name: "InspectStorageQuarantine", wire: "inspect-storage-quarantine", request: []field{{"After", "catalog.StorageTransitionID", "after"}, {"Limit", "int", "limit"}}, response: []field{{"Page", "catalog.StorageQuarantinePage", "page"}}},
	{name: "ResolveStorageQuarantine", wire: "resolve-storage-quarantine", request: []field{{"TransitionID", "catalog.StorageTransitionID", "id"}, {"Token", "catalog.StorageQuarantineToken", "token"}, {"Resolution", "catalog.StorageQuarantineResolution", "resolution"}}, response: []field{{"Receipt", "catalog.StorageQuarantineResolutionReceipt", "receipt"}}},
	{name: "AcknowledgeStorageQuarantineResolution", wire: "acknowledge-storage-quarantine-resolution", request: []field{{"Receipt", "catalog.StorageQuarantineResolutionReceipt", "receipt"}}},
}

var mutatingOperations = map[string]bool{
	"ClaimMutation": true, "PrepareMutationSource": true, "SetMutationSourceResult": true,
	"ReclaimMutation": true,
	"ProvisionTenant": true, "ReplaceTenantProvision": true, "RemoveTenantProvision": true,
	"SaveTenantState":                true,
	"BeginFileProviderDomainRemoval": true, "ConfirmFileProviderDomainRemoval": true,
	"ConfirmFileProviderDomain": true, "InvalidateFileProviderDomain": true, "ConfirmFileProviderDomainAbsent": true,
	"NextBrokerCommandID": true, "BeginBrokerCommandAttempt": true,
	"TransitionBrokerCommandAttempt": true, "AbandonBrokerCommandAttempt": true,
	"RecoverReapedBrokerCommandAttempts": true,
	"RecoverBrokerCommandAttempts":       true,
	"ReleaseUnclaimedContent":            true, "BeginMutation": true,
	"OpenSnapshotAt": true, "CloseSnapshot": true, "ForgetSnapshot": true,
	"OpenWriteAt": true, "WriteAt": true, "TruncateWrite": true, "SyncWrite": true,
	"SealAndBeginWrite": true, "ResolveCommittedWrite": true, "AbortWrite": true,
	"CloseNativeSession":                        true,
	"BeginFileProviderMaterializationSnapshot":  true,
	"SuspendFileProviderMaterialization":        true,
	"StageFileProviderMaterializationPage":      true,
	"CommitFileProviderMaterializationSnapshot": true,
	"QuarantineSourceObserver":                  true, "RequireSourceObserverSnapshot": true,
	"BeginSourceObserverConfiguration": true, "AppendSourceObserverConfigurationRoots": true,
	"AppendSourceObserverConfigurationCheckpoints": true, "CommitSourceObserverConfiguration": true,
	"AcknowledgeSourceObserverConfiguration": true,
	"AbortSourceObserverConfiguration":       true,
	"CompleteSourceMutationRepair":           true, "BeginSourceSnapshotStage": true,
	"AbortSourceSnapshotStage": true, "AppendSourceSnapshotStagePage": true,
	"BeginSourceSnapshotPublication":  true,
	"AppendSourceSnapshotPublication": true, "PromoteSourceSnapshot": true,
	"ReserveSourceAuthorityBinding": true, "SettleSourceObserver": true,
	"AcknowledgeSourceObserverSettlement": true,
	"AppendSourceObserverInbox":           true,
	"PutSourceMutationExpectation":        true, "CompleteSourceMutationExpectation": true,
	"RecoverSourceMutationExpectationReceipt": true,
	"EnsureTenantNamespace":                   true, "SetTenantPresent": true, "SetTenantAbsent": true,
	"StageApplication": true, "RecordPresentation": true, "ActivateTenant": true,
	"RecoverTenantPreparations": true, "RetirePresentation": true, "RetireApplication": true,
	"ClearTenantActivation": true,
	"RecoverDeliveries":     true, "ClaimDelivery": true, "RecordDelivery": true,
	"AcknowledgeDelivery": true, "QuarantineExpired": true,
	"PrepareFileProviderLease":    true,
	"CommitFileProviderLease":     true,
	"RenewFileProviderLease":      true,
	"ReleaseFileProviderLease":    true,
	"BeginSourcePublicationStage": true, "AppendSourcePublicationStage": true,
	"CommitSourcePublicationStage": true, "AbortSourcePublicationStage": true,
	"RequireSourceDriverSnapshot": true, "RebindSourceDriverCheckpoint": true, "BeginSourceDriverStage": true,
	"ReserveSourceDriverMutation": true, "PrepareSourceDriverMutationReservationBatch": true,
	"BindSourceDriverMutationRequest": true, "RecordSourceDriverMutationReceipt": true,
	"ReleaseUnboundSourceDriverMutationReservation": true,
	"AppendSourceDriverStage":                       true, "PrepareSourceDriverTargetDeclarationBatch": true,
	"PrepareSourceDriverPublicationBatch": true,
	"CommitSourceDriverStage":             true,
	"CommitSourceDriverMutation":          true, "AbortSourceDriverStage": true,
	"AcknowledgeSourceDriverCommittedReceipt": true,
	"ForgetSourceDriverCommittedReceipt":      true,
	"PublishDesiredSourceFleet":               true,
	"ReconcileSourceAuthorityFleet":           true, "AbortSourceAuthorityFleet": true,
	"RetireSourceAuthority":           true,
	"AcknowledgeSourceAuthorityFleet": true,
	"TakeoverSourceAuthorityRuntime":  true,
	"OpenSourceAuthorityRuntime":      true, "CloseSourceAuthorityRuntime": true,
	"BeginRecoverReapedSourceAuthorityRuntimes": true,
	"AcknowledgeSourceAuthorityRuntimeRecovery": true,
	"ResolveStorageQuarantine":                  true, "AcknowledgeStorageQuarantineResolution": true,
}

var generatedUnaryOperations = map[string]bool{
	"Inspect": true, "LookupAt": true, "PrivateMutationObject": true, "ReleaseUnclaimedContent": true,
	"PendingMutation": true, "PreparedMutation": true, "BeginMutation": true, "Mutation": true,
	"TopologyHead": true, "TopologySnapshot": true, "ProveTenantRetired": true,
	"TopologyChangesSince": true, "WaitTopologyChanges": true,
	"PageFileProviderDomains": true, "PageFileProviderDomainRemovals": true,
	"QuarantineSourceObserver": true, "SourceObserverStream": true,
	"BeginSourceObserverConfiguration": true, "AppendSourceObserverConfigurationRoots": true,
	"AppendSourceObserverConfigurationCheckpoints": true, "CommitSourceObserverConfiguration": true,
	"AcknowledgeSourceObserverConfiguration": true,
	"AbortSourceObserverConfiguration":       true, "SourceObserverRootsPage": true,
	"SourceObserverCheckpointsPage": true,
	"SourceObserverNextInbox":       true, "SourceObserverInboxPage": true,
	"RequireSourceObserverSnapshot": true, "SourceMutationExpectation": true,
	"SourceMutationExpectationsPage": true,
	"CompleteSourceMutationRepair":   true, "BeginSourceSnapshotStage": true,
	"AbortSourceSnapshotStage": true, "AppendSourceSnapshotStagePage": true,
	"SourceSnapshotStagePage": true, "SourceSnapshotStageLookup": true,
	"SourceWatermark": true, "BeginSourceSnapshotPublication": true,
	"SourceSnapshotRootLookup": true, "AppendSourceSnapshotPublication": true,
	"PromoteSourceSnapshot":        true,
	"SourceAuthorityBindingLookup": true,
	"SourceObserverBindingForKey":  true, "SourceObserverBindingIndexPage": true,
	"SourcePhysicalIndexLookup":      true,
	"SourcePhysicalIndexRecordsPage": true, "SourcePhysicalIndexRecordByIdentity": true,
	"ReserveSourceAuthorityBinding": true, "SettleSourceObserver": true,
	"AcknowledgeSourceObserverSettlement": true,
	"EnsureTenantNamespace":               true, "SetTenantPresent": true, "SetTenantAbsent": true,
	"StageApplication": true, "RecordPresentation": true, "ActivateTenant": true,
	"RecoverTenantPreparations": true, "TenantTargetingRevision": true,
	"RetirePresentation": true, "RetireApplication": true, "ClearTenantActivation": true,
	"TenantLifecycle":              true,
	"AppendSourceObserverInbox":    true,
	"PutSourceMutationExpectation": true, "CompleteSourceMutationExpectation": true,
	"RecoverSourceMutationExpectationReceipt": true,
	"RecoverDeliveries":                       true, "ClaimDelivery": true, "RecordDelivery": true,
	"AcknowledgeDelivery": true, "QuarantineExpired": true,
	"ActivationPresentationTarget": true,
	"FileProviderDomainForTenant":  true, "FileProviderDemand": true,
	"PrepareFileProviderLease": true, "CommitFileProviderLease": true,
	"RenewFileProviderLease": true, "ReleaseFileProviderLease": true,
	"FileProviderContentPolicy":                 true,
	"InvalidateFileProviderDomain":              true,
	"BeginFileProviderMaterializationSnapshot":  true,
	"SuspendFileProviderMaterialization":        true,
	"StageFileProviderMaterializationPage":      true,
	"CommitFileProviderMaterializationSnapshot": true,
	"ResolveCriticalObjects":                    true,
	"PendingSourcePublicationStage":             true, "BeginSourcePublicationStage": true,
	"AppendSourcePublicationStage": true, "CommitSourcePublicationStage": true,
	"AbortSourcePublicationStage": true,
	"SourceDriverCheckpoint":      true, "SourceDriverTargetCheckpoint": true,
	"SourceDriverCommittedTargetCheckpoints": true,
	"PendingSourceDriverStage":               true,
	"ValidateSourceDriverTargetEpoch":        true,
	"SourceDriverTargetEpoch":                true,
	"SourceDriverStageTargets":               true,
	"RequireSourceDriverSnapshot":            true, "RebindSourceDriverCheckpoint": true, "BeginSourceDriverStage": true,
	"ReserveSourceDriverMutation": true, "SourceDriverMutationReservation": true,
	"ActiveSourceDriverMutationReservation":         true,
	"PrepareSourceDriverMutationReservationBatch":   true,
	"SourceDriverMutationReservationTargets":        true,
	"BindSourceDriverMutationRequest":               true,
	"RecordSourceDriverMutationReceipt":             true,
	"ReleaseUnboundSourceDriverMutationReservation": true,
	"AppendSourceDriverStage":                       true, "CommitSourceDriverStage": true,
	"PrepareSourceDriverTargetDeclarationBatch": true,
	"PrepareSourceDriverPublicationBatch":       true,
	"CommitSourceDriverMutation":                true, "AbortSourceDriverStage": true,
	"PendingSourceDriverCommittedReceipt": true, "CommittedSourceDriverMutation": true,
	"PendingSourceDriverReceiptAuthorities":   true,
	"AcknowledgeSourceDriverCommittedReceipt": true,
	"ForgetSourceDriverCommittedReceipt":      true,
	"RecoverReapedBrokerCommandAttempts":      true,
	"PublishDesiredSourceFleet":               true, "DesiredSourceFleetPage": true,
	"SourceAuthorityFleetHead": true, "SourceAuthorityFleetPage": true,
	"ReconcileSourceAuthorityFleet": true, "AbortSourceAuthorityFleet": true,
	"RetireSourceAuthority":           true,
	"AcknowledgeSourceAuthorityFleet": true,
	"SourceAuthorityRuntimeStatus":    true, "TakeoverSourceAuthorityRuntime": true,
	"OpenSourceAuthorityRuntime": true, "CloseSourceAuthorityRuntime": true,
	"BeginRecoverReapedSourceAuthorityRuntimes": true,
	"AcknowledgeSourceAuthorityRuntimeRecovery": true,
	"SourceAuthorityRuntimeRecoveryPage":        true,
	"InspectStorageQuarantine":                  true, "ResolveStorageQuarantine": true,
	"AcknowledgeStorageQuarantineResolution": true,
}

func main() {
	check := flag.Bool("check", false, "fail if generated output differs")
	flag.Parse()
	source, err := format.Source([]byte(render()))
	if err != nil {
		panic(err)
	}
	path := filepath.Join(moduleRoot(), "catalogworker", "messages_gen.go")
	if *check {
		existing, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(existing, source) {
			fmt.Fprintf(os.Stderr, "%s is stale\n", path)
			os.Exit(1)
		}
		return
	}
	if err := os.WriteFile(path, source, 0o644); err != nil {
		panic(err)
	}
}

func moduleRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("catalogworker/gen: caller path unavailable")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func render() string {
	var b strings.Builder
	b.WriteString("// Code generated by catalogworker/gen; DO NOT EDIT.\n\npackage catalogworker\n\n")
	b.WriteString("import (\n\"context\"\n\"time\"\n\"github.com/yasyf/daemonkit/proc\"\n\"github.com/yasyf/daemonkit/wire\"\n\"github.com/yasyf/fusekit/catalog\"\n\"github.com/yasyf/fusekit/causal\"\n\"github.com/yasyf/fusekit/convergence\"\n)\n\n")
	b.WriteString("const Version uint16 = 1\n\n")
	fmt.Fprintf(&b, "const SchemaFingerprint = %q\n\n", schemaFingerprint())
	b.WriteString("type Operation string\n\nconst (\n")
	for _, operation := range operations {
		fmt.Fprintf(&b, "Operation%s Operation = %q\n", operation.name, "fusekit.catalog-worker."+operation.wire+".v1")
	}
	b.WriteString(")\n\n")
	for _, operation := range operations {
		fmt.Fprintf(&b, "type %sRequest struct {\nHeader requestHeader `json:\"header\"`\n", lowerFirst(operation.name))
		writeFields(&b, operation.request)
		b.WriteString("}\n\n")
		fmt.Fprintf(&b, "type %sResponse struct {\nHeader responseHeader `json:\"header\"`\n", lowerFirst(operation.name))
		writeFields(&b, operation.response)
		b.WriteString("}\n\n")
	}
	b.WriteString("func generatedHandlers(service *server) []wire.HandlerSpec {\nreturn []wire.HandlerSpec{\n")
	for _, operation := range operations {
		handler := "service.handle" + operation.name
		if mutatingOperations[operation.name] {
			handler = "service.mutationHandler(" + handler + ")"
		}
		fmt.Fprintf(&b, "{Op: wire.Op(Operation%s), Handler: %s, Concurrent: true},\n", operation.name, handler)
	}
	b.WriteString("}\n}\n\n")
	b.WriteString("func generatedLadder(serverDeadline, clientDeadline time.Duration) (wire.Ladder, error) {\n")
	b.WriteString("server := map[wire.Op]time.Duration{\n")
	for _, operation := range operations {
		fmt.Fprintf(&b, "wire.Op(Operation%s): serverDeadline,\n", operation.name)
	}
	b.WriteString("}\nclient := map[wire.Op]time.Duration{\n")
	for _, operation := range operations {
		fmt.Fprintf(&b, "wire.Op(Operation%s): clientDeadline,\n", operation.name)
	}
	b.WriteString("}\nreturn wire.NewLadder(server, client)\n}\n\n")
	for _, operation := range operations {
		if !generatedUnaryOperations[operation.name] {
			continue
		}
		renderServerHandler(&b, operation)
		renderClientMethod(&b, operation)
		if len(operation.response) <= 1 {
			renderManagerMethod(&b, operation)
		}
	}
	return b.String()
}

func schemaFingerprint() string {
	manifest := schemaManifest()
	digest := sha256.Sum256([]byte(manifest))
	return "fusekit.catalog-worker." + hex.EncodeToString(digest[:])
}

func schemaManifest() string {
	var manifest strings.Builder
	manifest.WriteString("fusekit.catalog-worker.v1\n")
	for _, operation := range operations {
		fmt.Fprintf(&manifest, "%s:%s\n", operation.name, operation.wire)
		for _, field := range operation.request {
			fmt.Fprintf(&manifest, "request:%s:%s:%s\n", field.name, field.typeName, field.jsonName)
		}
		for _, field := range operation.response {
			fmt.Fprintf(&manifest, "response:%s:%s:%s\n", field.name, field.typeName, field.jsonName)
		}
	}
	seen := make(map[reflect.Type]bool)
	appendSchemaType(&manifest, reflect.TypeOf(catalog.FileProviderDomain{}), seen)
	appendSchemaType(&manifest, reflect.TypeOf(catalog.FileProviderDomainRemoval{}), seen)
	appendSchemaType(&manifest, reflect.TypeOf(catalog.SourceAuthorityRuntimeState{}), seen)
	appendSchemaType(&manifest, reflect.TypeOf(catalog.SourceDriverStagePage{}), seen)
	appendSchemaType(&manifest, reflect.TypeOf(catalog.SourceDriverStageResult{}), seen)
	return manifest.String()
}

func appendSchemaType(manifest *strings.Builder, value reflect.Type, seen map[reflect.Type]bool) {
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		fmt.Fprintf(manifest, "wire-type-container:%s\n", value.String())
		value = value.Elem()
	}
	if seen[value] {
		return
	}
	seen[value] = true
	fmt.Fprintf(manifest, "wire-type:%s:%s:%s\n", value.PkgPath(), value.Name(), value.Kind())
	if value.Kind() != reflect.Struct {
		return
	}
	for index := range value.NumField() {
		field := value.Field(index)
		fmt.Fprintf(manifest, "wire-field:%s:%s:%s\n", field.Name, field.Type.String(), field.Tag.Get("json"))
		appendSchemaType(manifest, field.Type, seen)
	}
}

func renderServerHandler(b *strings.Builder, operation operation) {
	lower := lowerFirst(operation.name)
	fmt.Fprintf(b, "func (s *server) handle%s(ctx context.Context, request wire.Request) (any, error) {\n", operation.name)
	fmt.Fprintf(b, "var input %sRequest\n", lower)
	fmt.Fprintf(b, "if err := decodePayload(request.Payload, &input); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	switch operation.name {
	case "ReleaseUnclaimedContent":
		fmt.Fprintf(b, "if err := validateReleaseUnclaimedContentRequest(input.Refs); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "TopologyHead":
		fmt.Fprintf(b, "if err := catalog.ValidateSourceAuthorityFleetOwnerID(input.Owner); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "TopologySnapshot", "TopologyChangesSince", "WaitTopologyChanges":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "PageFileProviderDomains", "PageFileProviderDomainRemovals":
		fmt.Fprintf(b, "if err := validateFileProviderDomainPageRequest(input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceObserverStream":
		fmt.Fprintf(b, "if err := validateSourceAuthority(input.Authority); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "BeginSourceObserverConfiguration":
		fmt.Fprintf(b, "if err := validateSourceObserverConfigurationIdentity(input.Identity); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AppendSourceObserverConfigurationRoots":
		fmt.Fprintf(b, "if err := validateSourceObserverRootAppendPage(input.Authority, input.Operation, input.Page); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AppendSourceObserverConfigurationCheckpoints":
		fmt.Fprintf(b, "if err := validateSourceObserverCheckpointAppendPage(input.Authority, input.Operation, input.Page); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "CommitSourceObserverConfiguration", "AcknowledgeSourceObserverConfiguration":
		fmt.Fprintf(b, "if err := validateSourceObserverConfigurationRef(input.Ref, input.Ref.Authority, input.Ref.Operation); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AbortSourceObserverConfiguration":
		fmt.Fprintf(b, "if err := validateSourceObserverConfigurationRequest(input.Authority, input.Operation); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceObserverRootsPage", "SourceObserverCheckpointsPage":
		fmt.Fprintf(b, "if err := validateSourceObserverPageRequest(input.Authority, input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceMutationExpectation":
		fmt.Fprintf(b, "if err := validateSourceMutationExpectationRequest(input.Authority, input.Operation); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceMutationExpectationsPage":
		fmt.Fprintf(b, "if err := validateSourceMutationExpectationsPageRequest(input.Authority, input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceObserverInboxPage":
		fmt.Fprintf(b, "if err := validateSourceObserverInboxPageRequest(input.Authority, input.AfterExclusive, input.ThroughInclusive, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AppendSourceObserverInbox":
		fmt.Fprintf(b, "if err := validateAppendSourceObserverInboxRecord(input.Record); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "PutSourceMutationExpectation":
		fmt.Fprintf(b, "if err := validatePutSourceMutationExpectation(input.Record); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "CompleteSourceMutationExpectation", "RecoverSourceMutationExpectationReceipt":
		fmt.Fprintf(b, "if err := validateCompleteSourceMutationExpectation(input.Authority, input.Operation, input.Receipt); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SettleSourceObserver":
		fmt.Fprintf(b, "if err := validateSourceObserverSettlement(input.Settlement); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AcknowledgeSourceObserverSettlement":
		fmt.Fprintf(b, "if err := validateSourceObserverSettlementAcknowledgement(input.Ref); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "PromoteSourceSnapshot":
		fmt.Fprintf(b, "if err := validateSourceSnapshotSettlement(input.Ref, input.Settlement); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AppendSourceSnapshotStagePage":
		fmt.Fprintf(b, "if err := validateSourceSnapshotStageAppendRequest(input.Authority, input.Snapshot, input.Page); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceSnapshotStagePage":
		fmt.Fprintf(b, "if err := validateSourceSnapshotStagePageRequest(input.Authority, input.Snapshot, input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceSnapshotStageLookup":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceSnapshotRootLookup":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceObserverBindingForKey":
		fmt.Fprintf(b, "if err := validateSourceBindingRequest(input.Authority, input.Key); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceAuthorityBindingLookup":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "ResolveCriticalObjects":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceObserverBindingIndexPage":
		fmt.Fprintf(b, "if err := validateSourceBindingIndexPageRequest(input.Authority, input.Key, input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourcePhysicalIndexLookup":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourcePhysicalIndexRecordsPage":
		fmt.Fprintf(b, "if err := validateSourcePhysicalIndexPageRequest(input.Authority, input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourcePhysicalIndexRecordByIdentity":
		fmt.Fprintf(b, "if err := validateSourcePhysicalIdentityRequest(input.Authority, input.Identity); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceAuthorityFleetHead":
		fmt.Fprintf(b, "if err := catalog.ValidateSourceAuthorityFleetOwnerID(input.Owner); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "PublishDesiredSourceFleet", "DesiredSourceFleetPage":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceAuthorityFleetPage":
		fmt.Fprintf(b, "if err := validateSourceAuthorityFleetPageRequest(input.Request); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "ReconcileSourceAuthorityFleet":
		fmt.Fprintf(b, "if err := validateSourceAuthorityFleetReconcileRequest(input.Request); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AbortSourceAuthorityFleet":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "RetireSourceAuthority":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AcknowledgeSourceAuthorityFleet":
		fmt.Fprintf(b, "if err := input.Acknowledgement.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceAuthorityRuntimeStatus":
		fmt.Fprintf(b, "if err := input.Ref.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "TakeoverSourceAuthorityRuntime":
		fmt.Fprintf(b, "if err := input.Takeover.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "OpenSourceAuthorityRuntime", "CloseSourceAuthorityRuntime":
		fmt.Fprintf(b, "if err := input.Fence.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "BeginRecoverReapedSourceAuthorityRuntimes":
		fmt.Fprintf(b, "if err := validateSourceAuthorityReapReceipt(input.Receipt); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AcknowledgeSourceAuthorityRuntimeRecovery":
		fmt.Fprintf(b, "if err := validateSourceAuthorityRuntimeRecoveryFloor(input.Floor); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "SourceAuthorityRuntimeRecoveryPage":
		fmt.Fprintf(b, "if err := input.Request.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "InspectStorageQuarantine":
		fmt.Fprintf(b, "if err := validateStorageQuarantinePageRequest(input.After, input.Limit); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "ResolveStorageQuarantine":
		fmt.Fprintf(b, "if err := validateStorageQuarantineResolutionRequest(input.TransitionID, input.Token, input.Resolution); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	case "AcknowledgeStorageQuarantineResolution":
		fmt.Fprintf(b, "if err := input.Receipt.Validate(); err != nil { return encodeResponse(%sResponse{Header: decodeError(err)}) }\n", lower)
	}
	fmt.Fprintf(b, "response := %sResponse{Header: s.response(input.Header)}\n", lower)
	b.WriteString("if response.Header.Error == nil {\n")
	if len(operation.response) > 0 {
		b.WriteString("var callErr error\n")
		for index, field := range operation.response {
			if index > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(b, "response.%s", field.name)
		}
		b.WriteString(", callErr = ")
	} else {
		b.WriteString("response.Header.Error = encodeRemoteError(")
	}
	fmt.Fprintf(b, "s.store.%s(ctx", operation.name)
	for _, field := range operation.request {
		fmt.Fprintf(b, ", input.%s", field.name)
	}
	b.WriteString(")")
	if len(operation.response) == 0 {
		b.WriteString(")")
	} else {
		switch operation.name {
		case "PageFileProviderDomains", "PageFileProviderDomainRemovals":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.After, input.Limit) }")
		case "TopologyHead":
			b.WriteString("\nif callErr == nil { callErr = response.Head.Validate(input.Owner) }")
		case "TopologySnapshot":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "TopologyChangesSince", "WaitTopologyChanges":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "SourceObserverStream":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverStreamRecord(response.Record, input.Authority) }")
		case "AppendSourceObserverConfigurationRoots":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverRootAppendResult(response.Ref, input.Authority, input.Operation, input.Page) }")
		case "AppendSourceObserverConfigurationCheckpoints":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverCheckpointAppendResult(response.Ref, input.Authority, input.Operation, input.Page) }")
		case "CommitSourceObserverConfiguration":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverStreamRecord(response.Record, input.Ref.Authority) }")
		case "SourceObserverRootsPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverRootPage(response.Page, input.After, input.Limit) }")
		case "SourceObserverCheckpointsPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverCheckpointPage(response.Page, input.After, input.Limit) }")
		case "SourceMutationExpectation":
			b.WriteString("\nif callErr == nil { callErr = validateSourceMutationExpectationRecord(response.Record, input.Authority, input.Operation) }")
		case "SourceMutationExpectationsPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceMutationExpectationPage(response.Page, input.Authority, input.After, input.Limit) }")
		case "SourceObserverNextInbox":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverNextInbox(response.Record, input.Authority, input.After) }")
		case "SourceObserverInboxPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceObserverInboxPage(response.Page, input.Authority, input.AfterExclusive, input.ThroughInclusive, input.Limit) }")
		case "SourceSnapshotStagePage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceSnapshotStagePage(response.Page, input.Authority, input.After, input.Limit) }")
		case "SourceSnapshotStageLookup":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "SourceSnapshotRootLookup":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "SourceObserverBindingForKey":
			b.WriteString("\nif callErr == nil { callErr = validateSourceAuthorityBinding(response.Binding, input.Authority, input.Key) }")
		case "SourceAuthorityBindingLookup":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "ResolveCriticalObjects":
			b.WriteString("\nif callErr == nil { callErr = response.Resolution.Validate(input.Request) }")
		case "SourceObserverBindingIndexPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourcePhysicalIndexPage(response.Page, input.Authority, input.After, input.Limit) }")
		case "SourcePhysicalIndexLookup":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "SourcePhysicalIndexRecordsPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourcePhysicalIndexPage(response.Page, input.Authority, input.After, input.Limit) }")
		case "SourcePhysicalIndexRecordByIdentity":
			b.WriteString("\nif callErr == nil { callErr = validateSourcePhysicalIndexRecordIdentity(response.Record, input.Authority, input.Identity) }")
		case "SourceAuthorityFleetHead":
			b.WriteString("\nif callErr == nil { callErr = validateSourceAuthorityFleetStatus(response.Status, input.Owner) }")
		case "PublishDesiredSourceFleet":
			b.WriteString("\nif callErr == nil { callErr = response.State.Validate() }")
		case "DesiredSourceFleetPage":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "SourceAuthorityFleetPage":
			b.WriteString("\nif callErr == nil { callErr = validateSourceAuthorityFleetPage(response.Page, input.Request) }")
		case "ReconcileSourceAuthorityFleet":
			b.WriteString("\nif callErr == nil { callErr = validateSourceAuthorityFleetReconcileState(response.State, input.Request) }")
		case "AbortSourceAuthorityFleet":
			b.WriteString("\nif callErr == nil { callErr = response.Receipt.Validate(input.Request) }")
		case "RetireSourceAuthority":
			b.WriteString("\nif callErr == nil { callErr = response.Receipt.Validate(input.Request) }")
		case "AcknowledgeSourceAuthorityFleet":
			b.WriteString("\nif callErr == nil { callErr = validateSourceAuthorityFleetAcknowledgementState(response.State, input.Acknowledgement) }")
		case "SourceAuthorityRuntimeStatus":
			b.WriteString("\nif callErr == nil { callErr = response.State.Validate(input.Ref) }")
		case "BeginRecoverReapedSourceAuthorityRuntimes":
			b.WriteString("\nif callErr == nil { callErr = response.Summary.Validate(input.Receipt) }")
		case "SourceAuthorityRuntimeRecoveryPage":
			b.WriteString("\nif callErr == nil { callErr = response.Page.Validate(input.Request) }")
		case "InspectStorageQuarantine":
			b.WriteString("\nif callErr == nil { callErr = validateStorageQuarantinePage(response.Page, input.After, input.Limit) }")
		case "ResolveStorageQuarantine":
			b.WriteString("\nif callErr == nil { callErr = validateStorageQuarantineResolutionReceipt(response.Receipt, input.TransitionID, input.Token, input.Resolution) }")
		}
		b.WriteString("\nresponse.Header.Error = encodeRemoteError(callErr)")
	}
	b.WriteString("\n}\nreturn encodeResponse(response)\n}\n\n")
}

func renderClientMethod(b *strings.Builder, operation operation) {
	fmt.Fprintf(b, "func (c *Client) %s(ctx context.Context", operation.name)
	for _, field := range operation.request {
		fmt.Fprintf(b, ", %s %s", lowerFirst(field.name), field.typeName)
	}
	b.WriteString(") (")
	for _, field := range operation.response {
		fmt.Fprintf(b, "%s, ", field.typeName)
	}
	b.WriteString("error) {\n")
	switch operation.name {
	case "ReleaseUnclaimedContent":
		b.WriteString("if err := validateReleaseUnclaimedContentRequest(refs); err != nil { return err }\n")
	case "TopologyHead":
		b.WriteString("if err := catalog.ValidateSourceAuthorityFleetOwnerID(owner); err != nil { var zero catalog.TopologyHeadState; return zero, err }\n")
	case "TopologySnapshot":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.TopologySnapshotPage; return zero, err }\n")
	case "TopologyChangesSince", "WaitTopologyChanges":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.TopologyChangePage; return zero, err }\n")
	case "PageFileProviderDomains", "PageFileProviderDomainRemovals":
		fmt.Fprintf(b, "if err := validateFileProviderDomainPageRequest(after, limit); err != nil { var zero %s; return zero, err }\n", operation.response[0].typeName)
	case "SourceObserverStream":
		b.WriteString("if err := validateSourceAuthority(authority); err != nil { var zero catalog.SourceObserverStreamRecord; return zero, err }\n")
	case "BeginSourceObserverConfiguration":
		b.WriteString("if err := validateSourceObserverConfigurationIdentity(identity); err != nil { return err }\n")
	case "AppendSourceObserverConfigurationRoots":
		b.WriteString("if err := validateSourceObserverRootAppendPage(authority, operation, page); err != nil { var zero catalog.SourceObserverConfigurationRef; return zero, err }\n")
	case "AppendSourceObserverConfigurationCheckpoints":
		b.WriteString("if err := validateSourceObserverCheckpointAppendPage(authority, operation, page); err != nil { var zero catalog.SourceObserverConfigurationRef; return zero, err }\n")
	case "CommitSourceObserverConfiguration":
		b.WriteString("if err := validateSourceObserverConfigurationRef(ref, ref.Authority, ref.Operation); err != nil { var zero catalog.SourceObserverStreamRecord; return zero, err }\n")
	case "AcknowledgeSourceObserverConfiguration":
		b.WriteString("if err := validateSourceObserverConfigurationRef(ref, ref.Authority, ref.Operation); err != nil { return err }\n")
	case "AbortSourceObserverConfiguration":
		b.WriteString("if err := validateSourceObserverConfigurationRequest(authority, operation); err != nil { return err }\n")
	case "SourceObserverRootsPage":
		b.WriteString("if err := validateSourceObserverPageRequest(authority, after, limit); err != nil { var zero catalog.SourceObserverRootPage; return zero, err }\n")
	case "SourceObserverCheckpointsPage":
		b.WriteString("if err := validateSourceObserverPageRequest(authority, after, limit); err != nil { var zero catalog.SourceObserverCheckpointPage; return zero, err }\n")
	case "SourceMutationExpectation":
		b.WriteString("if err := validateSourceMutationExpectationRequest(authority, operation); err != nil { var zero catalog.SourceMutationExpectationRecord; return zero, err }\n")
	case "SourceMutationExpectationsPage":
		b.WriteString("if err := validateSourceMutationExpectationsPageRequest(authority, after, limit); err != nil { var zero catalog.SourceMutationExpectationPage; return zero, err }\n")
	case "SourceObserverInboxPage":
		b.WriteString("if err := validateSourceObserverInboxPageRequest(authority, afterExclusive, throughInclusive, limit); err != nil { var zero catalog.SourceObserverInboxPage; return zero, err }\n")
	case "AppendSourceObserverInbox":
		b.WriteString("if err := validateAppendSourceObserverInboxRecord(record); err != nil { var zero uint64; return zero, err }\n")
	case "PutSourceMutationExpectation":
		b.WriteString("if err := validatePutSourceMutationExpectation(record); err != nil { return err }\n")
	case "CompleteSourceMutationExpectation", "RecoverSourceMutationExpectationReceipt":
		b.WriteString("if err := validateCompleteSourceMutationExpectation(authority, operation, receipt); err != nil { return err }\n")
	case "SettleSourceObserver":
		b.WriteString("if err := validateSourceObserverSettlement(settlement); err != nil { return err }\n")
	case "AcknowledgeSourceObserverSettlement":
		b.WriteString("if err := validateSourceObserverSettlementAcknowledgement(ref); err != nil { return err }\n")
	case "PromoteSourceSnapshot":
		b.WriteString("if err := validateSourceSnapshotSettlement(ref, settlement); err != nil { var zero catalog.SourceResult; return zero, err }\n")
	case "AppendSourceSnapshotStagePage":
		b.WriteString("if err := validateSourceSnapshotStageAppendRequest(authority, snapshot, page); err != nil { return err }\n")
	case "SourceSnapshotStagePage":
		b.WriteString("if err := validateSourceSnapshotStagePageRequest(authority, snapshot, after, limit); err != nil { var zero catalog.SourceSnapshotPage; return zero, err }\n")
	case "SourceSnapshotStageLookup":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceSnapshotPhysicalLookupPage; return zero, err }\n")
	case "SourceSnapshotRootLookup":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceSnapshotRootLookupPage; return zero, err }\n")
	case "SourceObserverBindingForKey":
		b.WriteString("if err := validateSourceBindingRequest(authority, key); err != nil { var zero catalog.SourceAuthorityBindingRecord; return zero, err }\n")
	case "SourceAuthorityBindingLookup":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceAuthorityBindingLookupPage; return zero, err }\n")
	case "ResolveCriticalObjects":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.CriticalObjectResolution; return zero, err }\n")
	case "SourceObserverBindingIndexPage":
		b.WriteString("if err := validateSourceBindingIndexPageRequest(authority, key, after, limit); err != nil { var zero catalog.SourcePhysicalIndexPage; return zero, err }\n")
	case "SourcePhysicalIndexLookup":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourcePhysicalIndexLookupPage; return zero, err }\n")
	case "SourcePhysicalIndexRecordsPage":
		b.WriteString("if err := validateSourcePhysicalIndexPageRequest(authority, after, limit); err != nil { var zero catalog.SourcePhysicalIndexPage; return zero, err }\n")
	case "SourcePhysicalIndexRecordByIdentity":
		b.WriteString("if err := validateSourcePhysicalIdentityRequest(authority, identity); err != nil { var zero catalog.SourcePhysicalIndexRecord; return zero, err }\n")
	case "SourceAuthorityFleetHead":
		b.WriteString("if err := catalog.ValidateSourceAuthorityFleetOwnerID(owner); err != nil { var zero catalog.SourceAuthorityFleetStatus; return zero, err }\n")
	case "PublishDesiredSourceFleet":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.DesiredSourceAuthorityFleetState; return zero, err }\n")
	case "DesiredSourceFleetPage":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.DesiredSourceFleetPage; return zero, err }\n")
	case "SourceAuthorityFleetPage":
		b.WriteString("if err := validateSourceAuthorityFleetPageRequest(request); err != nil { var zero catalog.SourceAuthorityFleetPage; return zero, err }\n")
	case "ReconcileSourceAuthorityFleet":
		b.WriteString("if err := validateSourceAuthorityFleetReconcileRequest(request); err != nil { var zero catalog.SourceAuthorityFleetReconcileState; return zero, err }\n")
	case "AbortSourceAuthorityFleet":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceAuthorityFleetAbortReceipt; return zero, err }\n")
	case "RetireSourceAuthority":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceAuthorityRetirementReceipt; return zero, err }\n")
	case "AcknowledgeSourceAuthorityFleet":
		b.WriteString("if err := acknowledgement.Validate(); err != nil { var zero catalog.SourceAuthorityFleetState; return zero, err }\n")
	case "SourceAuthorityRuntimeStatus":
		b.WriteString("if err := ref.Validate(); err != nil { var zero catalog.SourceAuthorityRuntimeState; return zero, err }\n")
	case "TakeoverSourceAuthorityRuntime":
		b.WriteString("if err := takeover.Validate(); err != nil { return err }\n")
	case "OpenSourceAuthorityRuntime", "CloseSourceAuthorityRuntime":
		b.WriteString("if err := fence.Validate(); err != nil { return err }\n")
	case "BeginRecoverReapedSourceAuthorityRuntimes":
		b.WriteString("if err := validateSourceAuthorityReapReceipt(receipt); err != nil { var zero catalog.SourceAuthorityRuntimeRecoverySummary; return zero, err }\n")
	case "AcknowledgeSourceAuthorityRuntimeRecovery":
		b.WriteString("if err := validateSourceAuthorityRuntimeRecoveryFloor(floor); err != nil { return err }\n")
	case "SourceAuthorityRuntimeRecoveryPage":
		b.WriteString("if err := request.Validate(); err != nil { var zero catalog.SourceAuthorityRuntimeRecoveryPage; return zero, err }\n")
	case "InspectStorageQuarantine":
		b.WriteString("if err := validateStorageQuarantinePageRequest(after, limit); err != nil { var zero catalog.StorageQuarantinePage; return zero, err }\n")
	case "ResolveStorageQuarantine":
		b.WriteString("if err := validateStorageQuarantineResolutionRequest(transitionID, token, resolution); err != nil { var zero catalog.StorageQuarantineResolutionReceipt; return zero, err }\n")
	case "AcknowledgeStorageQuarantineResolution":
		b.WriteString("if err := receipt.Validate(); err != nil { return err }\n")
	}
	b.WriteString("header, err := c.header()\nif err != nil {")
	writeZeroReturn(b, operation.response, "err")
	b.WriteString("}\n")
	lower := lowerFirst(operation.name)
	fmt.Fprintf(b, "response, err := call[%sResponse](ctx, c.wire, Operation%s, %sRequest{Header: header", lower, operation.name, lower)
	for _, field := range operation.request {
		fmt.Fprintf(b, ", %s: %s", field.name, lowerFirst(field.name))
	}
	b.WriteString("})\nif err := validateResponse(header, response.Header, err); err != nil {")
	writeZeroReturn(b, operation.response, "err")
	b.WriteString("}\n")
	switch operation.name {
	case "PageFileProviderDomains", "PageFileProviderDomainRemovals":
		b.WriteString("if err := response.Page.Validate(after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "TopologyHead":
		b.WriteString("if err := response.Head.Validate(owner); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "TopologySnapshot":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "TopologyChangesSince", "WaitTopologyChanges":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverStream":
		b.WriteString("if err := validateSourceObserverStreamRecord(response.Record, authority); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "AppendSourceObserverConfigurationRoots":
		b.WriteString("if err := validateSourceObserverRootAppendResult(response.Ref, authority, operation, page); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "AppendSourceObserverConfigurationCheckpoints":
		b.WriteString("if err := validateSourceObserverCheckpointAppendResult(response.Ref, authority, operation, page); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "CommitSourceObserverConfiguration":
		b.WriteString("if err := validateSourceObserverStreamRecord(response.Record, ref.Authority); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverRootsPage":
		b.WriteString("if err := validateSourceObserverRootPage(response.Page, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverCheckpointsPage":
		b.WriteString("if err := validateSourceObserverCheckpointPage(response.Page, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceMutationExpectation":
		b.WriteString("if err := validateSourceMutationExpectationRecord(response.Record, authority, operation); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceMutationExpectationsPage":
		b.WriteString("if err := validateSourceMutationExpectationPage(response.Page, authority, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverNextInbox":
		b.WriteString("if err := validateSourceObserverNextInbox(response.Record, authority, after); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverInboxPage":
		b.WriteString("if err := validateSourceObserverInboxPage(response.Page, authority, afterExclusive, throughInclusive, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceSnapshotStagePage":
		b.WriteString("if err := validateSourceSnapshotStagePage(response.Page, authority, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceSnapshotStageLookup":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceSnapshotRootLookup":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverBindingForKey":
		b.WriteString("if err := validateSourceAuthorityBinding(response.Binding, authority, key); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceAuthorityBindingLookup":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "ResolveCriticalObjects":
		b.WriteString("if err := response.Resolution.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceObserverBindingIndexPage":
		b.WriteString("if err := validateSourcePhysicalIndexPage(response.Page, authority, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourcePhysicalIndexLookup":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourcePhysicalIndexRecordsPage":
		b.WriteString("if err := validateSourcePhysicalIndexPage(response.Page, authority, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourcePhysicalIndexRecordByIdentity":
		b.WriteString("if err := validateSourcePhysicalIndexRecordIdentity(response.Record, authority, identity); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceAuthorityFleetHead":
		b.WriteString("if err := validateSourceAuthorityFleetStatus(response.Status, owner); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "PublishDesiredSourceFleet":
		b.WriteString("if err := response.State.Validate(); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "DesiredSourceFleetPage":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceAuthorityFleetPage":
		b.WriteString("if err := validateSourceAuthorityFleetPage(response.Page, request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "ReconcileSourceAuthorityFleet":
		b.WriteString("if err := validateSourceAuthorityFleetReconcileState(response.State, request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "AbortSourceAuthorityFleet":
		b.WriteString("if err := response.Receipt.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "RetireSourceAuthority":
		b.WriteString("if err := response.Receipt.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "AcknowledgeSourceAuthorityFleet":
		b.WriteString("if err := validateSourceAuthorityFleetAcknowledgementState(response.State, acknowledgement); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "InspectStorageQuarantine":
		b.WriteString("if err := validateStorageQuarantinePage(response.Page, after, limit); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "ResolveStorageQuarantine":
		b.WriteString("if err := validateStorageQuarantineResolutionReceipt(response.Receipt, transitionID, token, resolution); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceAuthorityRuntimeStatus":
		b.WriteString("if err := response.State.Validate(ref); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "BeginRecoverReapedSourceAuthorityRuntimes":
		b.WriteString("if err := response.Summary.Validate(receipt); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	case "SourceAuthorityRuntimeRecoveryPage":
		b.WriteString("if err := response.Page.Validate(request); err != nil {")
		writeZeroReturn(b, operation.response, "err")
		b.WriteString("}\n")
	}
	b.WriteString("return ")
	for _, field := range operation.response {
		fmt.Fprintf(b, "response.%s, ", field.name)
	}
	b.WriteString("nil\n}\n\n")
}

func renderManagerMethod(b *strings.Builder, operation operation) {
	fmt.Fprintf(b, "func (m *Manager) %s(ctx context.Context", operation.name)
	for _, field := range operation.request {
		fmt.Fprintf(b, ", %s %s", lowerFirst(field.name), field.typeName)
	}
	b.WriteString(") (")
	for _, field := range operation.response {
		fmt.Fprintf(b, "%s, ", field.typeName)
	}
	b.WriteString("error) {\n")
	if operation.name == "WaitTopologyChanges" {
		fmt.Fprintf(b, "return managerWaitCall(m, ctx, func(client *Client) (%s, error) { return client.", operation.response[0].typeName)
	} else if len(operation.response) == 0 {
		b.WriteString("_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) { return struct{}{}, client.")
	} else {
		fmt.Fprintf(b, "return managerCall(m, ctx, func(client *Client) (%s, error) { return client.", operation.response[0].typeName)
	}
	fmt.Fprintf(b, "%s(ctx", operation.name)
	for _, field := range operation.request {
		fmt.Fprintf(b, ", %s", lowerFirst(field.name))
	}
	b.WriteString(") })\n")
	if len(operation.response) == 0 {
		b.WriteString("return err\n")
	}
	b.WriteString("}\n\n")
}

func writeZeroReturn(b *strings.Builder, fields []field, errName string) {
	for _, field := range fields {
		fmt.Fprintf(b, "var zero%s %s; ", field.name, field.typeName)
	}
	b.WriteString("return ")
	for _, field := range fields {
		fmt.Fprintf(b, "zero%s, ", field.name)
	}
	fmt.Fprintf(b, "%s", errName)
}

func writeFields(b *strings.Builder, fields []field) {
	for _, field := range fields {
		fmt.Fprintf(b, "%s %s `json:\"%s\"`\n", field.name, field.typeName, field.jsonName)
	}
}

func lowerFirst(value string) string { return strings.ToLower(value[:1]) + value[1:] }
