package catalogworker

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestReleaseRequestCountIsValidatedBeforeDispatch(t *testing.T) {
	exact := make([]catalog.ContentRef, catalog.ReleaseUnclaimedContentLimit)
	if err := validateReleaseUnclaimedContentRequest(exact); err != nil {
		t.Fatalf("exact release request: %v", err)
	}
	if err := validateReleaseUnclaimedContentRequest(append(exact, catalog.ContentRef{})); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit release request = %v, want ErrInvalidObject", err)
	}
}

func TestStorageQuarantineWireBoundsAndExactReceipt(t *testing.T) {
	var after catalog.StorageTransitionID
	if err := validateStorageQuarantinePageRequest(
		after, catalog.MaintenancePageLimit,
	); err != nil {
		t.Fatalf("exact quarantine page request: %v", err)
	}
	for _, limit := range []int{0, catalog.MaintenancePageLimit + 1} {
		if err := validateStorageQuarantinePageRequest(
			after, limit,
		); !errors.Is(err, catalog.ErrInvalidObject) {
			t.Fatalf("quarantine page limit %d = %v, want invalid object", limit, err)
		}
	}
	if err := validateStorageQuarantinePage(
		catalog.StorageQuarantinePage{More: true}, after, 1,
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("empty continuing quarantine page = %v, want integrity", err)
	}
	id := catalog.StorageTransitionID{1}
	token := catalog.StorageQuarantineToken{1}
	receipt := catalog.StorageQuarantineResolutionReceipt{
		ID: id, Token: token, Resolution: catalog.StorageQuarantineRetry,
		OutcomeDigest: [32]byte{1},
	}
	if err := validateStorageQuarantineResolutionReceipt(
		receipt, id, token, catalog.StorageQuarantineRetry,
	); err != nil {
		t.Fatalf("exact quarantine receipt: %v", err)
	}
	tampered := receipt
	tampered.OutcomeDigest[0] ^= 0xff
	if err := validateStorageQuarantineResolutionReceipt(
		tampered, id, token, catalog.StorageQuarantineDiscard,
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("mismatched quarantine receipt = %v, want integrity", err)
	}
}

func TestPageRequestsHaveExactCountAndCursorBounds(t *testing.T) {
	cursor := catalog.TenantID(strings.Repeat("a", maxWorkerCursorBytes))
	if err := validateFileProviderDomainPageRequest(cursor, catalog.FileProviderDomainPageLimit); err != nil {
		t.Fatalf("exact domain page request: %v", err)
	}
	if err := validateFileProviderDomainPageRequest("", catalog.FileProviderDomainPageLimit+1); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit page count = %v, want ErrInvalidObject", err)
	}
}

func TestOpenAtObjectIsStructurallyBoundedBeforePreambleEncoding(t *testing.T) {
	id := catalog.ObjectID{1}
	object := catalog.Object{
		Tenant: "tenant", ID: id, Parent: catalog.ObjectID{2},
		Revision: 1, MetadataRevision: 1,
		Name: strings.Repeat("a", catalog.MaxNameBytes), Kind: catalog.KindDirectory,
	}
	if err := validateWorkerObject(object); err != nil {
		t.Fatalf("exact worker object: %v", err)
	}
	object.Name += "a"
	if err := validateWorkerObject(object); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("oversized worker object name = %v, want ErrIntegrity", err)
	}
	object.Name = "valid"
	object.Convergence = catalog.Convergence{Desired: 1, Observed: 2}
	if err := validateWorkerObject(object); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("invalid worker convergence = %v, want ErrIntegrity", err)
	}
}

func TestSourceSnapshotAppendPageHasExactCountAndEncodedBounds(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	records := make([]catalog.SourcePhysicalIndexRecord, catalog.SourcePhysicalIndexPageLimit)
	for index := range records {
		records[index] = sourcePhysicalRecordForBounds(authority, fmt.Sprintf("%03d", index), []byte{1})
	}
	if err := validateSourceSnapshotStageAppendRequest(
		authority,
		"snapshot",
		catalog.SourceSnapshotPage{Records: records, Next: sourcePhysicalIndexLocator(records[len(records)-1])},
	); err != nil {
		t.Fatalf("exact source snapshot item count: %v", err)
	}
	records = append(records, sourcePhysicalRecordForBounds(authority, "256", []byte{1}))
	if err := validateSourceSnapshotStageAppendRequest(
		authority,
		"snapshot",
		catalog.SourceSnapshotPage{Records: records, Next: sourcePhysicalIndexLocator(records[len(records)-1])},
	); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit source snapshot item count = %v, want ErrInvalidObject", err)
	}

	payloadBytes := largestValidSourcePayload(t, authority)
	valid := catalog.SourceSnapshotPage{
		Records: []catalog.SourcePhysicalIndexRecord{
			sourcePhysicalRecordForBounds(authority, "record", make([]byte, payloadBytes)),
		},
	}
	valid.Next = sourcePhysicalIndexLocator(valid.Records[0])
	if err := validateSourceSnapshotStageAppendRequest(authority, "snapshot", valid); err != nil {
		t.Fatalf("largest valid source snapshot payload: %v", err)
	}
	valid.Records[0].Payload = make([]byte, payloadBytes+3)
	if err := validateSourceSnapshotStageAppendRequest(authority, "snapshot", valid); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit encoded source snapshot = %v, want ErrInvalidObject", err)
	}
}

func TestSourcePhysicalIndexPageRequiresStrictCursorOrder(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	first := sourcePhysicalRecordForBounds(authority, "first", []byte{1})
	second := sourcePhysicalRecordForBounds(authority, "second", []byte{2})
	page := catalog.SourcePhysicalIndexPage{
		Records: []catalog.SourcePhysicalIndexRecord{first, second},
		Next:    sourcePhysicalIndexLocator(second),
	}
	if err := validateSourcePhysicalIndexPage(page, authority, catalog.SourceIndexLocator{}, 2); err != nil {
		t.Fatalf("valid source physical index page: %v", err)
	}
	page.Next = sourcePhysicalIndexLocator(first)
	if err := validateSourcePhysicalIndexPage(page, authority, catalog.SourceIndexLocator{}, 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("stale source physical index cursor = %v, want ErrIntegrity", err)
	}
	page.Records[0], page.Records[1] = page.Records[1], page.Records[0]
	page.Next = catalog.SourceIndexLocator{}
	if err := validateSourcePhysicalIndexPage(page, authority, catalog.SourceIndexLocator{}, 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unordered source physical index page = %v, want ErrIntegrity", err)
	}
}

func TestSourceKeyedLookupWireBoundsBindExactAuthorityAndScope(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	locators := make([]catalog.SourceIndexLocator, catalog.SourceKeyedLookupLimit)
	logicals := make([]string, catalog.SourceKeyedLookupLimit)
	for index := range locators {
		locators[index] = catalog.SourceIndexLocator{
			RootID: "root", Relative: fmt.Sprintf("entry-%03d", index),
		}
		logicals[index] = fmt.Sprintf("logical-%03d", index)
	}
	physical, err := catalog.NewSourcePhysicalIndexLookupRequest(authority, 0, locators)
	if err != nil {
		t.Fatal(err)
	}
	if err := physical.Validate(); err != nil {
		t.Fatal(err)
	}
	physicalEntries := make([]catalog.SourcePhysicalIndexLookupEntry, len(locators))
	for index, locator := range locators {
		physicalEntries[index].Locator = locator
	}
	physicalPage, err := catalog.NewSourcePhysicalIndexLookupPage(physical, physicalEntries)
	if err != nil {
		t.Fatal(err)
	}
	foreignPhysical, err := catalog.NewSourcePhysicalIndexLookupRequest("foreign", 0, locators)
	if err != nil {
		t.Fatal(err)
	}
	if err := physicalPage.Validate(foreignPhysical); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("cross-authority physical replay = %v, want integrity", err)
	}
	bindings, err := catalog.NewSourceAuthorityBindingLookupRequest(authority, 0, logicals)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindings.Validate(); err != nil {
		t.Fatal(err)
	}
	bindingEntries := make([]catalog.SourceAuthorityBindingLookupEntry, len(logicals))
	for index, logical := range logicals {
		bindingEntries[index].Logical = logical
	}
	bindingPage, err := catalog.NewSourceAuthorityBindingLookupPage(bindings, bindingEntries)
	if err != nil {
		t.Fatal(err)
	}
	foreignBindings, err := catalog.NewSourceAuthorityBindingLookupRequest("foreign", 0, logicals)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindingPage.Validate(foreignBindings); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("cross-authority binding replay = %v, want integrity", err)
	}
	if _, err := catalog.NewSourceSnapshotPhysicalLookupRequest(
		authority, "snapshot", 0, locators,
	); err != nil {
		t.Fatal(err)
	}
	tenants := make([]catalog.TenantID, catalog.SourceKeyedLookupLimit)
	for index := range tenants {
		tenants[index] = catalog.TenantID(fmt.Sprintf("tenant-%03d", index))
	}
	if _, err := catalog.NewSourceSnapshotRootLookupRequest(
		authority, "snapshot", 0, tenants,
	); err != nil {
		t.Fatal(err)
	}
}

func TestSourceAuthorityFleetWireBoundsAndExactProofs(t *testing.T) {
	owner := catalog.SourceAuthorityFleetOwnerID("owner")
	authorities := make([]causal.SourceAuthorityID, catalog.SourceAuthorityFleetPageLimit)
	for index := range authorities {
		authorities[index] = causal.SourceAuthorityID(fmt.Sprintf("authority-%03d", index))
	}
	declarations, digest, declarationsDigest := testSourceAuthorityFleet(t, authorities)
	request := catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Declarations: declarations, Complete: true,
		AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: digest,
		DeclarationsDigest: declarationsDigest,
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("exact 256-authority reconciliation page: %v", err)
	}
	oversized := request
	oversizedAuthorities := append(
		append([]causal.SourceAuthorityID(nil), authorities...), "overflow",
	)
	oversized.Declarations, oversized.AuthoritiesDigest,
		oversized.DeclarationsDigest = testSourceAuthorityFleet(t, oversizedAuthorities)
	oversized.AuthorityCount = uint64(len(oversizedAuthorities))
	if err := oversized.Validate(); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("257-authority reconciliation page = %v, want invalid object", err)
	}
	reconcileAtWidth := func(width int) catalog.SourceAuthorityFleetReconcileRequest {
		t.Helper()
		values := make([]causal.SourceAuthorityID, catalog.SourceAuthorityFleetPageLimit)
		for index := range values {
			prefix := fmt.Sprintf("%03d-", index)
			values[index] = causal.SourceAuthorityID(prefix + strings.Repeat("x", width-len(prefix)))
		}
		valueDeclarations, valueDigest, valueDeclarationsDigest :=
			testSourceAuthorityFleet(t, values)
		return catalog.SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: valueDeclarations, Complete: true,
			AuthorityCount: uint64(len(values)), AuthoritiesDigest: valueDigest,
			DeclarationsDigest: valueDeclarationsDigest,
		}
	}
	low, high := 16, causal.SourceAuthorityIDMaxBytes
	for low < high {
		middle := low + (high-low+1)/2
		if validateSourceAuthorityFleetReconcileRequest(reconcileAtWidth(middle)) == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	exactBytes := reconcileAtWidth(low)
	if err := validateSourceAuthorityFleetReconcileRequest(exactBytes); err != nil {
		t.Fatalf("largest valid encoded reconciliation page: %v", err)
	}
	overBytes := exactBytes
	overBytes.Declarations = append(
		[]catalog.SourceAuthorityDeclaration(nil), exactBytes.Declarations...,
	)
	overBytes.Declarations[len(overBytes.Declarations)-1].Authority =
		causal.SourceAuthorityID(strings.Repeat("x", causal.SourceAuthorityIDMaxBytes+1))
	if err := overBytes.Validate(); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("authority-id max+1 reconciliation page = %v, want invalid object", err)
	}
	emptyDigest, err := catalog.SourceAuthorityFleetDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyDeclarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	empty := catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Complete: true, AuthoritiesDigest: emptyDigest,
		DeclarationsDigest: emptyDeclarationsDigest,
	}
	if err := empty.Validate(); err != nil {
		t.Fatalf("empty desired fleet: %v", err)
	}
	pageRequest := catalog.SourceAuthorityFleetPageRequest{
		Owner: owner, Generation: 1, Limit: catalog.SourceAuthorityFleetPageLimit,
	}
	if err := pageRequest.Validate(); err != nil {
		t.Fatalf("exact fleet page request: %v", err)
	}
	fleetPage := catalog.SourceAuthorityFleetPage{
		Fleet: catalog.SourceAuthorityFleetState{
			Owner: owner, Generation: 1, AuthorityCount: uint64(len(authorities)),
			AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
			AcknowledgementDigest: [32]byte{1},
		},
		Declarations: declarations, Next: authorities[len(authorities)-1],
	}
	if err := validateSourceAuthorityFleetPage(fleetPage, pageRequest); err != nil {
		t.Fatalf("exact fleet page response: %v", err)
	}
	foreignPageRequest := pageRequest
	foreignPageRequest.Owner = "foreign"
	if err := validateSourceAuthorityFleetPage(fleetPage, foreignPageRequest); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("cross-owner fleet page replay = %v, want mutation conflict", err)
	}
	stalePageRequest := pageRequest
	stalePageRequest.After = authorities[0]
	if err := validateSourceAuthorityFleetPage(fleetPage, stalePageRequest); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("stale fleet page cursor = %v, want invalid object", err)
	}
	pageRequest.Limit++
	if err := pageRequest.Validate(); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit fleet page request = %v, want invalid object", err)
	}
	reconcileState := catalog.SourceAuthorityFleetReconcileState{
		Owner: owner, Generation: 1, NextSequence: 1,
		ReceivedCount: uint64(len(authorities)), AuthorityCount: uint64(len(authorities)),
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		StageSeed: [32]byte{2}, StageDigest: [32]byte{1}, Complete: true,
	}
	if err := reconcileState.ValidateRequest(request); err != nil {
		t.Fatalf("exact reconciliation response: %v", err)
	}
	acknowledgement := catalog.SourceAuthorityFleetAcknowledgement{
		Owner: owner, Generation: 1, AuthorityCount: uint64(len(authorities)),
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		StageDigest: reconcileState.StageDigest,
	}
	acknowledgementDigest, err := catalog.SourceAuthorityFleetAcknowledgementDigest(acknowledgement)
	if err != nil {
		t.Fatal(err)
	}
	state := catalog.SourceAuthorityFleetState{
		Owner: owner, Generation: 1, AuthorityCount: uint64(len(authorities)),
		AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
		AcknowledgementDigest: acknowledgementDigest,
	}
	if err := validateSourceAuthorityFleetAcknowledgementState(state, acknowledgement); err != nil {
		t.Fatalf("exact acknowledgement response: %v", err)
	}
	tampered := state
	tampered.Generation++
	if err := validateSourceAuthorityFleetAcknowledgementState(tampered, acknowledgement); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("tampered acknowledgement response = %v, want integrity", err)
	}
	fence := catalog.SourceAuthorityRuntimeFence{
		Owner: owner, Generation: 1, Authority: authorities[0], Epoch: [16]byte{1},
	}
	if err := fence.Validate(); err != nil {
		t.Fatalf("exact runtime fence: %v", err)
	}
	retire := catalog.SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authorities[0], StageDigest: [32]byte{1},
	}
	receipt := catalog.SourceAuthorityRetirementReceipt{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: authorities[0], StageDigest: [32]byte{1}, ReceiptDigest: [32]byte{2},
	}
	if err := receipt.Validate(retire); err != nil {
		t.Fatalf("exact retirement receipt: %v", err)
	}
	retire.Authority = authorities[1]
	if err := receipt.Validate(retire); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("cross-authority retirement replay = %v, want mutation conflict", err)
	}
}

func TestSourceObserverReceiptAcknowledgementsAreExactAndBounded(t *testing.T) {
	settlement := catalog.SourcePublicationStageRef{
		Authority: "authority", Operation: causal.OperationID{1},
		Sequence: 1, Items: 1, Bytes: 1, Digest: [32]byte{1},
	}
	if err := validateSourceObserverSettlementAcknowledgement(settlement); err != nil {
		t.Fatalf("exact source observer settlement acknowledgement: %v", err)
	}
	settlement.Items = 0
	if err := validateSourceObserverSettlementAcknowledgement(settlement); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("incomplete source observer settlement acknowledgement = %v, want invalid object", err)
	}
}

func TestSourceSnapshotStageReadsAreBoundedBeforeDispatch(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	if err := validateSourceSnapshotStagePageRequest(
		authority,
		"snapshot",
		catalog.SourceIndexLocator{},
		catalog.SourcePhysicalIndexPageLimit,
	); err != nil {
		t.Fatalf("exact source snapshot read page request: %v", err)
	}
	if err := validateSourceSnapshotStagePageRequest(
		authority,
		"snapshot",
		catalog.SourceIndexLocator{},
		catalog.SourcePhysicalIndexPageLimit+1,
	); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit source snapshot read page = %v, want ErrInvalidObject", err)
	}
	if err := validateSourcePhysicalIdentityRequest(authority, make([]byte, maxSourceIdentityBytes)); err != nil {
		t.Fatalf("exact source physical identity request: %v", err)
	}
	if err := validateSourcePhysicalIdentityRequest(authority, make([]byte, maxSourceIdentityBytes+1)); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit source physical identity request = %v, want ErrInvalidObject", err)
	}
}

func TestSourceMutationExpectationPageRequiresStrictCursorOrder(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	first := sourceMutationExpectationForBounds(authority, catalog.MutationID{1})
	second := sourceMutationExpectationForBounds(authority, catalog.MutationID{2})
	page := catalog.SourceMutationExpectationPage{
		Records: []catalog.SourceMutationExpectationRecord{first, second},
		Next:    second.Operation,
	}
	if err := validateSourceMutationExpectationPage(page, authority, catalog.MutationID{}, 2); err != nil {
		t.Fatalf("valid source mutation expectation page: %v", err)
	}
	page.Next = first.Operation
	if err := validateSourceMutationExpectationPage(page, authority, catalog.MutationID{}, 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("stale source mutation expectation cursor = %v, want ErrIntegrity", err)
	}
	page.Records[0], page.Records[1] = page.Records[1], page.Records[0]
	page.Next = catalog.MutationID{}
	if err := validateSourceMutationExpectationPage(page, authority, catalog.MutationID{}, 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unordered source mutation expectation page = %v, want ErrIntegrity", err)
	}
}

func TestSourceObserverConfigurationWireBoundsAreExactAndOrdered(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	operation := causal.OperationID{1}
	identity := catalog.SourceObserverConfigurationIdentity{
		Authority: authority, FleetOwner: "owner", FleetGeneration: 1,
		Operation: operation, Stream: "stream", RootEpoch: "epoch",
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
		RootCount: maxSourceObserverConfigurationRecords, CheckpointCount: maxSourceObserverConfigurationRecords,
		RootsDigest: [32]byte{3}, CheckpointsDigest: [32]byte{4},
	}
	if err := validateSourceObserverConfigurationIdentity(identity); err != nil {
		t.Fatalf("maximum configuration identity: %v", err)
	}
	identity.RootCount++
	if err := validateSourceObserverConfigurationIdentity(identity); err == nil {
		t.Fatal("root count max+1 was accepted")
	}

	pageForPathLength := func(length int) catalog.SourceObserverRootAppendPage {
		records := make([]catalog.SourceObserverRootRecord, catalog.SourceObserverConfigurationPageLimit)
		for index := range records {
			records[index] = catalog.SourceObserverRootRecord{
				ID: fmt.Sprintf("root-%04d", index), Generation: 1,
				Path: strings.Repeat("x", length), VolumeUUID: "volume",
				Inode: uint64(index + 1), Kind: 1,
			}
		}
		return catalog.SourceObserverRootAppendPage{Records: records}
	}
	low, high := 1, maxSourceWorkerPathBytes
	for low < high {
		middle := low + (high-low+1)/2
		if validateSourceObserverRootAppendPage(authority, operation, pageForPathLength(middle)) == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	exact := pageForPathLength(low)
	if err := validateSourceObserverRootAppendPage(authority, operation, exact); err != nil {
		t.Fatalf("largest encoded root page rejected: %v", err)
	}
	if low == maxSourceWorkerPathBytes {
		t.Fatal("root page did not reach its encoded byte limit before its string limit")
	}
	if err := validateSourceObserverRootAppendPage(authority, operation, pageForPathLength(low+1)); err == nil {
		t.Fatal("encoded root page max+1 was accepted")
	}
	exact.Records[1].ID = exact.Records[0].ID
	if err := validateSourceObserverRootAppendPage(authority, operation, exact); err == nil {
		t.Fatal("unordered root page was accepted")
	}

	checkpoints := make([]catalog.SourceObserverCheckpointRecord, catalog.SourceObserverConfigurationPageLimit+1)
	for index := range checkpoints {
		checkpoints[index] = catalog.SourceObserverCheckpointRecord{
			Stream: fmt.Sprintf("stream-%04d", index), RootEpoch: "epoch",
		}
	}
	if err := validateSourceObserverCheckpointAppendPage(authority, operation, catalog.SourceObserverCheckpointAppendPage{
		Records: checkpoints,
	}); err == nil {
		t.Fatal("checkpoint page item max+1 was accepted")
	}
}

func TestSourceObserverConfigurationProofsAndReadPagesAreFenced(t *testing.T) {
	t.Parallel()
	authority := causal.SourceAuthorityID("authority")
	operation := causal.OperationID{1}
	rootPage := catalog.SourceObserverRootAppendPage{Sequence: 0, Records: []catalog.SourceObserverRootRecord{{
		ID: "a", Generation: 1, Path: "/a", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}}
	encodedRoot, err := json.Marshal(rootPage)
	if err != nil {
		t.Fatal(err)
	}
	ref := catalog.SourceObserverConfigurationRef{
		Authority: authority, Operation: operation, Sequence: 1, Roots: 1,
		Bytes: uint64(len(encodedRoot)), Digest: [32]byte{1},
	}
	for iteration := 0; iteration < 2; iteration++ {
		if err := validateSourceObserverRootAppendResult(ref, authority, operation, rootPage); err != nil {
			t.Fatalf("valid root append proof replay %d: %v", iteration, err)
		}
	}
	ref.Sequence++
	if err := validateSourceObserverRootAppendResult(ref, authority, operation, rootPage); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("wrong root append sequence = %v, want ErrIntegrity", err)
	}
	ref.Sequence, ref.Bytes = 1, maxSourceObserverConfigurationBytes+1
	if err := validateSourceObserverRootAppendResult(ref, authority, operation, rootPage); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("oversized root append proof = %v, want ErrIntegrity", err)
	}
	ref.Bytes, ref.Digest = uint64(len(encodedRoot)), [32]byte{}
	if err := validateSourceObserverRootAppendResult(ref, authority, operation, rootPage); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("digest-free root append proof = %v, want ErrIntegrity", err)
	}

	checkpointAppend := catalog.SourceObserverCheckpointAppendPage{
		Sequence: 1,
		Records: []catalog.SourceObserverCheckpointRecord{{
			Stream: "stream", RootEpoch: "epoch",
		}},
	}
	encodedCheckpoint, err := json.Marshal(checkpointAppend)
	if err != nil {
		t.Fatal(err)
	}
	checkpointRef := catalog.SourceObserverConfigurationRef{
		Authority: authority, Operation: operation, Sequence: 2, Roots: 1, Checkpoints: 1,
		Bytes: uint64(len(encodedRoot) + len(encodedCheckpoint)), Digest: [32]byte{2},
	}
	if err := validateSourceObserverCheckpointAppendResult(
		checkpointRef, authority, operation, checkpointAppend,
	); err != nil {
		t.Fatalf("valid checkpoint append proof: %v", err)
	}

	page := catalog.SourceObserverRootPage{
		Records: []catalog.SourceObserverRootRecord{
			{ID: "a", Generation: 1, Path: "/a", VolumeUUID: "volume", Inode: 1, Kind: 1},
			{ID: "b", Generation: 1, Path: "/b", VolumeUUID: "volume", Inode: 2, Kind: 1},
		},
		Next: "b",
	}
	if err := validateSourceObserverRootPage(page, "", 2); err != nil {
		t.Fatalf("valid root read page: %v", err)
	}
	page.Next = "a"
	if err := validateSourceObserverRootPage(page, "", 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("stale root page cursor = %v, want ErrIntegrity", err)
	}
	page.Next = ""
	page.Records[0], page.Records[1] = page.Records[1], page.Records[0]
	if err := validateSourceObserverRootPage(page, "", 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unordered root read page = %v, want ErrIntegrity", err)
	}

	checkpointPage := catalog.SourceObserverCheckpointPage{
		Records: []catalog.SourceObserverCheckpointRecord{
			{Stream: "a", RootEpoch: "epoch"},
			{Stream: "b", RootEpoch: "epoch"},
		},
		Next: "b",
	}
	if err := validateSourceObserverCheckpointPage(checkpointPage, "", 2); err != nil {
		t.Fatalf("valid checkpoint read page: %v", err)
	}
	checkpointPage.Records[0], checkpointPage.Records[1] =
		checkpointPage.Records[1], checkpointPage.Records[0]
	checkpointPage.Next = ""
	if err := validateSourceObserverCheckpointPage(checkpointPage, "", 2); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unordered checkpoint read page = %v, want ErrIntegrity", err)
	}
}

func TestSourceSnapshotSettlementIsScalarAndExactlyFenced(t *testing.T) {
	t.Parallel()
	ref := catalog.SourceSnapshotStageRef{
		Authority: "authority", Snapshot: "snapshot", FenceDigest: [32]byte{1},
		Digest: [32]byte{2}, Operation: causal.OperationID{1}, Revision: 1,
	}
	settlement := catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: ref.Authority, Stream: "stream", RootEpoch: "epoch",
			Through: 1, Operation: ref.Operation,
		},
		Snapshot: ref,
	}
	if err := validateSourceSnapshotSettlement(ref, settlement); err != nil {
		t.Fatalf("valid source snapshot settlement: %v", err)
	}
	settlement.Snapshot.Digest = [32]byte{3}
	if err := validateSourceSnapshotSettlement(ref, settlement); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("mismatched source snapshot settlement = %v, want ErrInvalidObject", err)
	}
}

func TestSourceObserverAppliedCheckpointWireBounds(t *testing.T) {
	valid := func() catalog.SourceObserverAppliedCheckpointPage {
		return catalog.SourceObserverAppliedCheckpointPage{
			Records: []catalog.SourceObserverAppliedCheckpointRecord{
				{Stream: "a", RootEpoch: "epoch-a", EventID: 1, ReceivedEventID: 2, Sequence: 2},
				{Stream: "b", RootEpoch: "epoch-b", EventID: 1, ReceivedEventID: 1, Sequence: 3},
			},
			LastReceived: 3,
			Next:         "b",
		}
	}
	if err := validateSourceObserverAppliedCheckpointPage(valid(), "", 2); err != nil {
		t.Fatalf("valid applied-checkpoint page: %v", err)
	}
	tests := map[string]func(*catalog.SourceObserverAppliedCheckpointPage){
		"applied beyond received": func(page *catalog.SourceObserverAppliedCheckpointPage) {
			page.Records[0].EventID = page.Records[0].ReceivedEventID + 1
		},
		"sequence beyond authority head": func(page *catalog.SourceObserverAppliedCheckpointPage) {
			page.Records[1].Sequence = page.LastReceived + 1
		},
		"unordered": func(page *catalog.SourceObserverAppliedCheckpointPage) {
			page.Records[0], page.Records[1] = page.Records[1], page.Records[0]
			page.Next = ""
		},
		"forged cursor": func(page *catalog.SourceObserverAppliedCheckpointPage) {
			page.Next = "a"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			page := valid()
			mutate(&page)
			if err := validateSourceObserverAppliedCheckpointPage(page, "", 2); !errors.Is(err, catalog.ErrIntegrity) {
				t.Fatalf("invalid applied-checkpoint page = %v, want ErrIntegrity", err)
			}
		})
	}
}

func TestSourceObserverInboxWireBoundsAreEncodedAndContinuous(t *testing.T) {
	record := catalog.SourceObserverInboxRecord{
		Authority: "authority", Stream: "stream", RootEpoch: "epoch",
		Sequence: 1, EventCount: 1, NativeCursor: 1,
	}
	maximum := maxEncodedPayload(t, func(payload []byte) error {
		candidate := record
		candidate.Payload = payload
		candidate.Digest = sha256.Sum256(payload)
		return validateSourceObserverInboxRecord(candidate)
	})
	record.Payload = make([]byte, maximum)
	record.Digest = sha256.Sum256(record.Payload)
	if err := validateSourceObserverInboxRecord(record); err != nil {
		t.Fatalf("exact encoded inbox limit rejected: %v", err)
	}
	if err := validateAppendSourceObserverInboxRecord(record); err == nil {
		t.Fatal("append accepted a catalog-owned sequence")
	}
	appendRecord := record
	appendRecord.Sequence = 0
	if err := validateAppendSourceObserverInboxRecord(appendRecord); err != nil {
		t.Fatalf("sequence-free append record rejected: %v", err)
	}
	record.Payload = make([]byte, maximum+1)
	record.Digest = sha256.Sum256(record.Payload)
	if err := validateSourceObserverInboxRecord(record); err == nil {
		t.Fatal("encoded inbox max+1 was accepted")
	}
	first := record
	first.Payload = []byte("first")
	first.Digest = sha256.Sum256(first.Payload)
	second := first
	second.Sequence, second.PredecessorSequence = 2, 1
	second.Payload = []byte("second")
	second.Digest = sha256.Sum256(second.Payload)
	page := catalog.SourceObserverInboxPage{Records: []catalog.SourceObserverInboxRecord{first, second}}
	if err := validateSourceObserverInboxPage(page, "authority", 0, 2, 2); err != nil {
		t.Fatal(err)
	}
	page.Records[1].PredecessorSequence = 0
	if err := validateSourceObserverInboxPage(page, "authority", 0, 2, 2); err == nil {
		t.Fatal("non-continuous inbox page was accepted")
	}
	page.Records[1].Sequence, page.Records[1].PredecessorSequence = 3, 2
	if err := validateSourceObserverInboxPage(page, "authority", 0, 3, 2); err != nil {
		t.Fatalf("sparse retained inbox page rejected: %v", err)
	}
}

func TestSourceMutationReceiptWireBoundIsExact(t *testing.T) {
	authority := causal.SourceAuthorityID("authority")
	operation := catalog.MutationID{1}
	maximum := maxEncodedPayload(t, func(payload []byte) error {
		return validateCompleteSourceMutationExpectation(authority, operation, payload)
	})
	if err := validateCompleteSourceMutationExpectation(authority, operation, make([]byte, maximum)); err != nil {
		t.Fatalf("exact encoded receipt limit rejected: %v", err)
	}
	if err := validateCompleteSourceMutationExpectation(authority, operation, make([]byte, maximum+1)); err == nil {
		t.Fatal("encoded receipt max+1 was accepted")
	}
}

func maxEncodedPayload(t *testing.T, validate func([]byte) error) int {
	t.Helper()
	low, high := 1, maxSourceWorkerPageBytes
	for low < high {
		middle := low + (high-low+1)/2
		if validate(make([]byte, middle)) == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return low
}

func largestValidSourcePayload(t *testing.T, authority causal.SourceAuthorityID) int {
	t.Helper()
	low, high := 1, maxSourceWorkerPageBytes
	for low < high {
		middle := low + (high-low+1)/2
		page := catalog.SourceSnapshotPage{
			Records: []catalog.SourcePhysicalIndexRecord{
				sourcePhysicalRecordForBounds(authority, "record", make([]byte, middle)),
			},
		}
		page.Next = sourcePhysicalIndexLocator(page.Records[0])
		if validateSourceSnapshotStageAppendRequest(authority, "snapshot", page) == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	return low
}

func sourcePhysicalRecordForBounds(
	authority causal.SourceAuthorityID,
	relative string,
	payload []byte,
) catalog.SourcePhysicalIndexRecord {
	return catalog.SourcePhysicalIndexRecord{
		Authority: authority, RootID: "root", Relative: relative,
		FileIdentity: []byte{1}, Kind: 1, Payload: payload,
	}
}

func sourceMutationExpectationForBounds(
	authority causal.SourceAuthorityID,
	operation catalog.MutationID,
) catalog.SourceMutationExpectationRecord {
	payload := []byte("payload")
	return catalog.SourceMutationExpectationRecord{
		Operation: operation, Authority: authority, Tenant: "tenant", Generation: 1,
		Digest: sha256.Sum256(payload), Payload: payload,
		State: catalog.SourceMutationExpectationPlanned,
	}
}
