package catalogworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

const (
	maxWorkerCursorBytes     = 255
	maxSourceWorkerPathBytes = 4096
	maxSourceIdentityBytes   = 4096
	maxSourceWorkerPageBytes = catalog.SourcePhysicalIndexPageByteLimit

	maxSourceObserverConfigurationRecords = 10_000
	maxSourceObserverConfigurationBytes   = 64 << 20
	maxSourceObserverConfigurationPages   = 2 * ((maxSourceObserverConfigurationRecords + catalog.SourceObserverConfigurationPageLimit - 1) / catalog.SourceObserverConfigurationPageLimit)
)

func validateSourceObserverConfigurationIdentity(identity catalog.SourceObserverConfigurationIdentity) error {
	if err := validateSourceAuthority(identity.Authority); err != nil {
		return err
	}
	if identity.Operation == (causal.OperationID{}) ||
		!validSourceWorkerText(identity.Stream, maxWorkerCursorBytes) ||
		!validSourceWorkerText(identity.RootEpoch, maxWorkerCursorBytes) ||
		identity.RootDigest == ([32]byte{}) || identity.FleetDigest == ([32]byte{}) ||
		identity.RootCount == 0 || identity.RootCount > maxSourceObserverConfigurationRecords ||
		identity.CheckpointCount == 0 || identity.CheckpointCount > maxSourceObserverConfigurationRecords ||
		identity.RootsDigest == ([32]byte{}) || identity.CheckpointsDigest == ([32]byte{}) {
		return fmt.Errorf("%w: source observer configuration identity is invalid", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(identity)
}

func validateSourceObserverConfigurationRequest(authority causal.SourceAuthorityID, operation causal.OperationID) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if operation == (causal.OperationID{}) {
		return fmt.Errorf("%w: source observer configuration operation is missing", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverRootAppendPage(
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page catalog.SourceObserverRootAppendPage,
) error {
	if err := validateSourceObserverConfigurationRequest(authority, operation); err != nil {
		return err
	}
	if page.Sequence >= maxSourceObserverConfigurationPages ||
		len(page.Records) == 0 || len(page.Records) > catalog.SourceObserverConfigurationPageLimit {
		return fmt.Errorf("%w: source observer root page exceeds its count limit", catalog.ErrInvalidObject)
	}
	for index, record := range page.Records {
		if err := validateSourceObserverRootRecord(record); err != nil {
			return err
		}
		if index > 0 && page.Records[index-1].ID >= record.ID {
			return fmt.Errorf("%w: source observer root page is not strictly ordered", catalog.ErrInvalidObject)
		}
	}
	return validateEncodedSourceValue(page)
}

func validateSourceObserverCheckpointAppendPage(
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page catalog.SourceObserverCheckpointAppendPage,
) error {
	if err := validateSourceObserverConfigurationRequest(authority, operation); err != nil {
		return err
	}
	if page.Sequence >= maxSourceObserverConfigurationPages ||
		len(page.Records) == 0 || len(page.Records) > catalog.SourceObserverConfigurationPageLimit {
		return fmt.Errorf("%w: source observer checkpoint page exceeds its count limit", catalog.ErrInvalidObject)
	}
	for index, record := range page.Records {
		if err := validateSourceObserverCheckpointRecord(record); err != nil {
			return err
		}
		if index > 0 && page.Records[index-1].Stream >= record.Stream {
			return fmt.Errorf("%w: source observer checkpoint page is not strictly ordered", catalog.ErrInvalidObject)
		}
	}
	return validateEncodedSourceValue(page)
}

func validateSourceObserverConfigurationRef(
	ref catalog.SourceObserverConfigurationRef,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) error {
	if err := validateSourceObserverConfigurationRequest(authority, operation); err != nil {
		return err
	}
	if ref.Authority != authority || ref.Operation != operation ||
		ref.Sequence == 0 || ref.Sequence > maxSourceObserverConfigurationPages ||
		ref.Roots > maxSourceObserverConfigurationRecords ||
		ref.Checkpoints > maxSourceObserverConfigurationRecords ||
		ref.Roots+ref.Checkpoints == 0 ||
		ref.Bytes == 0 || ref.Bytes > maxSourceObserverConfigurationBytes ||
		ref.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: source observer configuration ref is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(ref)
}

func validateSourceObserverRootAppendResult(
	ref catalog.SourceObserverConfigurationRef,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page catalog.SourceObserverRootAppendPage,
) error {
	if err := validateSourceObserverConfigurationRef(ref, authority, operation); err != nil {
		return err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return err
	}
	if ref.Sequence != page.Sequence+1 || ref.Roots < uint64(len(page.Records)) ||
		ref.Checkpoints != 0 || ref.Bytes < uint64(len(encoded)) {
		return fmt.Errorf("%w: source observer root append proof is invalid", catalog.ErrIntegrity)
	}
	return nil
}

func validateSourceObserverCheckpointAppendResult(
	ref catalog.SourceObserverConfigurationRef,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	page catalog.SourceObserverCheckpointAppendPage,
) error {
	if err := validateSourceObserverConfigurationRef(ref, authority, operation); err != nil {
		return err
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return err
	}
	if ref.Sequence != page.Sequence+1 || ref.Roots == 0 ||
		ref.Checkpoints < uint64(len(page.Records)) || ref.Bytes < uint64(len(encoded)) {
		return fmt.Errorf("%w: source observer checkpoint append proof is invalid", catalog.ErrIntegrity)
	}
	return nil
}

func validateSourceObserverPageRequest(authority causal.SourceAuthorityID, after string, limit int) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if len(after) > maxWorkerCursorBytes || !utf8.ValidString(after) || strings.IndexByte(after, 0) >= 0 ||
		limit < 1 || limit > catalog.SourceObserverConfigurationPageLimit {
		return fmt.Errorf("%w: source observer page request is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverRootPage(
	page catalog.SourceObserverRootPage,
	after string,
	limit int,
) error {
	if len(page.Records) > limit || len(page.Records) > catalog.SourceObserverConfigurationPageLimit {
		return fmt.Errorf("%w: source observer root page exceeds its item limit", catalog.ErrIntegrity)
	}
	previous := after
	for _, record := range page.Records {
		if err := validateSourceObserverRootRecord(record); err != nil {
			return fmt.Errorf("%w: invalid source observer root: %v", catalog.ErrIntegrity, err)
		}
		if record.ID <= previous {
			return fmt.Errorf("%w: source observer root page escaped its cursor", catalog.ErrIntegrity)
		}
		previous = record.ID
	}
	if page.Next != "" && (len(page.Records) == 0 || page.Next != previous) {
		return fmt.Errorf("%w: source observer root page cursor is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(page)
}

func validateSourceObserverCheckpointPage(
	page catalog.SourceObserverCheckpointPage,
	after string,
	limit int,
) error {
	if len(page.Records) > limit || len(page.Records) > catalog.SourceObserverConfigurationPageLimit {
		return fmt.Errorf("%w: source observer checkpoint page exceeds its item limit", catalog.ErrIntegrity)
	}
	previous := after
	for _, record := range page.Records {
		if err := validateSourceObserverCheckpointRecord(record); err != nil {
			return fmt.Errorf("%w: invalid source observer checkpoint: %v", catalog.ErrIntegrity, err)
		}
		if record.Stream <= previous {
			return fmt.Errorf("%w: source observer checkpoint page escaped its cursor", catalog.ErrIntegrity)
		}
		previous = record.Stream
	}
	if page.Next != "" && (len(page.Records) == 0 || page.Next != previous) {
		return fmt.Errorf("%w: source observer checkpoint page cursor is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(page)
}

func validateSourceObserverRootRecord(record catalog.SourceObserverRootRecord) error {
	if !validSourceWorkerText(record.ID, maxWorkerCursorBytes) ||
		!validSourceWorkerText(record.Path, maxSourceWorkerPathBytes) ||
		!validSourceWorkerText(record.VolumeUUID, maxWorkerCursorBytes) ||
		record.Generation == 0 || record.Inode == 0 || record.Kind < 1 || record.Kind > 2 ||
		record.BirthNsec < 0 || record.BirthNsec >= 1_000_000_000 {
		return fmt.Errorf("%w: source observer root is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverCheckpointRecord(record catalog.SourceObserverCheckpointRecord) error {
	if !validSourceWorkerText(record.Stream, maxWorkerCursorBytes) ||
		!validSourceWorkerText(record.RootEpoch, maxWorkerCursorBytes) {
		return fmt.Errorf("%w: source observer checkpoint is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverStreamRecord(record catalog.SourceObserverStreamRecord, authority causal.SourceAuthorityID) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if record.Authority != authority ||
		!validSourceWorkerText(record.Stream, maxWorkerCursorBytes) ||
		!validSourceWorkerText(record.RootEpoch, maxWorkerCursorBytes) ||
		record.RootDigest == ([32]byte{}) || record.FleetDigest == ([32]byte{}) ||
		record.Mode < catalog.SourceObserverSnapshotRequired ||
		record.Mode > catalog.SourceObserverStreamResetRequired ||
		len(record.Quarantine) > maxSourceWorkerPathBytes || !utf8.ValidString(record.Quarantine) ||
		strings.IndexByte(record.Quarantine, 0) >= 0 {
		return fmt.Errorf("%w: source observer stream record is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(record)
}

func validateSourceObserverInboxRecord(record catalog.SourceObserverInboxRecord) error {
	if err := validateSourceAuthority(record.Authority); err != nil {
		return err
	}
	if !validSourceWorkerText(record.Stream, maxWorkerCursorBytes) ||
		!validSourceWorkerText(record.RootEpoch, maxWorkerCursorBytes) ||
		record.EventCount == 0 || len(record.Payload) == 0 ||
		sha256.Sum256(record.Payload) != record.Digest ||
		(record.Sequence == 0 && record.PredecessorSequence != 0) ||
		(record.Sequence != 0 && record.PredecessorSequence != record.Sequence-1) {
		return fmt.Errorf("%w: source observer inbox record is invalid", catalog.ErrInvalidObject)
	}
	discontinuity := record.NativeCursor == 0 && record.NativePredecessor == 0
	if !discontinuity && record.NativePredecessor >= record.NativeCursor {
		return fmt.Errorf("%w: source observer inbox cursor is invalid", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(record)
}

func validateAppendSourceObserverInboxRecord(record catalog.SourceObserverInboxRecord) error {
	if err := validateSourceObserverInboxRecord(record); err != nil {
		return err
	}
	if record.Sequence != 0 || record.PredecessorSequence != 0 {
		return fmt.Errorf("%w: source observer inbox append supplied catalog-owned sequence", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverInboxPageRequest(
	authority causal.SourceAuthorityID,
	afterExclusive, throughInclusive uint64,
	limit int,
) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if throughInclusive < afterExclusive || limit < 1 || limit > catalog.SourceObserverInboxPageLimit {
		return fmt.Errorf("%w: source observer inbox page request is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceObserverInboxPage(
	page catalog.SourceObserverInboxPage,
	authority causal.SourceAuthorityID,
	afterExclusive, throughInclusive uint64,
	limit int,
) error {
	if len(page.Records) > limit || len(page.Records) > catalog.SourceObserverInboxPageLimit {
		return fmt.Errorf("%w: source observer inbox page exceeds item limit", catalog.ErrIntegrity)
	}
	previous := afterExclusive
	for _, record := range page.Records {
		if record.Authority != authority || record.Sequence <= previous ||
			record.Sequence != previous+1 || record.Sequence > throughInclusive ||
			record.PredecessorSequence != previous {
			return fmt.Errorf("%w: source observer inbox page is not continuous", catalog.ErrIntegrity)
		}
		if err := validateSourceObserverInboxRecord(record); err != nil {
			return fmt.Errorf("%w: invalid source observer inbox record: %v", catalog.ErrIntegrity, err)
		}
		previous = record.Sequence
	}
	if page.Next != 0 && (len(page.Records) == 0 || page.Next != previous) {
		return fmt.Errorf("%w: source observer inbox cursor is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(page)
}

func validateSourceObserverNextInbox(
	record *catalog.SourceObserverInboxRecord,
	authority causal.SourceAuthorityID,
	after uint64,
) error {
	if record == nil {
		return nil
	}
	if record.Authority != authority || record.Sequence <= after {
		return fmt.Errorf("%w: source observer next inbox record escaped its cursor", catalog.ErrIntegrity)
	}
	return validateSourceObserverInboxRecord(*record)
}

func validatePutSourceMutationExpectation(record catalog.SourceMutationExpectationRecord) error {
	return validateSourceMutationExpectationRecord(record, record.Authority, record.Operation)
}

func validateCompleteSourceMutationExpectation(
	authority causal.SourceAuthorityID,
	operation catalog.MutationID,
	receipt []byte,
) error {
	if err := validateSourceMutationExpectationRequest(authority, operation); err != nil {
		return err
	}
	if len(receipt) == 0 || len(receipt) > maxSourceWorkerPageBytes {
		return fmt.Errorf("%w: source mutation receipt exceeds its byte limit", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(struct {
		Authority causal.SourceAuthorityID `json:"authority"`
		Operation catalog.MutationID       `json:"operation"`
		Receipt   []byte                   `json:"receipt"`
	}{Authority: authority, Operation: operation, Receipt: receipt})
}

func validateSourceObserverSettlement(settlement catalog.SourceObserverSettlement) error {
	if err := validateSourceAuthority(settlement.Authority); err != nil {
		return err
	}
	if !validSourceWorkerText(settlement.Stream, maxWorkerCursorBytes) ||
		!validSourceWorkerText(settlement.RootEpoch, maxWorkerCursorBytes) ||
		settlement.Through == 0 || settlement.Operation == (causal.OperationID{}) {
		return fmt.Errorf("%w: source observer settlement is not scalar", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(settlement)
}

func validateSourceObserverSettlementAcknowledgement(ref catalog.SourcePublicationStageRef) error {
	if err := validateSourceAuthority(ref.Authority); err != nil {
		return err
	}
	if ref.Operation == (causal.OperationID{}) ||
		ref.Sequence == 0 || ref.Items == 0 || ref.Bytes == 0 ||
		ref.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete source observer settlement acknowledgement", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(ref)
}

func validateSourceSnapshotSettlement(
	ref catalog.SourceSnapshotStageRef,
	settlement catalog.SourceSnapshotSettlement,
) error {
	if err := validateSourceObserverSettlement(settlement.Fence); err != nil {
		return err
	}
	if ref.Authority == "" || !validSourceWorkerText(ref.Snapshot, maxWorkerCursorBytes) ||
		ref.FenceDigest == ([32]byte{}) || ref.Digest == ([32]byte{}) ||
		ref.Operation == (causal.OperationID{}) || ref.Revision == 0 ||
		settlement.Snapshot != ref ||
		settlement.Fence.Authority != ref.Authority ||
		settlement.Fence.Operation != ref.Operation {
		return fmt.Errorf("%w: source snapshot settlement does not match its stage", catalog.ErrInvalidObject)
	}
	return validateEncodedSourceValue(settlement)
}

func validSourceWorkerText(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && strings.IndexByte(value, 0) < 0
}

func validateReleaseUnclaimedContentRequest(refs []catalog.ContentRef) error {
	if len(refs) > catalog.ReleaseUnclaimedContentLimit {
		return fmt.Errorf("%w: release request exceeds %d refs", catalog.ErrInvalidObject, catalog.ReleaseUnclaimedContentLimit)
	}
	return nil
}

func validateFileProviderDomainPageRequest(after catalog.TenantID, limit int) error {
	if len(after) > maxWorkerCursorBytes || strings.ContainsRune(string(after), 0) {
		return fmt.Errorf("%w: File Provider domain cursor is invalid", catalog.ErrInvalidObject)
	}
	if limit < 1 || limit > catalog.FileProviderDomainPageLimit {
		return fmt.Errorf("%w: invalid File Provider domain page limit", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceMutationExpectationRequest(authority causal.SourceAuthorityID, operation catalog.MutationID) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if operation == (catalog.MutationID{}) {
		return fmt.Errorf("%w: source mutation operation is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceMutationExpectationsPageRequest(
	authority causal.SourceAuthorityID,
	_ catalog.MutationID,
	limit int,
) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if limit < 1 || limit > catalog.SourceMutationExpectationPageLimit {
		return fmt.Errorf("%w: invalid source mutation expectation page limit", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceMutationExpectationRecord(
	record catalog.SourceMutationExpectationRecord,
	authority causal.SourceAuthorityID,
	operation catalog.MutationID,
) error {
	if err := validateSourceMutationExpectationRequest(authority, operation); err != nil {
		return err
	}
	if record.Authority != authority || record.Operation != operation || record.Tenant == "" ||
		!validSourceWorkerText(string(record.Tenant), maxWorkerCursorBytes) ||
		(record.Origin.Domain != "" && !validSourceWorkerText(string(record.Origin.Domain), maxWorkerCursorBytes)) ||
		record.Generation == 0 || len(record.Payload) == 0 ||
		sha256.Sum256(record.Payload) != record.Digest ||
		record.State < catalog.SourceMutationExpectationPlanned ||
		record.State > catalog.SourceMutationExpectationRepairPublished {
		return fmt.Errorf("%w: source mutation expectation is invalid", catalog.ErrIntegrity)
	}
	if len(record.Receipt) == 0 {
		if record.ReceiptDigest != ([32]byte{}) {
			return fmt.Errorf("%w: source mutation receipt is invalid", catalog.ErrIntegrity)
		}
	} else if sha256.Sum256(record.Receipt) != record.ReceiptDigest {
		return fmt.Errorf("%w: source mutation receipt is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(record)
}

func validateSourceMutationExpectationPage(
	page catalog.SourceMutationExpectationPage,
	authority causal.SourceAuthorityID,
	after catalog.MutationID,
	limit int,
) error {
	if len(page.Records) > limit || len(page.Records) > catalog.SourceMutationExpectationPageLimit {
		return fmt.Errorf("%w: source mutation expectation page exceeds item limit", catalog.ErrIntegrity)
	}
	previous := after
	for _, record := range page.Records {
		if bytes.Compare(record.Operation[:], previous[:]) <= 0 {
			return fmt.Errorf("%w: source mutation expectation page is not strictly ordered", catalog.ErrIntegrity)
		}
		if err := validateSourceMutationExpectationRecord(record, authority, record.Operation); err != nil {
			return err
		}
		previous = record.Operation
	}
	if page.Next != (catalog.MutationID{}) &&
		(len(page.Records) == 0 || page.Next != page.Records[len(page.Records)-1].Operation) {
		return fmt.Errorf("%w: source mutation expectation cursor is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(page)
}

func validateSourceSnapshotStageAppendRequest(
	authority causal.SourceAuthorityID,
	snapshot string,
	page catalog.SourceSnapshotPage,
) error {
	if err := validateSourceSnapshotIdentity(authority, snapshot); err != nil {
		return err
	}
	if len(page.Records) == 0 || len(page.Records) > catalog.SourcePhysicalIndexPageLimit {
		return fmt.Errorf("%w: source snapshot stage page exceeds item limit", catalog.ErrInvalidObject)
	}
	var previous catalog.SourceIndexLocator
	for index, record := range page.Records {
		locator := sourcePhysicalIndexLocator(record)
		if index > 0 && compareSourceIndexLocator(previous, locator) >= 0 {
			return fmt.Errorf("%w: source snapshot stage page is not strictly ordered", catalog.ErrInvalidObject)
		}
		if err := validateSourcePhysicalIndexRecord(record, authority, locator); err != nil {
			return fmt.Errorf("%w: invalid source snapshot stage record: %v", catalog.ErrInvalidObject, err)
		}
		previous = locator
	}
	if page.Next != previous {
		return fmt.Errorf("%w: source snapshot stage cursor is invalid", catalog.ErrInvalidObject)
	}
	if err := validateSourceIndexLocator(page.Next, false); err != nil {
		return err
	}
	return validateEncodedSourceValue(page)
}

func validateSourceSnapshotStagePageRequest(
	authority causal.SourceAuthorityID,
	snapshot string,
	after catalog.SourceIndexLocator,
	limit int,
) error {
	if err := validateSourceSnapshotIdentity(authority, snapshot); err != nil {
		return err
	}
	return validateSourcePhysicalIndexPageRequest(authority, after, limit)
}

func validateSourceSnapshotStagePage(
	page catalog.SourceSnapshotPage,
	authority causal.SourceAuthorityID,
	after catalog.SourceIndexLocator,
	limit int,
) error {
	return validateSourcePhysicalIndexPage(catalog.SourcePhysicalIndexPage(page), authority, after, limit)
}

func validateSourceSnapshotIdentity(authority causal.SourceAuthorityID, snapshot string) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if snapshot == "" || len(snapshot) > maxWorkerCursorBytes || !utf8.ValidString(snapshot) ||
		strings.IndexByte(snapshot, 0) >= 0 {
		return fmt.Errorf("%w: source snapshot identity is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceBindingRequest(authority causal.SourceAuthorityID, key catalog.SourceObjectKey) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	value := string(key)
	if value == "" || len(value) > maxWorkerCursorBytes || !utf8.ValidString(value) ||
		strings.ContainsAny(value, "/\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: source object key is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceBindingIndexPageRequest(
	authority causal.SourceAuthorityID,
	key catalog.SourceObjectKey,
	after catalog.SourceIndexLocator,
	limit int,
) error {
	if err := validateSourceBindingRequest(authority, key); err != nil {
		return err
	}
	return validateSourcePhysicalIndexPageRequest(authority, after, limit)
}

func validateSourceAuthorityBinding(
	record catalog.SourceAuthorityBindingRecord,
	authority causal.SourceAuthorityID,
	key catalog.SourceObjectKey,
) error {
	if record.Authority != authority || record.SourceKey != key || record.LogicalID == "" ||
		len(record.LogicalID) > maxWorkerCursorBytes || !utf8.ValidString(record.LogicalID) ||
		strings.IndexFunc(record.LogicalID, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: source authority binding is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(record)
}

func validateSourcePhysicalIndexPageRequest(
	authority causal.SourceAuthorityID,
	after catalog.SourceIndexLocator,
	limit int,
) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if err := validateSourceIndexLocator(after, true); err != nil {
		return err
	}
	if limit < 1 || limit > catalog.SourcePhysicalIndexPageLimit {
		return fmt.Errorf("%w: invalid source physical index page limit", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourcePhysicalIndexPage(
	page catalog.SourcePhysicalIndexPage,
	authority causal.SourceAuthorityID,
	after catalog.SourceIndexLocator,
	limit int,
) error {
	if len(page.Records) > limit || len(page.Records) > catalog.SourcePhysicalIndexPageLimit {
		return fmt.Errorf("%w: source physical index page exceeds item limit", catalog.ErrIntegrity)
	}
	previous := after
	for _, record := range page.Records {
		locator := sourcePhysicalIndexLocator(record)
		if compareSourceIndexLocator(previous, locator) >= 0 {
			return fmt.Errorf("%w: source physical index page is not strictly ordered", catalog.ErrIntegrity)
		}
		if err := validateSourcePhysicalIndexRecord(record, authority, locator); err != nil {
			return err
		}
		previous = locator
	}
	if page.Next != (catalog.SourceIndexLocator{}) &&
		(len(page.Records) == 0 || page.Next != sourcePhysicalIndexLocator(page.Records[len(page.Records)-1])) {
		return fmt.Errorf("%w: source physical index cursor is invalid", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(page)
}

func validateSourcePhysicalIndexRecord(
	record catalog.SourcePhysicalIndexRecord,
	authority causal.SourceAuthorityID,
	locator catalog.SourceIndexLocator,
) error {
	if record.Authority != authority || sourcePhysicalIndexLocator(record) != locator ||
		len(record.FileIdentity) == 0 || len(record.FileIdentity) > maxSourceIdentityBytes ||
		record.Kind < 1 || record.Kind > 3 || len(record.Payload) == 0 {
		return fmt.Errorf("%w: source physical index record is invalid", catalog.ErrIntegrity)
	}
	if err := validateSourceIndexLocator(locator, false); err != nil {
		return err
	}
	for _, logical := range record.Logical {
		if logical == "" || len(logical) > maxWorkerCursorBytes || !utf8.ValidString(logical) ||
			strings.IndexFunc(logical, unicode.IsControl) >= 0 {
			return fmt.Errorf("%w: source physical logical identity is invalid", catalog.ErrIntegrity)
		}
	}
	return validateEncodedSourceValue(record)
}

func validateSourcePhysicalIdentityRequest(authority causal.SourceAuthorityID, identity []byte) error {
	if err := validateSourceAuthority(authority); err != nil {
		return err
	}
	if len(identity) == 0 || len(identity) > maxSourceIdentityBytes {
		return fmt.Errorf("%w: source physical identity is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourcePhysicalIndexRecordIdentity(
	record catalog.SourcePhysicalIndexRecord,
	authority causal.SourceAuthorityID,
	identity []byte,
) error {
	if !bytes.Equal(record.FileIdentity, identity) {
		return fmt.Errorf("%w: source physical identity mismatch", catalog.ErrIntegrity)
	}
	return validateSourcePhysicalIndexRecord(record, authority, sourcePhysicalIndexLocator(record))
}

func validateSourceAuthority(authority causal.SourceAuthorityID) error {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return fmt.Errorf("%w: source authority is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityFleetStatus(
	status catalog.SourceAuthorityFleetStatus,
	owner catalog.SourceAuthorityFleetOwnerID,
) error {
	if err := catalog.ValidateSourceAuthorityFleetOwnerID(owner); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	if status.Current != nil && status.Current.Owner != owner {
		return fmt.Errorf("%w: source authority fleet current owner mismatch", catalog.ErrIntegrity)
	}
	if status.Pending != nil && status.Pending.Owner != owner {
		return fmt.Errorf("%w: source authority fleet pending owner mismatch", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(status)
}

func validateSourceAuthorityFleetPageRequest(request catalog.SourceAuthorityFleetPageRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return validateEncodedSourceValue(request)
}

func validateSourceAuthorityFleetPage(
	page catalog.SourceAuthorityFleetPage,
	request catalog.SourceAuthorityFleetPageRequest,
) error {
	if err := page.Validate(request); err != nil {
		return err
	}
	return validateEncodedSourceValue(page)
}

func validateSourceAuthorityFleetReconcileRequest(
	request catalog.SourceAuthorityFleetReconcileRequest,
) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return validateEncodedSourceValue(request)
}

func validateStorageQuarantinePageRequest(
	_ catalog.StorageTransitionID,
	limit int,
) error {
	if limit <= 0 || limit > catalog.MaintenancePageLimit {
		return fmt.Errorf("%w: invalid storage quarantine page limit", catalog.ErrInvalidObject)
	}
	return nil
}

func validateStorageQuarantinePage(
	page catalog.StorageQuarantinePage,
	after catalog.StorageTransitionID,
	limit int,
) error {
	if err := validateStorageQuarantinePageRequest(after, limit); err != nil {
		return err
	}
	if len(page.Entries) > limit || (page.More && len(page.Entries) == 0) {
		return fmt.Errorf("%w: invalid storage quarantine page cardinality", catalog.ErrIntegrity)
	}
	previous := after
	for _, entry := range page.Entries {
		if err := entry.Validate(); err != nil {
			return errors.Join(catalog.ErrIntegrity, err)
		}
		if bytes.Compare(entry.ID[:], previous[:]) <= 0 {
			return fmt.Errorf("%w: storage quarantine page is not ordered", catalog.ErrIntegrity)
		}
		previous = entry.ID
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("catalogworker: encode storage quarantine page: %w", err)
	}
	if len(encoded) > catalog.StorageQuarantinePageByteLimit-4096 {
		return fmt.Errorf("%w: storage quarantine page exceeds byte limit", catalog.ErrIntegrity)
	}
	return nil
}

func validateStorageQuarantineResolutionRequest(
	id catalog.StorageTransitionID,
	token catalog.StorageQuarantineToken,
	resolution catalog.StorageQuarantineResolution,
) error {
	if id == (catalog.StorageTransitionID{}) ||
		token == (catalog.StorageQuarantineToken{}) ||
		(resolution != catalog.StorageQuarantineRetry &&
			resolution != catalog.StorageQuarantineDiscard) {
		return fmt.Errorf("%w: invalid storage quarantine resolution", catalog.ErrInvalidObject)
	}
	return nil
}

func validateStorageQuarantineResolutionReceipt(
	receipt catalog.StorageQuarantineResolutionReceipt,
	id catalog.StorageTransitionID,
	token catalog.StorageQuarantineToken,
	resolution catalog.StorageQuarantineResolution,
) error {
	if err := validateStorageQuarantineResolutionRequest(id, token, resolution); err != nil {
		return err
	}
	if err := receipt.Validate(); err != nil {
		return errors.Join(catalog.ErrIntegrity, err)
	}
	if receipt.ID != id || receipt.Token != token || receipt.Resolution != resolution {
		return fmt.Errorf("%w: storage quarantine receipt does not match request", catalog.ErrIntegrity)
	}
	return nil
}

func validateSourceAuthorityReapReceipt(receipt proc.ReapReceipt) error {
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("%w: invalid source authority reap receipt: %v", catalog.ErrInvalidObject, err)
	}
	if receipt.Record.RecoveryClass != proc.RecoverySourceOwner {
		return fmt.Errorf("%w: reap receipt is not for a source owner", catalog.ErrInvalidObject)
	}
	record, err := json.Marshal(receipt.Record)
	if err != nil {
		return fmt.Errorf("catalogworker: encode source authority runtime owner: %w", err)
	}
	if len(record) == 0 || len(record) > catalog.SourceAuthorityRuntimeOwnerByteLimit {
		return fmt.Errorf("%w: source authority runtime owner exceeds byte limit", catalog.ErrInvalidObject)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("catalogworker: encode source authority reap receipt: %w", err)
	}
	if len(encoded) > 32<<10 {
		return fmt.Errorf("%w: source authority reap receipt exceeds byte limit", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityRuntimeRecoveryFloor(floor proc.ReapReceiptFloor) error {
	if floor.LedgerID == (proc.ReceiptLedgerID{}) || floor.Sequence == 0 ||
		floor.RecoveryClass != proc.RecoverySourceOwner {
		return fmt.Errorf("%w: invalid source authority runtime recovery floor", catalog.ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityFleetReconcileState(
	state catalog.SourceAuthorityFleetReconcileState,
	request catalog.SourceAuthorityFleetReconcileRequest,
) error {
	if err := state.ValidateRequest(request); err != nil {
		return err
	}
	return validateEncodedSourceValue(state)
}

func validateSourceAuthorityFleetAcknowledgementState(
	state catalog.SourceAuthorityFleetState,
	acknowledgement catalog.SourceAuthorityFleetAcknowledgement,
) error {
	if err := acknowledgement.Validate(); err != nil {
		return err
	}
	if err := state.Validate(); err != nil {
		return err
	}
	digest, err := catalog.SourceAuthorityFleetAcknowledgementDigest(acknowledgement)
	if err != nil {
		return err
	}
	if state.Owner != acknowledgement.Owner ||
		state.Generation != acknowledgement.Generation ||
		state.AuthorityCount != acknowledgement.AuthorityCount ||
		state.AuthoritiesDigest != acknowledgement.AuthoritiesDigest ||
		state.DeclarationsDigest != acknowledgement.DeclarationsDigest ||
		state.AcknowledgementDigest != digest {
		return fmt.Errorf("%w: source authority fleet acknowledgement state mismatch", catalog.ErrIntegrity)
	}
	return validateEncodedSourceValue(state)
}

func validateSourceIndexLocator(locator catalog.SourceIndexLocator, allowZero bool) error {
	if locator == (catalog.SourceIndexLocator{}) && allowZero {
		return nil
	}
	if locator.RootID == "" || len(locator.RootID) > maxWorkerCursorBytes ||
		locator.Relative == "" || len(locator.Relative) > maxSourceWorkerPathBytes ||
		!utf8.ValidString(locator.RootID) || !utf8.ValidString(locator.Relative) ||
		strings.IndexByte(locator.RootID, 0) >= 0 || strings.IndexByte(locator.Relative, 0) >= 0 {
		return fmt.Errorf("%w: source physical index cursor is invalid", catalog.ErrInvalidObject)
	}
	return nil
}

func sourcePhysicalIndexLocator(record catalog.SourcePhysicalIndexRecord) catalog.SourceIndexLocator {
	return catalog.SourceIndexLocator{RootID: record.RootID, Relative: record.Relative}
}

func compareSourceIndexLocator(left, right catalog.SourceIndexLocator) int {
	if order := strings.Compare(left.RootID, right.RootID); order != 0 {
		return order
	}
	return strings.Compare(left.Relative, right.Relative)
}

func validateEncodedSourceValue(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: encode source worker value: %v", catalog.ErrInvalidObject, err)
	}
	if len(encoded) > maxSourceWorkerPageBytes {
		return fmt.Errorf("%w: source worker value exceeds %d encoded bytes", catalog.ErrInvalidObject, maxSourceWorkerPageBytes)
	}
	return nil
}

func validateWorkerObject(object catalog.Object) error {
	if object.Tenant == "" || len(object.Tenant) > maxWorkerCursorBytes ||
		object.ID == (catalog.ObjectID{}) || object.Parent == (catalog.ObjectID{}) ||
		object.Revision == 0 || object.MetadataRevision == 0 || object.Size < 0 {
		return fmt.Errorf("%w: worker object identity is invalid", catalog.ErrIntegrity)
	}
	if object.Name == "" {
		if object.ID != object.Parent || object.Kind != catalog.KindDirectory {
			return fmt.Errorf("%w: worker object name is invalid", catalog.ErrIntegrity)
		}
	} else if err := validateWorkerName(object.Name); err != nil {
		return err
	}
	switch object.Kind {
	case catalog.KindDirectory:
		if object.ContentRevision != 0 || object.Size != 0 || object.Hash != (catalog.ContentHash{}) || object.LinkTarget != "" {
			return fmt.Errorf("%w: worker directory content is invalid", catalog.ErrIntegrity)
		}
	case catalog.KindFile:
		if object.ContentRevision == 0 || object.LinkTarget != "" {
			return fmt.Errorf("%w: worker file content is invalid", catalog.ErrIntegrity)
		}
	case catalog.KindSymlink:
		if object.ContentRevision == 0 || len(object.LinkTarget) == 0 || len(object.LinkTarget) > 4096 ||
			!utf8.ValidString(object.LinkTarget) || strings.IndexByte(object.LinkTarget, 0) >= 0 ||
			object.Size != int64(len(object.LinkTarget)) || object.Hash != sha256.Sum256([]byte(object.LinkTarget)) {
			return fmt.Errorf("%w: worker symlink content is invalid", catalog.ErrIntegrity)
		}
	default:
		return fmt.Errorf("%w: worker object kind is invalid", catalog.ErrIntegrity)
	}
	if object.Convergence.Applied > object.Convergence.Verified ||
		object.Convergence.Verified > object.Convergence.Observed ||
		object.Convergence.Observed > object.Convergence.Desired {
		return fmt.Errorf("%w: worker object convergence is invalid", catalog.ErrIntegrity)
	}
	return nil
}

func validateWorkerName(name string) error {
	if name == "" || len(name) > catalog.MaxNameBytes || !utf8.ValidString(name) ||
		name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: worker object name is invalid", catalog.ErrIntegrity)
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: worker object name is invalid", catalog.ErrIntegrity)
		}
	}
	return nil
}
