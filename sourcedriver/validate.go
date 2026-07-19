package sourcedriver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// ValidateHead verifies one exact source head.
func ValidateHead(head Head) error { return validateToken(head.Revision, "head revision") }

// ValidateTargetSetRef verifies one exact authority-bound declaration reference.
func ValidateTargetSetRef(authority causal.SourceAuthorityID, ref TargetSetRef) error {
	if err := validateTargetSetRefShape(ref); err != nil {
		return err
	}
	id, err := deriveTargetSetID(authority, ref)
	if err != nil {
		return err
	}
	if id != ref.ID {
		return fmt.Errorf("%w: target set identity differs", ErrIntegrity)
	}
	return nil
}

// ValidateTargetSetState verifies one exact authority-bound resumable acknowledgement.
func ValidateTargetSetState(authority causal.SourceAuthorityID, state TargetSetState) error {
	if err := ValidateTargetSetRef(authority, state.Ref); err != nil {
		return err
	}
	return validateTargetSetStateShape(state)
}

// ValidateTargetSetPage verifies one bounded authority-bound declaration page.
func ValidateTargetSetPage(authority causal.SourceAuthorityID, page TargetSetPage) error {
	if err := ValidateTargetSetRef(authority, page.Ref); err != nil {
		return err
	}
	if len(page.Targets) == 0 || len(page.Targets) > MaxTargetPageItems ||
		page.PreviousDigest == ([sha256.Size]byte{}) || page.Digest == ([sha256.Size]byte{}) {
		return invalid("target set page proof is incomplete")
	}
	for index, target := range page.Targets {
		if err := validateTarget(target); err != nil {
			return err
		}
		if index > 0 && compareTarget(page.Targets[index-1], target) >= 0 {
			return invalid("target set page is not strictly ordered")
		}
	}
	return nil
}

// ValidateSnapshotRequest verifies one immutable bounded snapshot request.
func ValidateSnapshotRequest(request SnapshotRequest) error {
	if err := validateTargetSetRefShape(request.TargetSet); err != nil {
		return err
	}
	if err := validateToken(request.Revision, "snapshot revision"); err != nil {
		return err
	}
	if err := validateLimit(request.Limit); err != nil {
		return err
	}
	return validateRequestCursor(
		request.Cursor, request.TargetSet,
		PageSnapshot, "", request.Revision, request.Limit,
	)
}

// ValidateSnapshotPage verifies page bounds, ordering, cursor, and digest.
func ValidateSnapshotPage(request SnapshotRequest, page SnapshotPage) error {
	if err := ValidateSnapshotRequest(request); err != nil {
		return err
	}
	if page.Revision != request.Revision || len(page.Objects) > request.Limit {
		return invalid("snapshot response fence or count differs from request")
	}
	for index := range page.Objects {
		if err := ValidateProjection(page.Objects[index]); err != nil {
			return err
		}
		if index == 0 && request.Cursor != nil && comparePosition(
			PagePosition{Tenant: page.Objects[index].Tenant, Generation: page.Objects[index].Generation, ID: page.Objects[index].ID},
			PagePosition{Tenant: request.Cursor.AfterTenant, Generation: request.Cursor.AfterGeneration, ID: request.Cursor.After},
		) <= 0 {
			return invalid("snapshot page does not continue after its cursor")
		}
		if index > 0 && compareProjectionPosition(page.Objects[index-1], page.Objects[index]) >= 0 {
			return invalid("snapshot page is not strictly tenant-generation-logical-id ordered")
		}
	}
	digest, err := SnapshotPageDigest(page.Revision, page.Objects)
	if err != nil {
		return err
	}
	if page.Digest != digest {
		return fmt.Errorf("%w: snapshot page digest differs", ErrIntegrity)
	}
	return validateNextCursor(
		request.Cursor, page.Next, request.TargetSet, PageSnapshot,
		"", page.Revision, request.Limit,
		lastProjectionPosition(page.Objects), page.Digest,
	)
}

// ValidateChangesRequest verifies one immutable bounded delta request.
func ValidateChangesRequest(request ChangesRequest) error {
	if err := validateTargetSetRefShape(request.TargetSet); err != nil {
		return err
	}
	if err := validateToken(request.From, "change predecessor"); err != nil {
		return err
	}
	if err := validateToken(request.To, "change target"); err != nil {
		return err
	}
	if request.From == request.To {
		return invalid("change predecessor equals target")
	}
	if err := validateLimit(request.Limit); err != nil {
		return err
	}
	return validateRequestCursor(
		request.Cursor, request.TargetSet,
		PageChanges, request.From, request.To, request.Limit,
	)
}

// ValidateChangePage verifies page bounds, ordering, cursor, and digest.
func ValidateChangePage(request ChangesRequest, page ChangePage) error {
	if err := ValidateChangesRequest(request); err != nil {
		return err
	}
	if page.From != request.From || page.To != request.To || len(page.Changes) > request.Limit {
		return invalid("change response fence or count differs from request")
	}
	for index := range page.Changes {
		change := page.Changes[index]
		if err := ValidateChange(change); err != nil {
			return err
		}
		if index == 0 && request.Cursor != nil && comparePosition(
			PagePosition{Tenant: change.Tenant, Generation: change.Generation, Sequence: change.Sequence, ID: change.ID},
			PagePosition{Tenant: request.Cursor.AfterTenant, Generation: request.Cursor.AfterGeneration, Sequence: request.Cursor.AfterSequence, ID: request.Cursor.After},
		) <= 0 {
			return invalid("change page does not continue after its cursor")
		}
		if index > 0 && compareChangePosition(page.Changes[index-1], change) >= 0 {
			return invalid("change page is not strictly tenant-generation-logical-id ordered")
		}
	}
	digest, err := ChangePageDigest(page.From, page.To, page.Changes)
	if err != nil {
		return err
	}
	if page.Digest != digest {
		return fmt.Errorf("%w: change page digest differs", ErrIntegrity)
	}
	return validateNextCursor(
		request.Cursor, page.Next, request.TargetSet, PageChanges,
		page.From, page.To, request.Limit, lastChangePosition(page.Changes), page.Digest,
	)
}

// ValidateProjection verifies one complete path-independent projection.
func ValidateProjection(object Projection) error {
	if err := validateTenantID(object.Tenant, "projection tenant"); err != nil {
		return err
	}
	if object.Generation == 0 {
		return invalid("projection tenant or generation is invalid")
	}
	if err := validateLogicalID(object.ID, "projection id"); err != nil {
		return err
	}
	if object.Parent != "" {
		if err := validateLogicalID(object.Parent, "projection parent"); err != nil {
			return err
		}
	}
	if object.Name == "" || len(object.Name) > 255 || !utf8.ValidString(object.Name) || strings.ContainsAny(object.Name, "/\\\x00") || object.Name == "." || object.Name == ".." {
		return invalid("projection name is invalid")
	}
	if object.Mode > 0o7777 || object.Size < 0 || object.Size > MaxContentBytes {
		return invalid("projection mode or size is invalid")
	}
	if !object.Visibility.Mount && !object.Visibility.FileProvider {
		return invalid("projection is invisible")
	}
	switch object.Kind {
	case catalog.KindDirectory:
		if object.LinkTarget != "" || object.Content != nil || object.Size != 0 || object.Hash != (catalog.ContentHash{}) {
			return invalid("directory projection carries file or link data")
		}
	case catalog.KindFile:
		if object.LinkTarget != "" || object.Content == nil || object.Hash == (catalog.ContentHash{}) {
			return invalid("file projection has no exact content reference")
		}
		if err := ValidateContentRef(*object.Content); err != nil {
			return err
		}
		if object.Content.Tenant != object.Tenant || object.Content.Generation != object.Generation ||
			object.Content.Object != object.ID || object.Content.Size != object.Size || object.Content.Hash != object.Hash {
			return invalid("file projection content reference differs")
		}
	case catalog.KindSymlink:
		if object.LinkTarget == "" || len(object.LinkTarget) > 4096 || !utf8.ValidString(object.LinkTarget) || strings.ContainsRune(object.LinkTarget, 0) || object.Content != nil || object.Size != 0 || object.Hash != (catalog.ContentHash{}) {
			return invalid("symlink projection is invalid")
		}
	default:
		return invalid("projection kind is invalid")
	}
	return nil
}

// ValidateContentRef verifies one immutable stream reference.
func ValidateContentRef(ref ContentRef) error {
	if err := validateToken(ref.Revision, "content revision"); err != nil {
		return err
	}
	if err := validateTenantID(ref.Tenant, "content tenant"); err != nil {
		return err
	}
	if ref.Generation == 0 {
		return invalid("content tenant or generation is invalid")
	}
	if err := validateLogicalID(ref.Object, "content object"); err != nil {
		return err
	}
	if ref.Size < 0 || ref.Size > MaxContentBytes {
		return invalid("content size is invalid")
	}
	if ref.Hash == (catalog.ContentHash{}) {
		return invalid("content hash is empty")
	}
	return nil
}

// ValidateChange verifies one closed logical delta.
func ValidateChange(change Change) error {
	if err := validateTenantID(change.Tenant, "change tenant"); err != nil {
		return err
	}
	if change.Generation == 0 || change.Sequence == 0 {
		return invalid("change tenant or generation is invalid")
	}
	if err := validateLogicalID(change.ID, "change id"); err != nil {
		return err
	}
	switch change.Kind {
	case ChangeDelete:
		if change.Object != nil {
			return invalid("delete change carries a projection")
		}
	case ChangeUpsert:
		if change.Object == nil || change.Object.Tenant != change.Tenant ||
			change.Object.Generation != change.Generation || change.Object.ID != change.ID {
			return invalid("upsert change projection identity differs")
		}
		return ValidateProjection(*change.Object)
	default:
		return invalid("change kind is invalid")
	}
	return nil
}

// ValidateMutationRequest verifies one exact idempotent source mutation.
func ValidateMutationRequest(request MutationRequest) error {
	if err := validateTargetSetRefShape(request.TargetSet); err != nil {
		return err
	}
	if request.Generation == 0 {
		return invalid("mutation authority or target generation fence is incomplete")
	}
	if err := validateTenantID(request.Tenant, "mutation tenant"); err != nil {
		return err
	}
	if request.OperationID == (catalog.MutationID{}) {
		return invalid("mutation operation id is empty")
	}
	if err := validateToken(request.Expected, "mutation expected revision"); err != nil {
		return err
	}
	if err := catalog.ValidateSourceMutationContext(request.Context); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	if request.HasContent != request.Context.Operation.HasContent {
		return invalid("mutation content intent differs from source context")
	}
	if request.HasContent {
		if request.ContentSize < 0 || request.ContentSize > MaxContentBytes ||
			request.ContentHash == (catalog.ContentHash{}) {
			return invalid("mutation content size is invalid")
		}
	} else if request.ContentSize != 0 || request.ContentHash != (catalog.ContentHash{}) {
		return invalid("contentless mutation carries content metadata")
	}
	return nil
}

// ValidateMutationReceipt verifies one durable operation proof.
func ValidateMutationReceipt(receipt MutationReceipt) error {
	if receipt.OperationID == (catalog.MutationID{}) {
		return invalid("mutation receipt operation id is empty")
	}
	switch receipt.State {
	case MutationNotFound:
		if receipt.RequestDigest != ([sha256.Size]byte{}) || receipt.Expected != "" || receipt.Committed != "" || receipt.Result != "" || receipt.Digest != ([sha256.Size]byte{}) {
			return invalid("not-found mutation receipt carries durable state")
		}
		return nil
	case MutationPrepared:
		if receipt.RequestDigest == ([sha256.Size]byte{}) {
			return invalid("prepared mutation receipt request digest is empty")
		}
		if err := validateToken(receipt.Expected, "prepared expected revision"); err != nil {
			return err
		}
		if receipt.Committed != "" || receipt.Result != "" {
			return invalid("prepared mutation receipt carries applied state")
		}
	case MutationApplied:
		if receipt.RequestDigest == ([sha256.Size]byte{}) {
			return invalid("applied mutation receipt request digest is empty")
		}
		if err := validateToken(receipt.Expected, "applied expected revision"); err != nil {
			return err
		}
		if err := validateToken(receipt.Committed, "committed revision"); err != nil {
			return err
		}
		if receipt.Committed == receipt.Expected {
			return invalid("applied mutation did not advance the source revision")
		}
		if receipt.Result != "" {
			if err := validateLogicalID(receipt.Result, "mutation result"); err != nil {
				return err
			}
		}
	default:
		return invalid("mutation receipt state is invalid")
	}
	digest, err := MutationReceiptDigest(receipt)
	if err != nil {
		return err
	}
	if receipt.Digest != digest {
		return fmt.Errorf("%w: mutation receipt digest differs", ErrIntegrity)
	}
	return nil
}

// ValidateMutationSettlement verifies one exact source-side receipt transition.
func ValidateMutationSettlement(settlement MutationSettlement) error {
	if err := validateTargetSetRefShape(settlement.TargetSet); err != nil {
		return err
	}
	if settlement.OperationID == (catalog.MutationID{}) ||
		settlement.RequestDigest == ([32]byte{}) ||
		settlement.ReceiptDigest == ([32]byte{}) {
		return invalid("mutation settlement receipt proof is incomplete")
	}
	switch settlement.Kind {
	case MutationSettlementAcknowledge, MutationSettlementAbandon, MutationSettlementForget:
		return nil
	default:
		return invalid("mutation settlement kind is invalid")
	}
}

// NewPageCursor constructs the only valid cursor after one page.
func NewPageCursor(
	targetSet TargetSetRef,
	kind PageKind,
	from, to RevisionToken,
	page uint32,
	limit int,
	after PagePosition,
	continuation []byte,
	previous [sha256.Size]byte,
) (PageCursor, error) {
	cursor := PageCursor{
		TargetSet: targetSet, Kind: kind,
		From: from, To: to, Page: page, Limit: uint32(limit), AfterTenant: after.Tenant,
		AfterGeneration: after.Generation, AfterSequence: after.Sequence, After: after.ID,
		Continuation: append([]byte(nil), continuation...), PreviousDigest: previous,
	}
	if validateTargetSetRefShape(targetSet) != nil || page == 0 ||
		limit < 1 || limit > MaxPageItems ||
		after.Generation == 0 || after.ID == "" || len(continuation) > MaxContinuationBytes ||
		previous == ([sha256.Size]byte{}) {
		return PageCursor{}, invalid("next cursor page or position is empty")
	}
	if err := validateTenantID(after.Tenant, "cursor tenant"); err != nil {
		return PageCursor{}, err
	}
	if (kind == PageSnapshot && (from != "" || after.Sequence != 0)) ||
		(kind == PageChanges && (from == "" || after.Sequence == 0)) ||
		(kind != PageSnapshot && kind != PageChanges) {
		return PageCursor{}, invalid("next cursor kind or sequence is invalid")
	}
	if from != "" {
		if err := validateToken(from, "cursor predecessor"); err != nil {
			return PageCursor{}, err
		}
	}
	if err := validateToken(to, "cursor target"); err != nil {
		return PageCursor{}, err
	}
	if err := validateLogicalID(after.ID, "cursor position"); err != nil {
		return PageCursor{}, err
	}
	digest, err := cursorDigest(cursor)
	if err != nil {
		return PageCursor{}, err
	}
	cursor.Digest = digest
	return cursor, nil
}

func validateRequestCursor(
	cursor *PageCursor,
	targetSet TargetSetRef,
	kind PageKind,
	from, to RevisionToken,
	limit int,
) error {
	if cursor == nil {
		return nil
	}
	digest, err := cursorDigest(*cursor)
	if err != nil {
		return err
	}
	if cursor.TargetSet != targetSet || cursor.Kind != kind ||
		cursor.From != from || cursor.To != to || cursor.Page == 0 || cursor.Limit != uint32(limit) ||
		cursor.AfterGeneration == 0 || cursor.After == "" ||
		len(cursor.Continuation) > MaxContinuationBytes ||
		cursor.PreviousDigest == ([sha256.Size]byte{}) ||
		(kind == PageSnapshot && cursor.AfterSequence != 0) ||
		(kind == PageChanges && cursor.AfterSequence == 0) || cursor.Digest == ([sha256.Size]byte{}) || cursor.Digest != digest {
		return invalid("page cursor fence or digest is invalid")
	}
	if err := validateTenantID(cursor.AfterTenant, "cursor tenant"); err != nil {
		return err
	}
	return validateLogicalID(cursor.After, "cursor position")
}

func validateNextCursor(
	previous, next *PageCursor,
	targetSet TargetSetRef,
	kind PageKind,
	from, to RevisionToken,
	limit int,
	after PagePosition,
	digest [sha256.Size]byte,
) error {
	if next == nil {
		return nil
	}
	if err := validateRequestCursor(
		next, targetSet, kind, from, to, limit,
	); err != nil {
		return fmt.Errorf("%w: next page cursor is invalid: %v", ErrIntegrity, err)
	}
	nextDigest, err := cursorDigest(*next)
	if err != nil {
		return fmt.Errorf("%w: next page cursor digest: %v", ErrIntegrity, err)
	}
	if after.ID == "" || next.TargetSet != targetSet || next.Kind != kind ||
		next.From != from || next.To != to || next.Limit != uint32(limit) || next.AfterTenant != after.Tenant ||
		next.AfterGeneration != after.Generation || next.After != after.ID ||
		next.AfterSequence != after.Sequence ||
		next.PreviousDigest != digest || next.Digest != nextDigest {
		return fmt.Errorf("%w: next page cursor is not bound to the page", ErrIntegrity)
	}
	wantPage := uint32(1)
	if previous != nil {
		wantPage = previous.Page + 1
	}
	if next.Page != wantPage {
		return fmt.Errorf("%w: next page cursor is not contiguous", ErrIntegrity)
	}
	return nil
}

func validateToken(token RevisionToken, field string) error {
	value := string(token)
	if value == "" || len(value) > RevisionTokenMaxBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return invalid("%s is invalid", field)
	}
	return nil
}

func validateLogicalID(id LogicalID, field string) error {
	value := string(id)
	if value == "" || len(value) > LogicalIDMaxBytes || !utf8.ValidString(value) ||
		strings.ContainsAny(value, "/\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return invalid("%s is invalid", field)
	}
	return nil
}

func validateTenantID(tenant catalog.TenantID, field string) error {
	value := string(tenant)
	if value == "" || len(value) > 255 || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return invalid("%s is invalid", field)
	}
	return nil
}

func validateTargets(count uint64, digest [sha256.Size]byte, targets []TargetDeclaration, bindDigest bool) error {
	if len(targets) == 0 || len(targets) > MaxTargets || count != uint64(len(targets)) {
		return invalid("target declaration count is invalid")
	}
	for index, target := range targets {
		if err := validateTarget(target); err != nil {
			return err
		}
		if index > 0 &&
			(compareTarget(targets[index-1], target) >= 0 || targets[index-1].Tenant == target.Tenant) {
			return invalid("target declaration is not sorted and unique")
		}
	}
	computed, err := targetsDigest(targets)
	if err != nil {
		return err
	}
	if bindDigest && (digest == ([sha256.Size]byte{}) || digest != computed) {
		return fmt.Errorf("%w: target declaration digest differs", ErrIntegrity)
	}
	return nil
}

func validateTargetSetRefShape(ref TargetSetRef) error {
	if ref.ID == (TargetSetID{}) || ref.AuthorityGeneration == 0 || ref.TargetEpoch == 0 ||
		ref.DeclarationDigest == ([sha256.Size]byte{}) || ref.TargetCount == 0 ||
		ref.TargetCount > MaxTargets || ref.TargetsDigest == ([sha256.Size]byte{}) {
		return invalid("target set reference is invalid")
	}
	return nil
}

func validateTarget(target TargetDeclaration) error {
	if err := validateTenantID(target.Tenant, "target tenant"); err != nil {
		return err
	}
	if target.Generation == 0 {
		return invalid("target generation is zero")
	}
	return nil
}

func compareTarget(left, right TargetDeclaration) int {
	if value := strings.Compare(string(left.Tenant), string(right.Tenant)); value != 0 {
		return value
	}
	if left.Generation < right.Generation {
		return -1
	}
	if left.Generation > right.Generation {
		return 1
	}
	return 0
}

func validateLimit(limit int) error {
	if limit <= 0 || limit > MaxPageItems {
		return invalid("page limit is invalid")
	}
	return nil
}

func canonicalDigest(value any, budget int) ([sha256.Size]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("source driver: encode digest value: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("source driver: canonicalize digest value: %w", err)
	}
	if compact.Len() > budget {
		return [sha256.Size]byte{}, invalid("page exceeds the encoded byte budget")
	}
	return sha256.Sum256(compact.Bytes()), nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidValue, fmt.Sprintf(format, args...))
}

func lastProjectionPosition(objects []Projection) PagePosition {
	if len(objects) == 0 {
		return PagePosition{}
	}
	object := objects[len(objects)-1]
	return PagePosition{Tenant: object.Tenant, Generation: object.Generation, ID: object.ID}
}

func lastChangePosition(changes []Change) PagePosition {
	if len(changes) == 0 {
		return PagePosition{}
	}
	change := changes[len(changes)-1]
	return PagePosition{Tenant: change.Tenant, Generation: change.Generation, Sequence: change.Sequence, ID: change.ID}
}

func compareProjectionPosition(left, right Projection) int {
	return comparePosition(
		PagePosition{Tenant: left.Tenant, Generation: left.Generation, ID: left.ID},
		PagePosition{Tenant: right.Tenant, Generation: right.Generation, ID: right.ID},
	)
}

func compareChangePosition(left, right Change) int {
	return comparePosition(
		PagePosition{Tenant: left.Tenant, Generation: left.Generation, Sequence: left.Sequence, ID: left.ID},
		PagePosition{Tenant: right.Tenant, Generation: right.Generation, Sequence: right.Sequence, ID: right.ID},
	)
}

func comparePosition(left, right PagePosition) int {
	if value := strings.Compare(string(left.Tenant), string(right.Tenant)); value != 0 {
		return value
	}
	if left.Generation < right.Generation {
		return -1
	}
	if left.Generation > right.Generation {
		return 1
	}
	if left.Sequence < right.Sequence {
		return -1
	}
	if left.Sequence > right.Sequence {
		return 1
	}
	return strings.Compare(string(left.ID), string(right.ID))
}
