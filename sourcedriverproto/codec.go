// Package sourcedriverproto defines the exact generated SourceDriver wire schema.
package sourcedriverproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

var (
	// ErrInvalidMessage means a value violates the closed protocol schema.
	ErrInvalidMessage = errors.New("source driver protocol: invalid message")
	// ErrProtocol means a peer selected any protocol other than the exact generated version.
	ErrProtocol = errors.New("source driver protocol: unsupported protocol")
)

// Encode validates and returns canonical JSON for one protocol value.
func Encode(value any) ([]byte, error) {
	if err := Validate(value); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("source driver protocol: encode: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, fmt.Errorf("source driver protocol: canonicalize: %w", err)
	}
	if compact.Len() > int(MaxPageBytes) {
		return nil, invalid("encoded message exceeds byte budget")
	}
	return compact.Bytes(), nil
}

// Decode strictly decodes and validates exactly one bounded protocol value.
func Decode(data []byte, dst any) error {
	if len(data) == 0 || len(data) > int(MaxPageBytes) {
		return invalid("encoded message is empty or exceeds byte budget")
	}
	if dst == nil || reflect.ValueOf(dst).Kind() != reflect.Pointer || reflect.ValueOf(dst).IsNil() {
		return invalid("decode destination must be a non-nil pointer")
	}
	if err := inspectJSON(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return invalid("decode: %v", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	return Validate(dst)
}

// Validate checks one generated message or nested protocol value.
func Validate(value any) error {
	if value == nil {
		return invalid("nil value")
	}
	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer {
		if current.IsNil() {
			return invalid("nil value")
		}
		current = current.Elem()
	}
	switch message := current.Interface().(type) {
	case RefreshRequest:
		return validateProtocol(message.Protocol)
	case RefreshResponse:
		return validateRefreshResponse(message)
	case InspectTargetSetRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateTargetSetRef(message.Ref)
	case InspectTargetSetResponse:
		return validateTargetSetResponse(message.Protocol, message.Code, message.Message, message.State)
	case DeclareTargetSetRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateTargetSetPage(message.Page)
	case DeclareTargetSetResponse:
		return validateTargetSetResponse(message.Protocol, message.Code, message.Message, message.State)
	case SnapshotRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateTargetSetRef(message.TargetSet); err != nil {
			return err
		}
		if err := validateToken(message.Revision, "snapshot revision"); err != nil {
			return err
		}
		if err := validateLimit(message.Limit); err != nil {
			return err
		}
		return validateCursor(
			message.Cursor, message.TargetSet,
			PageKindSnapshot, "", message.Revision, message.Limit,
		)
	case SnapshotResponse:
		return validateSnapshotResponse(message)
	case ChangesSinceRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateTargetSetRef(message.TargetSet); err != nil {
			return err
		}
		if err := validateToken(message.From, "change predecessor"); err != nil {
			return err
		}
		if err := validateToken(message.To, "change target"); err != nil {
			return err
		}
		if message.From == message.To {
			return invalid("change predecessor equals target")
		}
		if err := validateLimit(message.Limit); err != nil {
			return err
		}
		return validateCursor(
			message.Cursor, message.TargetSet,
			PageKindChanges, message.From, message.To, message.Limit,
		)
	case ChangesSinceResponse:
		return validateChangesResponse(message)
	case OpenContentRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateContentRef(message.Content)
	case OpenContentResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOK {
			if message.Content == nil || message.Actual != "" {
				return invalid("successful content response is incomplete")
			}
			return validateContentRef(*message.Content)
		}
		if message.Content != nil {
			return invalid("failed content response carries content")
		}
		return validateActual(message.Code, message.Actual)
	case ApplyMutationRequest:
		return validateApplyRequest(message)
	case ApplyMutationResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOK {
			if message.Receipt == nil || message.Actual != "" {
				return invalid("successful apply response is incomplete")
			}
			return validateReceipt(*message.Receipt)
		}
		if message.Receipt != nil {
			return invalid("failed apply response carries a receipt")
		}
		return validateActual(message.Code, message.Actual)
	case InspectMutationRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateHex(message.OperationID, sha256.Size, "operation id"); err != nil {
			return err
		}
		return validateHex(message.RequestDigest, sha256.Size, "mutation request digest")
	case InspectMutationResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOK {
			if message.Receipt == nil {
				return invalid("successful inspection response has no receipt")
			}
			return validateReceipt(*message.Receipt)
		}
		if message.Receipt != nil {
			return invalid("failed inspection response carries a receipt")
		}
		return nil
	case SettleMutationRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateSettlement(message.Settlement)
	case SettleMutationResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case PageCursor:
		return validateCursor(
			&message, message.TargetSet,
			message.Kind, message.From, message.To, message.Limit,
		)
	case TargetDeclaration:
		return validateTarget(message)
	case TargetSetRef:
		return validateTargetSetRef(message)
	case TargetSetState:
		return validateTargetSetState(message)
	case TargetSetPage:
		return validateTargetSetPage(message)
	case Projection:
		return validateProjection(message)
	case Change:
		return validateChange(message)
	case ContentRef:
		return validateContentRef(message)
	case MutationReceipt:
		return validateReceipt(message)
	case MutationSettlement:
		return validateSettlement(message)
	default:
		return invalid("unsupported value type %T", value)
	}
}

func validateSettlement(settlement MutationSettlement) error {
	if err := validateTargetSetRef(settlement.TargetSet); err != nil {
		return err
	}
	if err := validateHex(settlement.OperationID, sha256.Size, "mutation settlement operation id"); err != nil {
		return err
	}
	if err := validateHex(settlement.RequestDigest, sha256.Size, "mutation settlement request digest"); err != nil {
		return err
	}
	if err := validateHex(settlement.ReceiptDigest, sha256.Size, "mutation settlement receipt digest"); err != nil {
		return err
	}
	switch settlement.Kind {
	case MutationSettlementAcknowledge, MutationSettlementAbandon, MutationSettlementForget:
		return nil
	default:
		return invalid("mutation settlement kind is invalid")
	}
}

func validateRefreshResponse(message RefreshResponse) error {
	if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
		return err
	}
	if message.Code == ErrorCodeOK {
		if message.Actual != "" {
			return invalid("successful refresh response carries an actual revision")
		}
		return validateToken(message.Revision, "refresh revision")
	}
	if message.Revision != "" {
		return invalid("failed refresh response carries a revision")
	}
	return validateActual(message.Code, message.Actual)
}

func validateTargetSetResponse(
	protocol uint16,
	code ErrorCode,
	message string,
	state *TargetSetState,
) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOK {
		if state != nil {
			return invalid("failed target set response carries state")
		}
		return nil
	}
	if state == nil {
		return invalid("successful target set response has no state")
	}
	return validateTargetSetState(*state)
}

func validateSnapshotResponse(message SnapshotResponse) error {
	if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
		return err
	}
	if message.Code != ErrorCodeOK {
		if message.Revision != "" || len(message.Objects) != 0 || message.Next != nil || message.Digest != "" {
			return invalid("failed snapshot response carries page data")
		}
		return validateActual(message.Code, message.Actual)
	}
	if message.Actual != "" || len(message.Objects) > int(MaxPageItems) {
		return invalid("successful snapshot response has invalid actual revision or count")
	}
	if err := validateToken(message.Revision, "snapshot revision"); err != nil {
		return err
	}
	if err := validateHex(message.Digest, sha256.Size, "snapshot digest"); err != nil {
		return err
	}
	for index := range message.Objects {
		if err := validateProjection(message.Objects[index]); err != nil {
			return err
		}
		if index > 0 && compareProjectionPosition(message.Objects[index-1], message.Objects[index]) >= 0 {
			return invalid("snapshot projections are not strictly ordered")
		}
	}
	if err := validateResponseCursor(message.Next, PageKindSnapshot, "", message.Revision); err != nil {
		return err
	}
	if message.Next != nil {
		if len(message.Objects) == 0 || !cursorMatchesProjection(*message.Next, message.Objects[len(message.Objects)-1]) ||
			message.Next.PreviousDigest != message.Digest {
			return invalid("snapshot cursor is not bound to the returned page")
		}
	}
	return nil
}

func validateChangesResponse(message ChangesSinceResponse) error {
	if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
		return err
	}
	if message.Code != ErrorCodeOK {
		if message.From != "" || message.To != "" || len(message.Changes) != 0 || message.Next != nil || message.Digest != "" {
			return invalid("failed changes response carries page data")
		}
		return validateActual(message.Code, message.Actual)
	}
	if message.Actual != "" || len(message.Changes) > int(MaxPageItems) || message.From == message.To {
		return invalid("successful changes response has invalid actual revision or count")
	}
	if err := validateToken(message.From, "change predecessor"); err != nil {
		return err
	}
	if err := validateToken(message.To, "change target"); err != nil {
		return err
	}
	if err := validateHex(message.Digest, sha256.Size, "change digest"); err != nil {
		return err
	}
	for index := range message.Changes {
		if err := validateChange(message.Changes[index]); err != nil {
			return err
		}
		if index > 0 && compareChangePosition(message.Changes[index-1], message.Changes[index]) >= 0 {
			return invalid("changes are not strictly ordered")
		}
	}
	if err := validateResponseCursor(message.Next, PageKindChanges, message.From, message.To); err != nil {
		return err
	}
	if message.Next != nil {
		if len(message.Changes) == 0 || !cursorMatchesChange(*message.Next, message.Changes[len(message.Changes)-1]) ||
			message.Next.PreviousDigest != message.Digest {
			return invalid("change cursor is not bound to the returned page")
		}
	}
	return nil
}

func validateApplyRequest(message ApplyMutationRequest) error {
	if err := validateProtocol(message.Protocol); err != nil {
		return err
	}
	if message.Generation == 0 {
		return invalid("mutation authority or target generation is zero")
	}
	if err := validateTargetSetRef(message.TargetSet); err != nil {
		return err
	}
	if err := validateOpaque(message.Tenant, 255, "mutation tenant"); err != nil {
		return err
	}
	if err := validateHex(message.OperationID, sha256.Size, "operation id"); err != nil {
		return err
	}
	if err := validateToken(message.Expected, "expected revision"); err != nil {
		return err
	}
	if err := validateMutationContext(message.Context); err != nil {
		return err
	}
	if message.HasContent != message.Context.Operation.HasContent {
		return invalid("mutation content intent differs from context")
	}
	if message.HasContent {
		if message.ContentSize < 0 || uint64(message.ContentSize) > MaxContentBytes {
			return invalid("mutation content size is invalid")
		}
		return validateHex(message.ContentHash, sha256.Size, "content hash")
	}
	if message.ContentSize != 0 || message.ContentHash != "" {
		return invalid("contentless mutation carries content metadata")
	}
	return nil
}

func validateMutationContext(context MutationContext) error {
	if context.Operation.Kind < 2 || context.Operation.Kind > 5 ||
		context.Operation.ObjectKind < 1 || context.Operation.ObjectKind > 3 ||
		context.Operation.Mode > 0o7777 || context.Operation.Name == "" ||
		len(context.Operation.Name) > 255 || strings.ContainsAny(context.Operation.Name, "/\\\x00") ||
		len(context.Operation.LinkTarget) > 4096 || strings.ContainsRune(context.Operation.LinkTarget, 0) {
		return invalid("mutation operation is invalid")
	}
	for _, locator := range []*SourceLocator{context.Object, context.Parent, context.Target} {
		if locator == nil {
			continue
		}
		if err := validateOpaque(locator.Authority, 255, "source authority"); err != nil {
			return err
		}
		if err := validateOpaque(locator.Key, 4096, "source key"); err != nil {
			return err
		}
		if locator.Revision == 0 {
			return invalid("source locator revision is zero")
		}
	}
	return nil
}

func validateReceipt(receipt MutationReceipt) error {
	if err := validateHex(receipt.OperationID, sha256.Size, "operation id"); err != nil {
		return err
	}
	switch receipt.State {
	case MutationStateNotFound:
		if receipt.RequestDigest != "" || receipt.Expected != "" || receipt.Committed != "" || receipt.Result != "" || receipt.Digest != "" {
			return invalid("not-found receipt carries durable state")
		}
		return nil
	case MutationStatePrepared:
		if err := validateHex(receipt.RequestDigest, sha256.Size, "mutation request digest"); err != nil {
			return err
		}
		if err := validateToken(receipt.Expected, "prepared revision"); err != nil {
			return err
		}
		if receipt.Committed != "" || receipt.Result != "" {
			return invalid("prepared receipt carries applied state")
		}
	case MutationStateApplied:
		if err := validateHex(receipt.RequestDigest, sha256.Size, "mutation request digest"); err != nil {
			return err
		}
		if err := validateToken(receipt.Expected, "expected revision"); err != nil {
			return err
		}
		if err := validateToken(receipt.Committed, "committed revision"); err != nil {
			return err
		}
		if receipt.Committed == receipt.Expected {
			return invalid("applied receipt did not advance the source revision")
		}
		if receipt.Result != "" {
			if err := validateLogicalID(receipt.Result, "mutation result"); err != nil {
				return err
			}
		}
	default:
		return invalid("mutation receipt state is invalid")
	}
	return validateHex(receipt.Digest, sha256.Size, "mutation receipt digest")
}

func validateProjection(object Projection) error {
	if err := validateOpaque(object.Tenant, 255, "projection tenant"); err != nil {
		return err
	}
	if object.Generation == 0 {
		return invalid("projection generation is zero")
	}
	if err := validateLogicalID(object.ID, "projection id"); err != nil {
		return err
	}
	if object.Parent != "" {
		if err := validateLogicalID(object.Parent, "projection parent"); err != nil {
			return err
		}
	}
	if object.Name == "" || len(object.Name) > 255 || strings.ContainsAny(object.Name, "/\\\x00") || object.Name == "." || object.Name == ".." {
		return invalid("projection name is invalid")
	}
	if object.Mode > 0o7777 || object.Size < 0 || uint64(object.Size) > MaxContentBytes {
		return invalid("projection mode or size is invalid")
	}
	if !object.MountVisible && !object.FileProviderVisible {
		return invalid("projection is invisible")
	}
	switch object.Kind {
	case ObjectKindDirectory:
		if object.LinkTarget != "" || object.Content != nil || object.Size != 0 || object.Hash != "" {
			return invalid("directory carries file or link data")
		}
	case ObjectKindFile:
		if object.LinkTarget != "" || object.Content == nil {
			return invalid("file has no exact content reference")
		}
		if err := validateHex(object.Hash, sha256.Size, "projection hash"); err != nil {
			return err
		}
		if err := validateContentRef(*object.Content); err != nil {
			return err
		}
		if object.Content.Tenant != object.Tenant || object.Content.Generation != object.Generation ||
			object.Content.Object != object.ID || object.Content.Size != object.Size || object.Content.Hash != object.Hash {
			return invalid("projection content reference differs")
		}
	case ObjectKindSymlink:
		if object.LinkTarget == "" || len(object.LinkTarget) > 4096 || strings.ContainsRune(object.LinkTarget, 0) || object.Content != nil || object.Size != 0 || object.Hash != "" {
			return invalid("symlink is invalid")
		}
	default:
		return invalid("projection kind is invalid")
	}
	return nil
}

func validateChange(change Change) error {
	if err := validateOpaque(change.Tenant, 255, "change tenant"); err != nil {
		return err
	}
	if change.Generation == 0 || change.Sequence == 0 {
		return invalid("change generation or sequence is zero")
	}
	if err := validateLogicalID(change.ID, "change id"); err != nil {
		return err
	}
	switch change.Kind {
	case ChangeKindDelete:
		if change.Object != nil {
			return invalid("delete carries a projection")
		}
	case ChangeKindUpsert:
		if change.Object == nil || change.Object.Tenant != change.Tenant ||
			change.Object.Generation != change.Generation || change.Object.ID != change.ID {
			return invalid("upsert projection identity differs")
		}
		return validateProjection(*change.Object)
	default:
		return invalid("change kind is invalid")
	}
	return nil
}

func validateContentRef(ref ContentRef) error {
	if err := validateToken(ref.Revision, "content revision"); err != nil {
		return err
	}
	if err := validateOpaque(ref.Tenant, 255, "content tenant"); err != nil {
		return err
	}
	if ref.Generation == 0 {
		return invalid("content generation is zero")
	}
	if err := validateLogicalID(ref.Object, "content object"); err != nil {
		return err
	}
	if ref.Size < 0 || uint64(ref.Size) > MaxContentBytes {
		return invalid("content size is invalid")
	}
	return validateHex(ref.Hash, sha256.Size, "content hash")
}

func validateCursor(
	cursor *PageCursor,
	targetSet TargetSetRef,
	kind PageKind,
	from, to string,
	limit uint32,
) error {
	if cursor == nil {
		return nil
	}
	if cursor.TargetSet != targetSet || cursor.Kind != kind ||
		cursor.From != from || cursor.To != to || cursor.Page == 0 || cursor.Limit != limit ||
		limit == 0 || limit > MaxPageItems {
		return invalid("cursor fence or page is invalid")
	}
	domainRef, err := domainTargetSetRef(cursor.TargetSet)
	if err != nil {
		return err
	}
	if err := validateOpaque(cursor.AfterTenant, 255, "cursor tenant"); err != nil {
		return err
	}
	if cursor.AfterGeneration == 0 ||
		(kind == PageKindSnapshot && (cursor.From != "" || cursor.AfterSequence != 0)) ||
		(kind == PageKindChanges && (cursor.From == "" || cursor.AfterSequence == 0)) ||
		(kind != PageKindSnapshot && kind != PageKindChanges) {
		return invalid("cursor kind or position is invalid")
	}
	if err := validateLogicalID(cursor.After, "cursor position"); err != nil {
		return err
	}
	continuation, err := base64.StdEncoding.DecodeString(cursor.Continuation)
	if err != nil || len(continuation) > sourcedriver.MaxContinuationBytes ||
		base64.StdEncoding.EncodeToString(continuation) != cursor.Continuation {
		return invalid("cursor continuation is invalid")
	}
	previous, err := parseDigest(cursor.PreviousDigest, "cursor previous digest")
	if err != nil {
		return err
	}
	digest, err := parseDigest(cursor.Digest, "cursor digest")
	if err != nil {
		return err
	}
	domain, err := sourcedriver.NewPageCursor(
		domainRef, domainPageKind(cursor.Kind),
		sourcedriver.RevisionToken(cursor.From), sourcedriver.RevisionToken(cursor.To), cursor.Page, int(cursor.Limit),
		sourcedriver.PagePosition{
			Tenant: catalog.TenantID(cursor.AfterTenant), Generation: causal.Generation(cursor.AfterGeneration),
			Sequence: cursor.AfterSequence, ID: sourcedriver.LogicalID(cursor.After),
		},
		continuation, previous,
	)
	if err != nil || domain.Digest != digest {
		return invalid("cursor digest is invalid")
	}
	return nil
}

func validateResponseCursor(cursor *PageCursor, kind PageKind, from, to string) error {
	if cursor == nil {
		return nil
	}
	return validateCursor(
		cursor, cursor.TargetSet, kind, from, to, cursor.Limit,
	)
}

func validateTarget(target TargetDeclaration) error {
	if err := validateOpaque(target.Tenant, 255, "target tenant"); err != nil {
		return err
	}
	if target.Generation == 0 {
		return invalid("target generation is zero")
	}
	return nil
}

func validateTargetSetRef(ref TargetSetRef) error {
	if err := validateHex(ref.ID, len(sourcedriver.TargetSetID{}), "target set id"); err != nil {
		return err
	}
	if ref.AuthorityGeneration == 0 || ref.TargetEpoch == 0 || ref.TargetCount == 0 || ref.TargetCount > uint64(MaxTargets) {
		return invalid("target set reference fence is invalid")
	}
	if err := validateHex(ref.DeclarationDigest, sha256.Size, "target set declaration digest"); err != nil {
		return err
	}
	return validateHex(ref.TargetsDigest, sha256.Size, "target set targets digest")
}

func validateTargetSetState(state TargetSetState) error {
	if err := validateTargetSetRef(state.Ref); err != nil {
		return err
	}
	rolling, err := parseDigest(state.RollingDigest, "target set rolling digest")
	if err != nil {
		return err
	}
	if state.DeclaredCount > state.Ref.TargetCount ||
		(state.DeclaredCount == 0 && (state.NextPage != 0 || state.After != nil ||
			state.LastPageDigest != "" || state.Complete)) ||
		(state.DeclaredCount != 0 && (state.NextPage == 0 || state.After == nil ||
			validateHex(state.LastPageDigest, sha256.Size, "target set last page digest") != nil)) ||
		state.Complete != (state.DeclaredCount == state.Ref.TargetCount) {
		return invalid("target set state is invalid")
	}
	if state.After != nil {
		if err := validateTarget(*state.After); err != nil {
			return err
		}
	}
	if state.Complete && hex.EncodeToString(rolling[:]) != state.Ref.TargetsDigest {
		return invalid("complete target set state digest differs")
	}
	return nil
}

func validateTargetSetPage(page TargetSetPage) error {
	if err := validateTargetSetRef(page.Ref); err != nil {
		return err
	}
	if len(page.Targets) == 0 || len(page.Targets) > int(MaxTargetPageItems) {
		return invalid("target set page count is invalid")
	}
	for index, target := range page.Targets {
		if err := validateTarget(target); err != nil {
			return err
		}
		if index > 0 && page.Targets[index-1].Tenant >= target.Tenant {
			return invalid("target set page is not strictly ordered")
		}
	}
	if err := validateHex(page.PreviousDigest, sha256.Size, "target set previous digest"); err != nil {
		return err
	}
	if err := validateHex(page.Digest, sha256.Size, "target set digest"); err != nil {
		return err
	}
	return validateHex(page.PageDigest, sha256.Size, "target set page digest")
}

func domainTargetSetRef(ref TargetSetRef) (sourcedriver.TargetSetRef, error) {
	if err := validateTargetSetRef(ref); err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	id, err := hex.DecodeString(ref.ID)
	if err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	declaration, err := parseDigest(ref.DeclarationDigest, "target set declaration digest")
	if err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	targets, err := parseDigest(ref.TargetsDigest, "target set targets digest")
	if err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	value := sourcedriver.TargetSetRef{
		AuthorityGeneration: causal.Generation(ref.AuthorityGeneration), TargetEpoch: ref.TargetEpoch,
		DeclarationDigest: declaration,
		TargetCount:       ref.TargetCount, TargetsDigest: targets,
	}
	copy(value.ID[:], id)
	return value, nil
}

func domainPageKind(kind PageKind) sourcedriver.PageKind {
	if kind == PageKindSnapshot {
		return sourcedriver.PageSnapshot
	}
	if kind == PageKindChanges {
		return sourcedriver.PageChanges
	}
	return 0
}

func compareProjectionPosition(left, right Projection) int {
	if value := strings.Compare(left.Tenant, right.Tenant); value != 0 {
		return value
	}
	if left.Generation < right.Generation {
		return -1
	}
	if left.Generation > right.Generation {
		return 1
	}
	return strings.Compare(left.ID, right.ID)
}

func compareChangePosition(left, right Change) int {
	if value := strings.Compare(left.Tenant, right.Tenant); value != 0 {
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
	return strings.Compare(left.ID, right.ID)
}

func cursorMatchesProjection(cursor PageCursor, object Projection) bool {
	return cursor.AfterTenant == object.Tenant && cursor.AfterGeneration == object.Generation &&
		cursor.AfterSequence == 0 && cursor.After == object.ID
}

func cursorMatchesChange(cursor PageCursor, change Change) bool {
	return cursor.AfterTenant == change.Tenant && cursor.AfterGeneration == change.Generation &&
		cursor.AfterSequence == change.Sequence && cursor.After == change.ID
}

func validateLogicalID(value, field string) error {
	if err := validateOpaque(value, 255, field); err != nil {
		return err
	}
	for _, character := range value {
		if character == '/' || character == '\\' || character < 0x20 || character == 0x7f {
			return fmt.Errorf("sourcedriverproto: %s is not catalog-key compatible", field)
		}
	}
	return nil
}

func validateProtocol(protocol uint16) error {
	if protocol != Version {
		return fmt.Errorf("%w: got %d want %d", ErrProtocol, protocol, Version)
	}
	return nil
}

func validateResponse(protocol uint16, code ErrorCode, message string) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	if !validErrorCode(code) {
		return invalid("response error code is invalid")
	}
	if len(message) > int(MaxErrorMessageBytes) || !utf8.ValidString(message) || strings.ContainsRune(message, 0) {
		return invalid("response message is invalid")
	}
	if code == ErrorCodeOK && message != "" || code != ErrorCodeOK && message == "" {
		return invalid("response code and message disagree")
	}
	return nil
}

func validateActual(code ErrorCode, actual string) error {
	switch code {
	case ErrorCodeSnapshotRequired, ErrorCodeStaleRevision:
		return validateToken(actual, "actual revision")
	default:
		if actual != "" {
			return invalid("non-revision error carries an actual revision")
		}
		return nil
	}
}

func validateToken(value, field string) error {
	return validateOpaque(value, 255, field)
}

func validateOpaque(value string, limit int, field string) error {
	if value == "" || len(value) > limit || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return invalid("%s is invalid", field)
	}
	return nil
}

func validateLimit(limit uint32) error {
	if limit == 0 || limit > MaxPageItems {
		return invalid("page limit is invalid")
	}
	return nil
}

func validateHex(value string, size int, field string) error {
	if len(value) != size*2 {
		return invalid("%s has the wrong length", field)
	}
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != size || value != strings.ToLower(value) ||
		bytes.Equal(raw, make([]byte, size)) {
		return invalid("%s is not canonical hexadecimal", field)
	}
	return nil
}

func parseDigest(value, field string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if err := validateHex(value, sha256.Size, field); err != nil {
		return digest, err
	}
	_, _ = hex.Decode(digest[:], []byte(value))
	return digest, nil
}

func validErrorCode(code ErrorCode) bool {
	switch code {
	case ErrorCodeOK, ErrorCodeInvalidRequest, ErrorCodeNotFound, ErrorCodeSnapshotRequired,
		ErrorCodeStaleRevision, ErrorCodeConflict, ErrorCodeIntegrity, ErrorCodeCanceled, ErrorCodeUnavailable:
		return true
	default:
		return false
	}
}

func inspectJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return invalid("decode token: %v", err)
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return invalid("decode object key: %v", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return invalid("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return invalid("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return invalid("object is not terminated")
		}
	case '[':
		for decoder.More() {
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return invalid("array is not terminated")
		}
	default:
		return invalid("unexpected JSON delimiter")
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return invalid("trailing JSON value")
		}
		return invalid("trailing JSON: %v", err)
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMessage, fmt.Sprintf(format, args...))
}
