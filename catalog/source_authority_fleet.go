package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/causal"
)

const (
	// SourceAuthorityFleetPageLimit is the hard authority count per fleet page.
	SourceAuthorityFleetPageLimit = 256
	// SourceAuthorityFleetPageByteLimit is the hard encoded size per fleet page.
	SourceAuthorityFleetPageByteLimit = 1 << 20
	// SourceAuthorityFleetAuthorityLimit is the hard authority count per owner.
	SourceAuthorityFleetAuthorityLimit = 10_000
	// SourceAuthorityFleetByteLimit is the hard encoded size per owner.
	SourceAuthorityFleetByteLimit = 64 << 20
	// SourceAuthorityRuntimeOwnerByteLimit bounds one canonical daemonkit process record.
	SourceAuthorityRuntimeOwnerByteLimit = 16 << 10
	// SourceDriverIDMaxBytes bounds one immutable product driver identifier.
	SourceDriverIDMaxBytes = 128
	// SourceDriverConfigMaxBytes bounds one immutable opaque driver configuration.
	SourceDriverConfigMaxBytes = 64 << 10
)

const sourceAuthorityFleetDigestPrefix = "fusekit.source-authority-fleet.v1\x00"

// SourceAuthorityFleetOwnerID identifies one product-owned complete authority set.
type SourceAuthorityFleetOwnerID string

// SourceAuthorityDeclaration binds one authority ID to its exact policy and materializer identity.
type SourceAuthorityDeclaration struct {
	Authority         causal.SourceAuthorityID
	DriverID          string
	DriverConfig      []byte
	DeclarationDigest [32]byte
}

// SourceAuthorityFleetState is one acknowledged complete authority set.
type SourceAuthorityFleetState struct {
	Owner                 SourceAuthorityFleetOwnerID
	Generation            causal.Generation
	AuthorityCount        uint64
	AuthoritiesDigest     [32]byte
	DeclarationsDigest    [32]byte
	AcknowledgementDigest [32]byte
}

// SourceAuthorityFleetReconcileState is one durable pending complete-set stage.
type SourceAuthorityFleetReconcileState struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	NextSequence       uint64
	ReceivedCount      uint64
	AuthorityCount     uint64
	AuthoritiesDigest  [32]byte
	DeclarationsDigest [32]byte
	StageSeed          [32]byte
	StageDigest        [32]byte
	Complete           bool
}

// SourceAuthorityFleetStatus is the acknowledged and pending state for one owner.
type SourceAuthorityFleetStatus struct {
	Current *SourceAuthorityFleetState
	Pending *SourceAuthorityFleetReconcileState
}

// SourceAuthorityFleetPageRequest addresses one exact acknowledged fleet page.
type SourceAuthorityFleetPageRequest struct {
	Owner      SourceAuthorityFleetOwnerID
	Generation causal.Generation
	After      causal.SourceAuthorityID
	Limit      int
}

// SourceAuthorityFleetPage is one authority-ordered acknowledged fleet page.
type SourceAuthorityFleetPage struct {
	Fleet        SourceAuthorityFleetState
	Declarations []SourceAuthorityDeclaration
	Next         causal.SourceAuthorityID
}

// SourceAuthorityFleetReconcileRequest appends one exact desired-fleet page.
type SourceAuthorityFleetReconcileRequest struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	Sequence           uint64
	Declarations       []SourceAuthorityDeclaration
	Complete           bool
	AuthorityCount     uint64
	AuthoritiesDigest  [32]byte
	DeclarationsDigest [32]byte
}

// SourceAuthorityRetireRequest fences one absent authority retirement.
type SourceAuthorityRetireRequest struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	Authority          causal.SourceAuthorityID
	StageDigest        [32]byte
}

// SourceAuthorityRetirementReceipt proves one exact completed retirement.
type SourceAuthorityRetirementReceipt struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	Authority          causal.SourceAuthorityID
	StageDigest        [32]byte
	ReceiptDigest      [32]byte
}

// SourceAuthorityFleetAcknowledgement commits one complete reconciled fleet.
type SourceAuthorityFleetAcknowledgement struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	AuthorityCount     uint64
	AuthoritiesDigest  [32]byte
	DeclarationsDigest [32]byte
	StageDigest        [32]byte
}

// SourceAuthorityFleetAbortRequest exactly fences one unpublished fleet stage.
type SourceAuthorityFleetAbortRequest struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	StageDigest        [32]byte
}

// SourceAuthorityFleetAbortReceipt proves one exact unpublished stage was discarded.
type SourceAuthorityFleetAbortReceipt struct {
	Owner              SourceAuthorityFleetOwnerID
	ExpectedGeneration causal.Generation
	Generation         causal.Generation
	StageDigest        [32]byte
	ReceiptDigest      [32]byte
}

// SourceAuthorityRuntimeFence addresses one exact acknowledged authority runtime.
type SourceAuthorityRuntimeFence struct {
	Owner      SourceAuthorityFleetOwnerID
	Generation causal.Generation
	Authority  causal.SourceAuthorityID
	Epoch      [16]byte
}

// SourceAuthorityRuntimeRef addresses one current fleet member without claiming its epoch.
type SourceAuthorityRuntimeRef struct {
	Owner      SourceAuthorityFleetOwnerID
	Generation causal.Generation
	Authority  causal.SourceAuthorityID
}

// SourceAuthorityRuntimeState is the durable exact epoch and settlement state for one member.
type SourceAuthorityRuntimeState struct {
	Ref               SourceAuthorityRuntimeRef
	DeclarationDigest [32]byte
	Epoch             [16]byte
	Process           *proc.Record
	Closed            bool
}

// SourceAuthorityRuntimeTakeover replaces one exactly settled prior runtime epoch.
type SourceAuthorityRuntimeTakeover struct {
	Ref           SourceAuthorityRuntimeRef
	ExpectedEpoch [16]byte
	Epoch         [16]byte
	Process       proc.Record
}

// Validate verifies the acknowledged fleet state.
func (s SourceAuthorityFleetState) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(s.Owner); err != nil {
		return err
	}
	if s.Generation == 0 || s.AuthorityCount > SourceAuthorityFleetAuthorityLimit ||
		s.AuthoritiesDigest == ([32]byte{}) || s.DeclarationsDigest == ([32]byte{}) ||
		s.AcknowledgementDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority fleet state", ErrInvalidObject)
	}
	return nil
}

// Validate verifies the pending fleet reconciliation state.
func (s SourceAuthorityFleetReconcileState) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(s.Owner); err != nil {
		return err
	}
	if s.Generation == 0 || s.Generation <= s.ExpectedGeneration ||
		s.ReceivedCount > s.AuthorityCount || s.AuthorityCount > SourceAuthorityFleetAuthorityLimit ||
		s.AuthoritiesDigest == ([32]byte{}) || s.DeclarationsDigest == ([32]byte{}) ||
		s.StageSeed == ([32]byte{}) || s.StageDigest == ([32]byte{}) ||
		(s.Complete && s.ReceivedCount != s.AuthorityCount) {
		return fmt.Errorf("%w: invalid source authority fleet reconciliation state", ErrInvalidObject)
	}
	return nil
}

// Validate verifies a fleet status and its owner/generation relationship.
func (s SourceAuthorityFleetStatus) Validate() error {
	if s.Current == nil && s.Pending == nil {
		return fmt.Errorf("%w: empty source authority fleet status", ErrInvalidObject)
	}
	if s.Current != nil {
		if err := s.Current.Validate(); err != nil {
			return err
		}
	}
	if s.Pending != nil {
		if err := s.Pending.Validate(); err != nil {
			return err
		}
		if s.Current == nil {
			if s.Pending.ExpectedGeneration != 0 {
				return fmt.Errorf("%w: initial source authority fleet expects a predecessor", ErrInvalidObject)
			}
		} else if s.Current.Owner != s.Pending.Owner ||
			s.Current.Generation != s.Pending.ExpectedGeneration {
			return fmt.Errorf("%w: source authority fleet status is not generation contiguous", ErrInvalidObject)
		}
	}
	return nil
}

// Validate verifies a fleet page request.
func (r SourceAuthorityFleetPageRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Generation == 0 || r.Limit <= 0 || r.Limit > SourceAuthorityFleetPageLimit ||
		(r.After != "" && validateSourceAuthorityID(r.After) != nil) {
		return fmt.Errorf("%w: invalid source authority fleet page request", ErrInvalidObject)
	}
	return nil
}

// Validate verifies a fleet page against its request.
func (p SourceAuthorityFleetPage) Validate(request SourceAuthorityFleetPageRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := p.Fleet.Validate(); err != nil {
		return err
	}
	if p.Fleet.Owner != request.Owner || p.Fleet.Generation != request.Generation ||
		len(p.Declarations) > request.Limit || len(p.Declarations) > SourceAuthorityFleetPageLimit {
		return fmt.Errorf("%w: source authority fleet page fence mismatch", ErrMutationConflict)
	}
	if err := validateSourceAuthorityDeclarations(p.Declarations); err != nil {
		return err
	}
	if len(p.Declarations) != 0 && request.After != "" &&
		p.Declarations[0].Authority <= request.After {
		return fmt.Errorf("%w: source authority fleet page did not advance", ErrInvalidObject)
	}
	if p.Next != "" {
		if len(p.Declarations) == 0 ||
			p.Next != p.Declarations[len(p.Declarations)-1].Authority {
			return fmt.Errorf("%w: invalid source authority fleet next cursor", ErrInvalidObject)
		}
	}
	if sourceAuthorityDeclarationsBytes(p.Declarations) > SourceAuthorityFleetPageByteLimit {
		return fmt.Errorf("%w: source authority fleet page exceeds byte limit", ErrInvalidObject)
	}
	return nil
}

// Validate verifies one staged fleet page request.
func (r SourceAuthorityFleetReconcileRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Generation == 0 || r.Generation <= r.ExpectedGeneration ||
		r.AuthorityCount > SourceAuthorityFleetAuthorityLimit ||
		len(r.Declarations) > SourceAuthorityFleetPageLimit ||
		r.AuthoritiesDigest == ([32]byte{}) || r.DeclarationsDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority fleet reconciliation request", ErrInvalidObject)
	}
	if len(r.Declarations) == 0 && (!r.Complete || r.AuthorityCount != 0 || r.Sequence != 0) {
		return fmt.Errorf("%w: empty source authority fleet page is not terminal", ErrInvalidObject)
	}
	if uint64(len(r.Declarations)) > r.AuthorityCount ||
		sourceAuthorityDeclarationsBytes(r.Declarations) > SourceAuthorityFleetPageByteLimit {
		return fmt.Errorf("%w: source authority fleet page exceeds declared bounds", ErrInvalidObject)
	}
	if err := validateSourceAuthorityDeclarations(r.Declarations); err != nil {
		return err
	}
	if r.AuthorityCount == 0 {
		digest, _ := SourceAuthorityFleetDigest(nil)
		declarationsDigest, _ := SourceAuthorityFleetDeclarationsDigest(nil)
		if r.AuthoritiesDigest != digest || r.DeclarationsDigest != declarationsDigest {
			return fmt.Errorf("%w: empty source authority fleet digest mismatch", ErrMutationConflict)
		}
	}
	return nil
}

// Validate verifies a returned reconciliation state against its request.
func (s SourceAuthorityFleetReconcileState) ValidateRequest(request SourceAuthorityFleetReconcileRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if s.Owner != request.Owner || s.ExpectedGeneration != request.ExpectedGeneration ||
		s.Generation != request.Generation || s.AuthorityCount != request.AuthorityCount ||
		s.AuthoritiesDigest != request.AuthoritiesDigest ||
		s.DeclarationsDigest != request.DeclarationsDigest ||
		s.NextSequence < request.Sequence+1 {
		return fmt.Errorf("%w: source authority fleet reconciliation response mismatch", ErrMutationConflict)
	}
	return nil
}

// Validate verifies one exact authority retirement request.
func (r SourceAuthorityRetireRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.ExpectedGeneration == 0 || r.Generation <= r.ExpectedGeneration ||
		validateSourceAuthorityID(r.Authority) != nil || r.StageDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority retirement", ErrInvalidObject)
	}
	return nil
}

// Validate verifies a retirement receipt against its request.
func (r SourceAuthorityRetirementReceipt) Validate(request SourceAuthorityRetireRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Owner != request.Owner || r.ExpectedGeneration != request.ExpectedGeneration ||
		r.Generation != request.Generation || r.Authority != request.Authority ||
		r.StageDigest != request.StageDigest || r.ReceiptDigest == ([32]byte{}) {
		return fmt.Errorf("%w: source authority retirement receipt mismatch", ErrMutationConflict)
	}
	return nil
}

// Validate verifies a complete fleet acknowledgement.
func (a SourceAuthorityFleetAcknowledgement) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(a.Owner); err != nil {
		return err
	}
	if a.Generation == 0 || a.Generation <= a.ExpectedGeneration ||
		a.AuthorityCount > SourceAuthorityFleetAuthorityLimit ||
		a.AuthoritiesDigest == ([32]byte{}) || a.DeclarationsDigest == ([32]byte{}) ||
		a.StageDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority fleet acknowledgement", ErrInvalidObject)
	}
	return nil
}

// Validate verifies one exact fleet-stage abort request.
func (r SourceAuthorityFleetAbortRequest) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Generation == 0 || r.Generation <= r.ExpectedGeneration ||
		r.StageDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid source authority fleet abort", ErrInvalidObject)
	}
	return nil
}

// Validate verifies an abort receipt against its exact request.
func (r SourceAuthorityFleetAbortReceipt) Validate(request SourceAuthorityFleetAbortRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Owner != request.Owner || r.ExpectedGeneration != request.ExpectedGeneration ||
		r.Generation != request.Generation || r.StageDigest != request.StageDigest ||
		r.ReceiptDigest == ([32]byte{}) {
		return fmt.Errorf("%w: source authority fleet abort receipt mismatch", ErrMutationConflict)
	}
	return nil
}

// Validate verifies an exact authority runtime fence.
func (f SourceAuthorityRuntimeFence) Validate() error {
	if err := (SourceAuthorityRuntimeRef{
		Owner: f.Owner, Generation: f.Generation, Authority: f.Authority,
	}).Validate(); err != nil {
		return err
	}
	if f.Epoch == ([16]byte{}) {
		return fmt.Errorf("%w: invalid source authority runtime fence", ErrInvalidObject)
	}
	return nil
}

// Validate verifies one current runtime reference.
func (r SourceAuthorityRuntimeRef) Validate() error {
	if err := ValidateSourceAuthorityFleetOwnerID(r.Owner); err != nil {
		return err
	}
	if r.Generation == 0 || validateSourceAuthorityID(r.Authority) != nil {
		return fmt.Errorf("%w: invalid source authority runtime reference", ErrInvalidObject)
	}
	return nil
}

// Validate verifies one exact settled-epoch takeover.
func (r SourceAuthorityRuntimeTakeover) Validate() error {
	if err := r.Ref.Validate(); err != nil {
		return err
	}
	if r.Epoch == ([16]byte{}) || r.Epoch == r.ExpectedEpoch {
		return fmt.Errorf("%w: invalid source authority runtime takeover", ErrInvalidObject)
	}
	if _, _, err := sourceAuthorityRuntimeOwner(r.Process); err != nil {
		return fmt.Errorf("%w: invalid source authority runtime takeover", ErrInvalidObject)
	}
	return nil
}

// Validate verifies a returned runtime state against its exact reference.
func (s SourceAuthorityRuntimeState) Validate(ref SourceAuthorityRuntimeRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if s.Ref != ref || s.DeclarationDigest == ([32]byte{}) ||
		(!s.Closed && s.Epoch == ([16]byte{})) ||
		(s.Epoch == ([16]byte{})) != (s.Process == nil) {
		return fmt.Errorf("%w: source authority runtime state mismatch", ErrMutationConflict)
	}
	if s.Process != nil {
		if _, _, err := sourceAuthorityRuntimeOwner(*s.Process); err != nil {
			return fmt.Errorf("%w: source authority runtime state mismatch", ErrMutationConflict)
		}
	}
	return nil
}

func sourceAuthorityRuntimeOwner(record proc.Record) ([]byte, [32]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, [32]byte{}, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, [32]byte{}, err
	}
	if len(encoded) == 0 || len(encoded) > SourceAuthorityRuntimeOwnerByteLimit {
		return nil, [32]byte{}, fmt.Errorf("%w: source authority runtime owner exceeds byte limit", ErrInvalidObject)
	}
	return encoded, sha256.Sum256(encoded), nil
}

func decodeSourceAuthorityRuntimeOwner(encoded, rawDigest []byte) (proc.Record, error) {
	if len(encoded) == 0 || len(encoded) > SourceAuthorityRuntimeOwnerByteLimit ||
		len(rawDigest) != sha256.Size || sha256.Sum256(encoded) != bytesToDigest(rawDigest) {
		return proc.Record{}, ErrIntegrity
	}
	var record proc.Record
	if err := json.Unmarshal(encoded, &record); err != nil {
		return proc.Record{}, ErrIntegrity
	}
	canonical, digest, err := sourceAuthorityRuntimeOwner(record)
	if err != nil || !bytes.Equal(canonical, encoded) || digest != bytesToDigest(rawDigest) {
		return proc.Record{}, ErrIntegrity
	}
	return record, nil
}

// SourceAuthorityFleetDigest returns the canonical complete-set digest.
func SourceAuthorityFleetDigest(authorities []causal.SourceAuthorityID) ([32]byte, error) {
	if len(authorities) > SourceAuthorityFleetAuthorityLimit {
		return [32]byte{}, fmt.Errorf("%w: source authority fleet exceeds count limit", ErrInvalidObject)
	}
	if err := validateSourceAuthorityIDs(authorities, true); err != nil {
		return [32]byte{}, err
	}
	if sourceAuthorityIDsBytes(authorities) > SourceAuthorityFleetByteLimit {
		return [32]byte{}, fmt.Errorf("%w: source authority fleet exceeds byte limit", ErrInvalidObject)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourceAuthorityFleetDigestPrefix))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(authorities)))
	_, _ = hash.Write(encoded[:])
	for _, authority := range authorities {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(authority)))
		_, _ = hash.Write(encoded[:])
		_, _ = hash.Write([]byte(authority))
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

// SourceAuthorityFleetDeclarationsDigest returns the canonical policy/materializer digest.
func SourceAuthorityFleetDeclarationsDigest(
	declarations []SourceAuthorityDeclaration,
) ([32]byte, error) {
	if len(declarations) > SourceAuthorityFleetAuthorityLimit {
		return [32]byte{}, fmt.Errorf("%w: source authority fleet exceeds count limit", ErrInvalidObject)
	}
	if err := validateSourceAuthorityDeclarations(declarations); err != nil {
		return [32]byte{}, err
	}
	if sourceAuthorityDeclarationsBytes(declarations) > SourceAuthorityFleetByteLimit {
		return [32]byte{}, fmt.Errorf("%w: source authority fleet exceeds byte limit", ErrInvalidObject)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourceAuthorityFleetDigestPrefix + "declarations\x00"))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(declarations)))
	_, _ = hash.Write(encoded[:])
	for _, declaration := range declarations {
		binary.BigEndian.PutUint64(encoded[:], uint64(len(declaration.Authority)))
		_, _ = hash.Write(encoded[:])
		_, _ = hash.Write([]byte(declaration.Authority))
		binary.BigEndian.PutUint64(encoded[:], uint64(len(declaration.DriverID)))
		_, _ = hash.Write(encoded[:])
		_, _ = hash.Write([]byte(declaration.DriverID))
		binary.BigEndian.PutUint64(encoded[:], uint64(len(declaration.DriverConfig)))
		_, _ = hash.Write(encoded[:])
		_, _ = hash.Write(declaration.DriverConfig)
		_, _ = hash.Write(declaration.DeclarationDigest[:])
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

// SourceAuthorityFleetAcknowledgementDigest returns the exact acknowledgement digest.
func SourceAuthorityFleetAcknowledgementDigest(
	ack SourceAuthorityFleetAcknowledgement,
) ([32]byte, error) {
	if err := ack.Validate(); err != nil {
		return [32]byte{}, err
	}
	return digestSourceAuthorityFleetValue("ack", ack)
}

// SourceAuthorityRetirementDigest returns the exact retirement request digest.
func SourceAuthorityRetirementDigest(request SourceAuthorityRetireRequest) ([32]byte, error) {
	if err := request.Validate(); err != nil {
		return [32]byte{}, err
	}
	return digestSourceAuthorityFleetValue("retire", request)
}

// SourceAuthorityFleetAbortDigest returns the exact stage-abort request digest.
func SourceAuthorityFleetAbortDigest(request SourceAuthorityFleetAbortRequest) ([32]byte, error) {
	if err := request.Validate(); err != nil {
		return [32]byte{}, err
	}
	return digestSourceAuthorityFleetValue("abort", request)
}

// SourceAuthorityFleetHead returns acknowledged and pending owner state.
func (c *Catalog) SourceAuthorityFleetHead(
	ctx context.Context,
	owner SourceAuthorityFleetOwnerID,
) (SourceAuthorityFleetStatus, error) {
	if err := ValidateSourceAuthorityFleetOwnerID(owner); err != nil {
		return SourceAuthorityFleetStatus{}, err
	}
	status := SourceAuthorityFleetStatus{}
	current, found, err := readSourceAuthorityFleetState(ctx, c.db, owner)
	if err != nil {
		return SourceAuthorityFleetStatus{}, err
	}
	if found {
		status.Current = &current
	}
	pending, found, err := readSourceAuthorityFleetReconcileState(ctx, c.db, owner)
	if err != nil {
		return SourceAuthorityFleetStatus{}, err
	}
	if found {
		status.Pending = &pending
	}
	if status.Current == nil && status.Pending == nil {
		return SourceAuthorityFleetStatus{}, ErrNotFound
	}
	if err := status.Validate(); err != nil {
		return SourceAuthorityFleetStatus{}, err
	}
	return status, nil
}

// SourceAuthorityFleetPage returns one bounded acknowledged membership page.
func (c *Catalog) SourceAuthorityFleetPage(
	ctx context.Context,
	request SourceAuthorityFleetPageRequest,
) (page SourceAuthorityFleetPage, err error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityFleetPage{}, err
	}
	fleet, found, err := readSourceAuthorityFleetState(ctx, c.db, request.Owner)
	if err != nil {
		return SourceAuthorityFleetPage{}, err
	}
	if !found {
		return SourceAuthorityFleetPage{}, ErrNotFound
	}
	if fleet.Generation != request.Generation {
		return SourceAuthorityFleetPage{}, ErrGenerationMismatch
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT source_authority, driver_id, driver_config, declaration_digest
FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority > ?
ORDER BY source_authority
LIMIT ?`, string(request.Owner), uint64(request.Generation), string(request.After), request.Limit)
	if err != nil {
		return SourceAuthorityFleetPage{}, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	declarations := make([]SourceAuthorityDeclaration, 0, request.Limit)
	bytesRead := 0
	for rows.Next() {
		var authority string
		var driverID string
		var driverConfig, declarationDigest []byte
		if err := rows.Scan(&authority, &driverID, &driverConfig, &declarationDigest); err != nil {
			return SourceAuthorityFleetPage{}, err
		}
		if len(declarationDigest) != 32 {
			return SourceAuthorityFleetPage{}, ErrIntegrity
		}
		declaration := SourceAuthorityDeclaration{
			Authority: causal.SourceAuthorityID(authority), DriverID: driverID,
			DriverConfig: driverConfig,
		}
		copy(declaration.DeclarationDigest[:], declarationDigest)
		bytesRead += len(authority) + len(driverID) + len(driverConfig) + len(declarationDigest)
		if bytesRead > SourceAuthorityFleetPageByteLimit {
			return SourceAuthorityFleetPage{}, ErrIntegrity
		}
		declarations = append(declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		return SourceAuthorityFleetPage{}, err
	}
	page = SourceAuthorityFleetPage{Fleet: fleet, Declarations: declarations}
	if len(declarations) == request.Limit {
		last := declarations[len(declarations)-1].Authority
		var more int
		if err := c.db.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_authority_fleet_members
    WHERE owner_id = ? AND generation = ? AND source_authority > ?
)`, string(request.Owner), uint64(request.Generation), string(last)).Scan(&more); err != nil {
			return SourceAuthorityFleetPage{}, err
		}
		if more != 0 {
			page.Next = last
		}
	}
	if err := page.Validate(request); err != nil {
		return SourceAuthorityFleetPage{}, err
	}
	return page, nil
}

// ReconcileSourceAuthorityFleet appends one exact bounded desired-fleet page.
func (c *Catalog) ReconcileSourceAuthorityFleet(
	ctx context.Context,
	request SourceAuthorityFleetReconcileRequest,
) (SourceAuthorityFleetReconcileState, error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	requestDigest, err := sourceAuthorityFleetRequestDigest(request)
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	defer func() { _ = tx.Rollback() }()

	current, currentFound, err := readSourceAuthorityFleetState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	switch {
	case currentFound && current.Generation != request.ExpectedGeneration:
		return SourceAuthorityFleetReconcileState{}, ErrGenerationMismatch
	case !currentFound && request.ExpectedGeneration != 0:
		return SourceAuthorityFleetReconcileState{}, ErrGenerationMismatch
	}

	state, found, err := readSourceAuthorityFleetReconcileState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if !found {
		seed, seedErr := sourceAuthorityFleetStageSeed(request)
		if seedErr != nil {
			return SourceAuthorityFleetReconcileState{}, seedErr
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_stages(
    owner_id, expected_generation, generation, next_sequence, received_count,
    authority_count, byte_count, authorities_digest, declarations_digest,
    stage_seed, stage_digest, complete
) VALUES (?, ?, ?, 0, 0, ?, 0, ?, ?, ?, ?, 0)`,
			string(request.Owner), uint64(request.ExpectedGeneration), uint64(request.Generation),
			request.AuthorityCount, request.AuthoritiesDigest[:],
			request.DeclarationsDigest[:], seed[:], seed[:]); err != nil {
			return SourceAuthorityFleetReconcileState{}, mapConstraint(err)
		}
		state = SourceAuthorityFleetReconcileState{
			Owner: request.Owner, ExpectedGeneration: request.ExpectedGeneration,
			Generation: request.Generation, AuthorityCount: request.AuthorityCount,
			AuthoritiesDigest:  request.AuthoritiesDigest,
			DeclarationsDigest: request.DeclarationsDigest,
			StageSeed:          seed, StageDigest: seed,
		}
	} else if !equalSourceAuthorityFleetIdentity(state, request) {
		return SourceAuthorityFleetReconcileState{}, ErrMutationConflict
	}

	var storedRequest, storedResponse []byte
	err = tx.QueryRowContext(ctx, `
SELECT request_digest, response_json
FROM source_authority_fleet_stage_pages
WHERE owner_id = ? AND generation = ? AND sequence = ?`,
		string(request.Owner), uint64(request.Generation), request.Sequence).
		Scan(&storedRequest, &storedResponse)
	if err == nil {
		if !bytes.Equal(storedRequest, requestDigest[:]) {
			return SourceAuthorityFleetReconcileState{}, ErrMutationConflict
		}
		var replay SourceAuthorityFleetReconcileState
		if err := json.Unmarshal(storedResponse, &replay); err != nil {
			return SourceAuthorityFleetReconcileState{}, ErrIntegrity
		}
		if err := replay.ValidateRequest(request); err != nil {
			return SourceAuthorityFleetReconcileState{}, ErrIntegrity
		}
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if state.Complete || state.NextSequence != request.Sequence {
		return SourceAuthorityFleetReconcileState{}, ErrMutationConflict
	}
	if len(request.Declarations) != 0 {
		var last string
		err := tx.QueryRowContext(ctx, `
SELECT source_authority
FROM source_authority_fleet_stage_members
WHERE owner_id = ? AND generation = ?
ORDER BY source_authority DESC LIMIT 1`,
			string(request.Owner), uint64(request.Generation)).Scan(&last)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return SourceAuthorityFleetReconcileState{}, err
		}
		if err == nil && causal.SourceAuthorityID(last) >= request.Declarations[0].Authority {
			return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
				"%w: source authority fleet page does not continue ordering", ErrInvalidObject,
			)
		}
		for _, declaration := range request.Declarations {
			authority := declaration.Authority
			var foreignOwner int
			if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_authority_claims
    WHERE source_authority = ? AND owner_id <> ?
)`, string(authority), string(request.Owner)).Scan(&foreignOwner); err != nil {
				return SourceAuthorityFleetReconcileState{}, err
			}
			if foreignOwner != 0 {
				return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
					"%w: source authority belongs to another fleet owner", ErrMutationConflict,
				)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_stage_members(
    owner_id, generation, source_authority, driver_id, driver_config, declaration_digest
) VALUES (?, ?, ?, ?, ?, ?)`,
				string(request.Owner), uint64(request.Generation), string(authority),
				declaration.DriverID, sourceDriverConfigBytes(declaration.DriverConfig),
				declaration.DeclarationDigest[:]); err != nil {
				return SourceAuthorityFleetReconcileState{}, mapConstraint(err)
			}
			result, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_claims(
    source_authority, owner_id,
    pending_generation, pending_stage_seed, pending_declaration_digest
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    pending_generation = excluded.pending_generation,
    pending_stage_seed = excluded.pending_stage_seed,
    pending_declaration_digest = excluded.pending_declaration_digest
WHERE source_authority_claims.owner_id = excluded.owner_id
  AND (
      source_authority_claims.pending_generation IS NULL OR
      (
          source_authority_claims.pending_generation = excluded.pending_generation AND
          source_authority_claims.pending_stage_seed = excluded.pending_stage_seed AND
          source_authority_claims.pending_declaration_digest =
              excluded.pending_declaration_digest
      )
  )`,
				string(authority), string(request.Owner), uint64(request.Generation),
				state.StageSeed[:], declaration.DeclarationDigest[:])
			if err != nil {
				return SourceAuthorityFleetReconcileState{}, mapConstraint(err)
			}
			if changed, _ := result.RowsAffected(); changed != 1 {
				return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
					"%w: source authority is claimed by another pending fleet",
					ErrMutationConflict,
				)
			}
		}
	}

	nextCount := state.ReceivedCount + uint64(len(request.Declarations))
	nextBytes := sourceAuthorityDeclarationsBytes(request.Declarations)
	var priorBytes int
	if err := tx.QueryRowContext(ctx, `
SELECT byte_count FROM source_authority_fleet_stages WHERE owner_id = ?`,
		string(request.Owner)).Scan(&priorBytes); err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if nextCount > request.AuthorityCount || priorBytes+nextBytes > SourceAuthorityFleetByteLimit {
		return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
			"%w: source authority fleet exceeds cumulative bounds", ErrInvalidObject,
		)
	}
	if !request.Complete && nextCount == request.AuthorityCount {
		return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
			"%w: complete source authority fleet is not terminal", ErrInvalidObject,
		)
	}
	if request.Complete {
		if nextCount != request.AuthorityCount {
			return SourceAuthorityFleetReconcileState{}, fmt.Errorf(
				"%w: incomplete terminal source authority fleet page", ErrInvalidObject,
			)
		}
		actualAuthoritiesDigest, actualDeclarationsDigest, err :=
			sourceAuthorityFleetRowsDigests(ctx, tx, request.Owner, request.Generation)
		if err != nil {
			return SourceAuthorityFleetReconcileState{}, err
		}
		if actualAuthoritiesDigest != request.AuthoritiesDigest ||
			actualDeclarationsDigest != request.DeclarationsDigest {
			return SourceAuthorityFleetReconcileState{}, ErrMutationConflict
		}
	}
	previousStageDigest := state.StageDigest
	state.NextSequence++
	state.ReceivedCount = nextCount
	state.Complete = request.Complete
	state.StageDigest = advanceSourceAuthorityFleetStageDigest(previousStageDigest, requestDigest)
	result, err := tx.ExecContext(ctx, `
UPDATE source_authority_fleet_stages
SET next_sequence = ?, received_count = ?, byte_count = ?,
    stage_digest = ?, complete = ?
WHERE owner_id = ? AND expected_generation = ? AND generation = ?
  AND next_sequence = ? AND received_count = ? AND stage_digest = ? AND complete = 0`,
		state.NextSequence, state.ReceivedCount, priorBytes+nextBytes, state.StageDigest[:],
		boolInt(state.Complete), string(request.Owner), uint64(request.ExpectedGeneration),
		uint64(request.Generation), request.Sequence,
		state.ReceivedCount-uint64(len(request.Declarations)),
		previousStageDigest[:])
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return SourceAuthorityFleetReconcileState{}, ErrMutationConflict
	}
	response, err := json.Marshal(state)
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_stage_pages(
    owner_id, generation, sequence, request_digest, response_json
) VALUES (?, ?, ?, ?, ?)`,
		string(request.Owner), uint64(request.Generation), request.Sequence, requestDigest[:], response); err != nil {
		return SourceAuthorityFleetReconcileState{}, mapConstraint(err)
	}
	if state.Complete {
		if err := publishDesiredSourceAuthorityFleetTx(ctx, tx, state); err != nil {
			return SourceAuthorityFleetReconcileState{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return SourceAuthorityFleetReconcileState{}, err
	}
	if state.Complete {
		c.topology.signal()
	}
	return state, nil
}

// AbortSourceAuthorityFleet discards one exact unpublished stage.
func (c *Catalog) AbortSourceAuthorityFleet(
	ctx context.Context,
	request SourceAuthorityFleetAbortRequest,
) (SourceAuthorityFleetAbortReceipt, error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	receiptDigest, err := SourceAuthorityFleetAbortDigest(request)
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var stored SourceAuthorityFleetAbortReceipt
	var storedDigest []byte
	var storedReceiptDigest []byte
	var encoded []byte
	err = tx.QueryRowContext(ctx, `
SELECT owner_id, expected_generation, generation, stage_digest, receipt_digest, result_json
FROM source_authority_fleet_abort_receipts
WHERE owner_id = ? AND generation = ?`,
		string(request.Owner), uint64(request.Generation)).
		Scan(&stored.Owner, &stored.ExpectedGeneration, &stored.Generation,
			&storedDigest, &storedReceiptDigest, &encoded)
	if err == nil {
		if len(storedDigest) != 32 || len(storedReceiptDigest) != 32 {
			return SourceAuthorityFleetAbortReceipt{}, ErrIntegrity
		}
		copy(stored.StageDigest[:], storedDigest)
		copy(stored.ReceiptDigest[:], storedReceiptDigest)
		if err := stored.Validate(request); err != nil || stored.ReceiptDigest != receiptDigest {
			if err != nil {
				return SourceAuthorityFleetAbortReceipt{}, err
			}
			return SourceAuthorityFleetAbortReceipt{}, ErrMutationConflict
		}
		var replay SourceAuthorityFleetAbortReceipt
		if err := json.Unmarshal(encoded, &replay); err != nil || replay != stored {
			return SourceAuthorityFleetAbortReceipt{}, ErrIntegrity
		}
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityFleetAbortReceipt{}, err
	}

	current, found, err := readSourceAuthorityFleetState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	if request.ExpectedGeneration == 0 {
		if found {
			return SourceAuthorityFleetAbortReceipt{}, ErrGenerationMismatch
		}
	} else if !found || current.Generation != request.ExpectedGeneration {
		return SourceAuthorityFleetAbortReceipt{}, ErrGenerationMismatch
	}
	pending, found, err := readSourceAuthorityFleetReconcileState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	if !found || pending.ExpectedGeneration != request.ExpectedGeneration ||
		pending.Generation != request.Generation || pending.StageDigest != request.StageDigest {
		return SourceAuthorityFleetAbortReceipt{}, ErrMutationConflict
	}
	if desired, desiredFound, err := readDesiredSourceAuthorityFleet(ctx, tx, request.Owner); err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	} else if desiredFound && desired.Generation == request.Generation {
		return SourceAuthorityFleetAbortReceipt{}, fmt.Errorf(
			"%w: published desired source authority fleet cannot be aborted", ErrInvalidTransition,
		)
	}
	deletedClaims, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_claims
WHERE owner_id = ? AND current_generation IS NULL
  AND pending_generation = ? AND pending_stage_seed = ?`,
		string(request.Owner), uint64(request.Generation), pending.StageSeed[:])
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	deletedClaimCount, err := deletedClaims.RowsAffected()
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	updatedClaims, err := tx.ExecContext(ctx, `
UPDATE source_authority_claims
SET pending_generation = NULL,
    pending_stage_seed = NULL,
    pending_declaration_digest = NULL
WHERE owner_id = ? AND current_generation IS NOT NULL
  AND pending_generation = ? AND pending_stage_seed = ?`,
		string(request.Owner), uint64(request.Generation), pending.StageSeed[:])
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	updatedClaimCount, err := updatedClaims.RowsAffected()
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	if deletedClaimCount < 0 || updatedClaimCount < 0 ||
		uint64(deletedClaimCount+updatedClaimCount) != pending.ReceivedCount {
		return SourceAuthorityFleetAbortReceipt{}, ErrIntegrity
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_retirement_receipts
WHERE owner_id = ? AND expected_generation = ? AND generation = ? AND stage_digest = ?`,
		string(request.Owner), uint64(request.ExpectedGeneration), uint64(request.Generation),
		request.StageDigest[:]); err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_fleet_stages
WHERE owner_id = ? AND expected_generation = ? AND generation = ? AND stage_digest = ?`,
		string(request.Owner), uint64(request.ExpectedGeneration), uint64(request.Generation),
		request.StageDigest[:])
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return SourceAuthorityFleetAbortReceipt{}, ErrMutationConflict
	}
	receipt := SourceAuthorityFleetAbortReceipt{
		Owner: request.Owner, ExpectedGeneration: request.ExpectedGeneration,
		Generation: request.Generation, StageDigest: request.StageDigest,
		ReceiptDigest: receiptDigest,
	}
	encoded, err = json.Marshal(receipt)
	if err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_abort_receipts(
    owner_id, expected_generation, generation, stage_digest, receipt_digest, result_json
) VALUES (?, ?, ?, ?, ?, ?)`,
		string(receipt.Owner), uint64(receipt.ExpectedGeneration), uint64(receipt.Generation),
		receipt.StageDigest[:], receipt.ReceiptDigest[:], encoded); err != nil {
		return SourceAuthorityFleetAbortReceipt{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return SourceAuthorityFleetAbortReceipt{}, err
	}
	return receipt, nil
}

func (c *Catalog) setSourceAuthorityRuntimeClosed(
	ctx context.Context,
	fence SourceAuthorityRuntimeFence,
	closed bool,
) error {
	if err := fence.Validate(); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var storedEpoch, ownerJSON, ownerDigest []byte
	var storedClosed int
	if err := tx.QueryRowContext(ctx, `
SELECT member.runtime_epoch, member.runtime_owner_json,
       member.runtime_owner_digest, member.runtime_closed
FROM source_authority_fleet_heads head
JOIN source_authority_fleet_members member
  ON member.owner_id = head.owner_id AND member.generation = head.generation
WHERE head.owner_id = ? AND head.generation = ? AND member.source_authority = ?`,
		string(fence.Owner), uint64(fence.Generation), string(fence.Authority)).
		Scan(&storedEpoch, &ownerJSON, &ownerDigest, &storedClosed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGenerationMismatch
		}
		return err
	}
	if _, err := decodeSourceAuthorityRuntimeOwner(ownerJSON, ownerDigest); err != nil {
		return ErrIntegrity
	}
	if !bytes.Equal(storedEpoch, fence.Epoch[:]) {
		return fmt.Errorf("%w: stale source authority runtime epoch", ErrMutationConflict)
	}
	if storedClosed == boolInt(closed) {
		return tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `
UPDATE source_authority_fleet_members
SET runtime_closed = ?
WHERE owner_id = ? AND generation = ? AND source_authority = ? AND runtime_epoch = ?`,
		boolInt(closed), string(fence.Owner), uint64(fence.Generation),
		string(fence.Authority), fence.Epoch[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrMutationConflict
	}
	return tx.Commit()
}

// SourceAuthorityRuntimeStatus returns the exact epoch of one current member.
func (c *Catalog) SourceAuthorityRuntimeStatus(
	ctx context.Context,
	ref SourceAuthorityRuntimeRef,
) (SourceAuthorityRuntimeState, error) {
	if err := ref.Validate(); err != nil {
		return SourceAuthorityRuntimeState{}, err
	}
	var declarationDigest, epoch, ownerJSON, ownerDigest []byte
	var closed int
	if err := c.db.QueryRowContext(ctx, `
SELECT member.declaration_digest, member.runtime_epoch,
       member.runtime_owner_json, member.runtime_owner_digest, member.runtime_closed
FROM source_authority_fleet_heads head
JOIN source_authority_fleet_members member
  ON member.owner_id = head.owner_id AND member.generation = head.generation
WHERE head.owner_id = ? AND head.generation = ? AND member.source_authority = ?`,
		string(ref.Owner), uint64(ref.Generation), string(ref.Authority)).
		Scan(&declarationDigest, &epoch, &ownerJSON, &ownerDigest, &closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceAuthorityRuntimeState{}, ErrGenerationMismatch
		}
		return SourceAuthorityRuntimeState{}, err
	}
	if len(declarationDigest) != 32 || (len(epoch) != 0 && len(epoch) != 16) ||
		(len(epoch) == 0 && (len(ownerJSON) != 0 || len(ownerDigest) != 0)) ||
		(len(epoch) != 0 && (len(ownerJSON) == 0 || len(ownerDigest) != sha256.Size ||
			sha256.Sum256(ownerJSON) != bytesToDigest(ownerDigest))) {
		return SourceAuthorityRuntimeState{}, ErrIntegrity
	}
	state := SourceAuthorityRuntimeState{Ref: ref, Closed: closed != 0}
	copy(state.DeclarationDigest[:], declarationDigest)
	copy(state.Epoch[:], epoch)
	if len(ownerJSON) != 0 {
		process, err := decodeSourceAuthorityRuntimeOwner(ownerJSON, ownerDigest)
		if err != nil {
			return SourceAuthorityRuntimeState{}, ErrIntegrity
		}
		state.Process = &process
	}
	if err := state.Validate(ref); err != nil {
		return SourceAuthorityRuntimeState{}, ErrIntegrity
	}
	return state, nil
}

// TakeoverSourceAuthorityRuntime atomically opens a new process-owned epoch after exact prior settlement.
func (c *Catalog) TakeoverSourceAuthorityRuntime(
	ctx context.Context,
	takeover SourceAuthorityRuntimeTakeover,
) error {
	if err := takeover.Validate(); err != nil {
		return err
	}
	ownerJSON, ownerDigest, err := sourceAuthorityRuntimeOwner(takeover.Process)
	if err != nil {
		return err
	}
	result, err := c.db.ExecContext(ctx, `
UPDATE source_authority_fleet_members
SET runtime_epoch = ?, runtime_owner_json = ?, runtime_owner_digest = ?, runtime_closed = 0
WHERE owner_id = ? AND generation = ? AND source_authority = ?
  AND runtime_closed = 1
  AND ((? = 1 AND length(runtime_epoch) = 0) OR (? = 0 AND runtime_epoch = ?))
  AND EXISTS (
      SELECT 1 FROM source_authority_fleet_heads head
      WHERE head.owner_id = source_authority_fleet_members.owner_id
        AND head.generation = source_authority_fleet_members.generation
	)`,
		takeover.Epoch[:], ownerJSON, ownerDigest[:],
		string(takeover.Ref.Owner), uint64(takeover.Ref.Generation),
		string(takeover.Ref.Authority), boolInt(takeover.ExpectedEpoch == ([16]byte{})),
		boolInt(takeover.ExpectedEpoch == ([16]byte{})), takeover.ExpectedEpoch[:])
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("%w: source authority runtime takeover fence mismatch", ErrMutationConflict)
	}
	return nil
}

func requireSourceAuthorityRetirementUnreferenced(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
) error {
	var referenced int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM tenant_intents intent
    JOIN tenant_generations generation
      ON generation.tenant_id = intent.tenant_id
     AND generation.generation = intent.target_generation
    WHERE intent.state = 1 AND generation.content_source_id = ?
    UNION ALL
    SELECT 1
    FROM tenant_activations activation
    JOIN tenant_generations generation
      ON generation.tenant_id = activation.tenant_id
     AND generation.generation = activation.active_generation
    WHERE activation.active_generation IS NOT NULL AND generation.content_source_id = ?
    UNION ALL
    SELECT 1 FROM source_tenant_targets WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_object_bindings WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_tenant_roots WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_authority_bindings WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_mutation_expectations WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_key_reservations WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_publication_stages WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_driver_stage_receipts
      WHERE source_authority = ? AND forgotten = 0
    UNION ALL
    SELECT 1 FROM source_snapshot_stages WHERE source_authority = ?
    UNION ALL
    SELECT 1 FROM source_snapshot_sessions WHERE source_authority = ?
)`,
		string(authority), string(authority), string(authority), string(authority), string(authority),
		string(authority), string(authority), string(authority), string(authority), string(authority),
		string(authority), string(authority)).
		Scan(&referenced); err != nil {
		return err
	}
	if referenced != 0 {
		return fmt.Errorf(
			"%w: source authority retains tenant, write, binding, or materialization dependencies",
			ErrMutationConflict,
		)
	}
	return nil
}

func enqueueRetiredSourceAuthorityObjectMaintenance(
	ctx context.Context,
	tx *sql.Tx,
) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance
SET dirty_revision = MAX(
    dirty_revision,
    (SELECT head FROM tenants WHERE tenants.tenant = catalog_maintenance.tenant)
)
WHERE tenant IN (
    SELECT DISTINCT object.tenant
    FROM source_object_ids source_id
    JOIN objects object ON object.object_id = source_id.object_id
    WHERE source_id.source_authority IN (
        SELECT source_authority FROM temp.fusekit_retiring_source_authorities
    )
)`); err != nil {
		return fmt.Errorf("catalog: refresh retired-source maintenance: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
SELECT candidate.tenant, candidate.head, 0,
       sequence.next_ticket + ROW_NUMBER() OVER (ORDER BY candidate.tenant)
FROM (
    SELECT DISTINCT tenant.tenant, tenant.head
    FROM source_object_ids source_id
    JOIN objects object ON object.object_id = source_id.object_id
    JOIN tenants tenant ON tenant.tenant = object.tenant
    LEFT JOIN catalog_maintenance queued ON queued.tenant = tenant.tenant
    WHERE source_id.source_authority IN (
        SELECT source_authority FROM temp.fusekit_retiring_source_authorities
    )
      AND queued.tenant IS NULL
) candidate
CROSS JOIN catalog_maintenance_sequence sequence
WHERE sequence.singleton = 1`)
	if err != nil {
		return fmt.Errorf("catalog: enqueue retired-source maintenance: %w", err)
	}
	count, err := rowsAffectedInt(result)
	if err != nil {
		return err
	}
	if count != 0 {
		if _, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance_sequence
SET next_ticket = next_ticket + ?
WHERE singleton = 1`, count); err != nil {
			return fmt.Errorf("catalog: advance retired-source maintenance tickets: %w", err)
		}
	}
	return nil
}

func deleteRetiredSourceAuthorityRuntimeState(
	ctx context.Context,
	tx *sql.Tx,
) error {
	if err := enqueueRetiredSourceAuthorityObjectMaintenance(ctx, tx); err != nil {
		return err
	}
	// Ordered child-first. Exact receipts remain immutable generation history.
	for _, query := range []string{
		`DELETE FROM source_snapshot_publications WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_snapshot_sessions WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_snapshot_logical WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_snapshot_stages WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_publication_stages WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_driver_checkpoints WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_observer_configuration_stages WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_physical_logical WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_physical_index WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_observer_inbox WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_observer_checkpoints WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_observer_roots WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_object_ids WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
		`DELETE FROM source_observer_streams WHERE source_authority IN
		    (SELECT source_authority FROM temp.fusekit_retiring_source_authorities)`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

// OpenSourceAuthorityRuntime marks one exact acknowledged member live.
func (c *Catalog) OpenSourceAuthorityRuntime(
	ctx context.Context,
	fence SourceAuthorityRuntimeFence,
) error {
	return c.setSourceAuthorityRuntimeClosed(ctx, fence, false)
}

// CloseSourceAuthorityRuntime marks one exact acknowledged member terminated.
func (c *Catalog) CloseSourceAuthorityRuntime(
	ctx context.Context,
	fence SourceAuthorityRuntimeFence,
) error {
	return c.setSourceAuthorityRuntimeClosed(ctx, fence, true)
}

// RetireSourceAuthority removes one closed, absent, dependency-free authority.
func (c *Catalog) RetireSourceAuthority(
	ctx context.Context,
	request SourceAuthorityRetireRequest,
) (SourceAuthorityRetirementReceipt, error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	receiptDigest, err := SourceAuthorityRetirementDigest(request)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	defer func() { _ = tx.Rollback() }()

	stored, encoded, err := scanSourceAuthorityRetirementReceipt(tx.QueryRowContext(ctx, `
SELECT owner_id, expected_generation, generation, source_authority,
       stage_digest, receipt_digest, result_json
FROM source_authority_retirement_receipts
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(request.Owner), uint64(request.Generation), string(request.Authority)))
	if err == nil {
		if err := sourceAuthorityRetirementReceiptMatches(stored, request); err != nil {
			return SourceAuthorityRetirementReceipt{}, err
		}
		var replay SourceAuthorityRetirementReceipt
		if err := json.Unmarshal(encoded, &replay); err != nil || replay != stored {
			return SourceAuthorityRetirementReceipt{}, ErrIntegrity
		}
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityRetirementReceipt{}, err
	}

	current, found, err := readSourceAuthorityFleetState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	if !found || current.Generation != request.ExpectedGeneration {
		return SourceAuthorityRetirementReceipt{}, ErrGenerationMismatch
	}
	pending, found, err := readSourceAuthorityFleetReconcileState(ctx, tx, request.Owner)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	if !found || !pending.Complete || pending.Generation != request.Generation ||
		pending.ExpectedGeneration != request.ExpectedGeneration ||
		pending.StageDigest != request.StageDigest {
		return SourceAuthorityRetirementReceipt{}, ErrMutationConflict
	}
	var closed int
	if err := tx.QueryRowContext(ctx, `
SELECT runtime_closed FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND source_authority = ?`,
		string(request.Owner), uint64(request.ExpectedGeneration), string(request.Authority)).
		Scan(&closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceAuthorityRetirementReceipt{}, ErrGenerationMismatch
		}
		return SourceAuthorityRetirementReceipt{}, err
	}
	if closed == 0 {
		return SourceAuthorityRetirementReceipt{}, fmt.Errorf(
			"%w: source authority runtime is still open", ErrMutationConflict,
		)
	}
	var desired int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_authority_fleet_stage_members
    WHERE owner_id = ? AND generation = ? AND source_authority = ?
)`, string(request.Owner), uint64(request.Generation), string(request.Authority)).Scan(&desired); err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	if desired != 0 {
		return SourceAuthorityRetirementReceipt{}, fmt.Errorf(
			"%w: desired source authority cannot be retired", ErrMutationConflict,
		)
	}
	if err := requireSourceAuthorityRetirementUnreferenced(ctx, tx, request.Authority); err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	if err := retirePrivateMutationObjects(
		ctx, tx, "source_authority = ?", string(request.Authority),
	); err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	receipt := SourceAuthorityRetirementReceipt{
		Owner: request.Owner, ExpectedGeneration: request.ExpectedGeneration,
		Generation: request.Generation, Authority: request.Authority,
		StageDigest: request.StageDigest, ReceiptDigest: receiptDigest,
	}
	encoded, err = json.Marshal(receipt)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_retirement_receipts(
    owner_id, expected_generation, generation, source_authority,
    stage_digest, receipt_digest, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(receipt.Owner), uint64(receipt.ExpectedGeneration), uint64(receipt.Generation),
		string(receipt.Authority), receipt.StageDigest[:], receipt.ReceiptDigest[:], encoded); err != nil {
		return SourceAuthorityRetirementReceipt{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return SourceAuthorityRetirementReceipt{}, err
	}
	return receipt, nil
}

// AcknowledgeSourceAuthorityFleet atomically commits one reconciled complete set.
func (c *Catalog) AcknowledgeSourceAuthorityFleet(
	ctx context.Context,
	ack SourceAuthorityFleetAcknowledgement,
) (SourceAuthorityFleetState, error) {
	if err := ack.Validate(); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	ackDigest, err := SourceAuthorityFleetAcknowledgementDigest(ack)
	if err != nil {
		return SourceAuthorityFleetState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceAuthorityFleetState{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var storedExpected, storedCount uint64
	var storedAuthorities, storedDeclarations, storedStage, storedAck, storedResult []byte
	err = tx.QueryRowContext(ctx, `
SELECT expected_generation, authority_count, authorities_digest, declarations_digest,
       stage_digest, acknowledgement_digest, result_json
FROM source_authority_fleet_ack_receipts
WHERE owner_id = ? AND generation = ?`, string(ack.Owner), uint64(ack.Generation)).Scan(
		&storedExpected, &storedCount, &storedAuthorities, &storedDeclarations,
		&storedStage, &storedAck, &storedResult,
	)
	if err == nil {
		if storedExpected != uint64(ack.ExpectedGeneration) || storedCount != ack.AuthorityCount ||
			!bytes.Equal(storedAuthorities, ack.AuthoritiesDigest[:]) ||
			!bytes.Equal(storedDeclarations, ack.DeclarationsDigest[:]) ||
			!bytes.Equal(storedStage, ack.StageDigest[:]) ||
			!bytes.Equal(storedAck, ackDigest[:]) {
			return SourceAuthorityFleetState{}, ErrMutationConflict
		}
		var replay SourceAuthorityFleetState
		if err := json.Unmarshal(storedResult, &replay); err != nil {
			return SourceAuthorityFleetState{}, ErrIntegrity
		}
		if err := sourceAuthorityFleetStateMatchesAcknowledgement(replay, ack); err != nil {
			return SourceAuthorityFleetState{}, ErrIntegrity
		}
		return replay, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityFleetState{}, err
	}

	current, currentFound, err := readSourceAuthorityFleetState(ctx, tx, ack.Owner)
	if err != nil {
		return SourceAuthorityFleetState{}, err
	}
	switch {
	case currentFound && current.Generation != ack.ExpectedGeneration:
		return SourceAuthorityFleetState{}, ErrGenerationMismatch
	case !currentFound && ack.ExpectedGeneration != 0:
		return SourceAuthorityFleetState{}, ErrGenerationMismatch
	}
	pending, found, err := readSourceAuthorityFleetReconcileState(ctx, tx, ack.Owner)
	if err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if !found || !pending.Complete || pending.Generation != ack.Generation ||
		pending.ExpectedGeneration != ack.ExpectedGeneration ||
		pending.AuthorityCount != ack.AuthorityCount ||
		pending.AuthoritiesDigest != ack.AuthoritiesDigest ||
		pending.DeclarationsDigest != ack.DeclarationsDigest ||
		pending.StageDigest != ack.StageDigest {
		return SourceAuthorityFleetState{}, ErrMutationConflict
	}

	if _, err := tx.ExecContext(ctx, `
DROP TABLE IF EXISTS temp.fusekit_retiring_source_authorities;
CREATE TEMP TABLE fusekit_retiring_source_authorities(
    source_authority TEXT PRIMARY KEY
) WITHOUT ROWID;`); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if currentFound {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO temp.fusekit_retiring_source_authorities(source_authority)
SELECT current.source_authority
FROM source_authority_fleet_members current
WHERE current.owner_id = ? AND current.generation = ?
  AND NOT EXISTS (
      SELECT 1 FROM source_authority_fleet_stage_members desired
      WHERE desired.owner_id = ? AND desired.generation = ?
        AND desired.source_authority = current.source_authority
  )`,
			string(ack.Owner), uint64(ack.ExpectedGeneration),
			string(ack.Owner), uint64(ack.Generation)); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		var openMembers int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ? AND runtime_closed = 0`,
			string(ack.Owner), uint64(ack.ExpectedGeneration)).Scan(&openMembers); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if openMembers != 0 {
			return SourceAuthorityFleetState{}, fmt.Errorf(
				"%w: source authority fleet retains open runtimes", ErrMutationConflict,
			)
		}
		var retainedMutationLiability int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM source_mutation_expectations liability
    JOIN source_authority_fleet_members current
      ON current.source_authority = liability.source_authority
     AND current.owner_id = ? AND current.generation = ?
    JOIN source_authority_fleet_stage_members desired
      ON desired.source_authority = liability.source_authority
     AND desired.owner_id = ? AND desired.generation = ?
)`,
			string(ack.Owner), uint64(ack.ExpectedGeneration),
			string(ack.Owner), uint64(ack.Generation)).Scan(&retainedMutationLiability); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if retainedMutationLiability != 0 {
			return SourceAuthorityFleetState{}, fmt.Errorf(
				"%w: retained source authority has unsettled mutation liability", ErrMutationConflict,
			)
		}
		var missingRetirements int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM temp.fusekit_retiring_source_authorities retiring
WHERE NOT EXISTS (
    SELECT 1 FROM source_authority_retirement_receipts retired
    WHERE retired.owner_id = ? AND retired.expected_generation = ?
      AND retired.generation = ? AND retired.source_authority = retiring.source_authority
      AND retired.stage_digest = ?
)`,
			string(ack.Owner), uint64(ack.ExpectedGeneration), uint64(ack.Generation),
			ack.StageDigest[:]).Scan(&missingRetirements); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if missingRetirements != 0 {
			return SourceAuthorityFleetState{}, fmt.Errorf(
				"%w: source authority fleet has unretired absent members", ErrMutationConflict,
			)
		}
		var residual int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM tenant_intents intent
    JOIN tenant_generations dependency
      ON dependency.tenant_id = intent.tenant_id
     AND dependency.generation = intent.target_generation
    JOIN temp.fusekit_retiring_source_authorities retiring
      ON retiring.source_authority = dependency.content_source_id
    WHERE intent.state = 1
    UNION ALL
    SELECT 1
    FROM tenant_activations activation
    JOIN tenant_generations dependency
      ON dependency.tenant_id = activation.tenant_id
     AND dependency.generation = activation.active_generation
    JOIN temp.fusekit_retiring_source_authorities retiring
      ON retiring.source_authority = dependency.content_source_id
    WHERE activation.active_generation IS NOT NULL
    UNION ALL
    SELECT 1 FROM source_tenant_targets dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_object_bindings dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_tenant_roots dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_authority_bindings dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_mutation_expectations dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_key_reservations dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_publication_stages dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_snapshot_stages dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
    UNION ALL
    SELECT 1 FROM source_snapshot_sessions dependency
    JOIN temp.fusekit_retiring_source_authorities retiring USING (source_authority)
)`).Scan(&residual); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if residual != 0 {
			return SourceAuthorityFleetState{}, fmt.Errorf(
				"%w: source authority retirement proof is no longer drain-safe", ErrMutationConflict,
			)
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleets(
    owner_id, generation, authority_count, authorities_digest,
    declarations_digest, acknowledgement_digest
) VALUES (?, ?, ?, ?, ?, ?)`,
		string(ack.Owner), uint64(ack.Generation), ack.AuthorityCount,
		ack.AuthoritiesDigest[:], ack.DeclarationsDigest[:], ackDigest[:]); err != nil {
		return SourceAuthorityFleetState{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_members(
    owner_id, generation, source_authority, driver_id, driver_config, declaration_digest,
    runtime_epoch, runtime_closed
)
SELECT owner_id, generation, source_authority, driver_id, driver_config, declaration_digest, X'', 1
FROM source_authority_fleet_stage_members
WHERE owner_id = ? AND generation = ?`,
		string(ack.Owner), uint64(ack.Generation)); err != nil {
		return SourceAuthorityFleetState{}, mapConstraint(err)
	}
	if currentFound {
		if _, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_claims
WHERE owner_id = ? AND current_generation = ?
  AND source_authority IN (
      SELECT source_authority FROM temp.fusekit_retiring_source_authorities
  )`,
			string(ack.Owner), uint64(ack.ExpectedGeneration)); err != nil {
			return SourceAuthorityFleetState{}, err
		}
	}
	claimResult, err := tx.ExecContext(ctx, `
UPDATE source_authority_claims
SET current_generation = pending_generation,
    current_declaration_digest = pending_declaration_digest,
    pending_generation = NULL,
    pending_stage_seed = NULL,
    pending_declaration_digest = NULL
WHERE owner_id = ? AND pending_generation = ? AND pending_stage_seed = ?
  AND EXISTS (
      SELECT 1 FROM source_authority_fleet_stage_members desired
      WHERE desired.owner_id = ? AND desired.generation = ?
        AND desired.source_authority = source_authority_claims.source_authority
        AND desired.declaration_digest =
            source_authority_claims.pending_declaration_digest
  )`,
		string(ack.Owner), uint64(ack.Generation), pending.StageSeed[:],
		string(ack.Owner), uint64(ack.Generation))
	if err != nil {
		return SourceAuthorityFleetState{}, mapConstraint(err)
	}
	if changed, _ := claimResult.RowsAffected(); uint64(changed) != ack.AuthorityCount {
		return SourceAuthorityFleetState{}, fmt.Errorf(
			"%w: source authority pending claims are incomplete", ErrMutationConflict,
		)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_claims
WHERE owner_id = ? AND current_generation IS NULL AND pending_generation IS NULL`,
		string(ack.Owner)); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	var claimCount uint64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_authority_claims
WHERE owner_id = ? AND current_generation = ?`,
		string(ack.Owner), uint64(ack.Generation)).Scan(&claimCount); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if claimCount != ack.AuthorityCount {
		return SourceAuthorityFleetState{}, fmt.Errorf(
			"%w: source authority claim collision", ErrMutationConflict,
		)
	}
	if currentFound {
		if _, err := tx.ExecContext(ctx, `
DELETE FROM source_observer_configuration_stages
WHERE fleet_owner_id = ? AND fleet_generation = ?`,
			string(ack.Owner), uint64(ack.ExpectedGeneration)); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if err := deleteRetiredSourceAuthorityRuntimeState(ctx, tx); err != nil {
			return SourceAuthorityFleetState{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE source_observer_streams
SET fleet_generation = ?
WHERE fleet_owner_id = ? AND fleet_generation = ?
  AND source_authority IN (
      SELECT source_authority FROM source_authority_fleet_stage_members
      WHERE owner_id = ? AND generation = ?
  )`,
			uint64(ack.Generation), string(ack.Owner), uint64(ack.ExpectedGeneration),
			string(ack.Owner), uint64(ack.Generation)); err != nil {
			return SourceAuthorityFleetState{}, err
		}
	}
	var headResult sql.Result
	if currentFound {
		headResult, err = tx.ExecContext(ctx, `
UPDATE source_authority_fleet_heads
SET generation = ?
WHERE owner_id = ? AND generation = ?`,
			uint64(ack.Generation), string(ack.Owner), uint64(ack.ExpectedGeneration))
	} else {
		headResult, err = tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_heads(owner_id, generation) VALUES (?, ?)`,
			string(ack.Owner), uint64(ack.Generation))
	}
	if err != nil {
		return SourceAuthorityFleetState{}, mapConstraint(err)
	}
	if changed, _ := headResult.RowsAffected(); changed != 1 {
		return SourceAuthorityFleetState{}, ErrGenerationMismatch
	}
	var memberCount uint64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_authority_fleet_members
WHERE owner_id = ? AND generation = ?`,
		string(ack.Owner), uint64(ack.Generation)).Scan(&memberCount); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if memberCount != ack.AuthorityCount {
		return SourceAuthorityFleetState{}, ErrIntegrity
	}
	state := SourceAuthorityFleetState{
		Owner: ack.Owner, Generation: ack.Generation, AuthorityCount: ack.AuthorityCount,
		AuthoritiesDigest: ack.AuthoritiesDigest, DeclarationsDigest: ack.DeclarationsDigest,
		AcknowledgementDigest: ackDigest,
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_authority_fleet_ack_receipts(
    owner_id, generation, expected_generation, authority_count,
    authorities_digest, declarations_digest, stage_digest,
    acknowledgement_digest, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(ack.Owner), uint64(ack.Generation), uint64(ack.ExpectedGeneration),
		ack.AuthorityCount, ack.AuthoritiesDigest[:], ack.DeclarationsDigest[:],
		ack.StageDigest[:], ackDigest[:], encoded); err != nil {
		return SourceAuthorityFleetState{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_authority_fleet_stages
WHERE owner_id = ? AND generation = ?`,
		string(ack.Owner), uint64(ack.Generation)); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if _, err := tx.ExecContext(ctx, `
DROP TABLE temp.fusekit_retiring_source_authorities`); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceAuthorityFleetState{}, err
	}
	return state, nil
}

// ValidateSourceAuthorityFleetOwnerID validates one exact fleet owner identifier.
func ValidateSourceAuthorityFleetOwnerID(owner SourceAuthorityFleetOwnerID) error {
	value := string(owner)
	if value == "" || len(value) > causal.SourceAuthorityIDMaxBytes ||
		!utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: invalid source authority fleet owner", ErrInvalidObject)
	}
	return nil
}

// ValidateSourceDriverID validates one stable versionless product driver ID.
func ValidateSourceDriverID(driverID string) error {
	if len(driverID) == 0 || len(driverID) > SourceDriverIDMaxBytes {
		return fmt.Errorf("%w: invalid source driver id", ErrInvalidObject)
	}
	for index := 0; index < len(driverID); index++ {
		value := driverID[index]
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '.' || value == '_' || value == '-' {
			continue
		}
		return fmt.Errorf("%w: invalid source driver id", ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityID(authority causal.SourceAuthorityID) error {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return fmt.Errorf("%w: invalid source authority id", ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityIDs(authorities []causal.SourceAuthorityID, exact bool) error {
	for index, authority := range authorities {
		if err := validateSourceAuthorityID(authority); err != nil {
			return err
		}
		if index != 0 && authorities[index-1] >= authority {
			return fmt.Errorf("%w: source authority ids are not sorted and unique", ErrInvalidObject)
		}
	}
	if exact && !slices.IsSorted(authorities) {
		return fmt.Errorf("%w: source authority ids are not sorted", ErrInvalidObject)
	}
	return nil
}

func validateSourceAuthorityDeclarations(declarations []SourceAuthorityDeclaration) error {
	for index, declaration := range declarations {
		if err := validateSourceAuthorityID(declaration.Authority); err != nil {
			return err
		}
		if declaration.DeclarationDigest == ([32]byte{}) {
			return fmt.Errorf("%w: empty source authority declaration digest", ErrInvalidObject)
		}
		if err := ValidateSourceDriverID(declaration.DriverID); err != nil {
			return err
		}
		if len(declaration.DriverConfig) > SourceDriverConfigMaxBytes {
			return fmt.Errorf("%w: source driver config exceeds byte limit", ErrInvalidObject)
		}
		if index != 0 && declarations[index-1].Authority >= declaration.Authority {
			return fmt.Errorf("%w: source authority declarations are not sorted and unique", ErrInvalidObject)
		}
	}
	return nil
}

func sourceAuthorityIDsBytes(authorities []causal.SourceAuthorityID) int {
	total := 0
	for _, authority := range authorities {
		total += len(authority)
	}
	return total
}

func sourceAuthorityDeclarationsBytes(declarations []SourceAuthorityDeclaration) int {
	total := 0
	for _, declaration := range declarations {
		total += len(declaration.Authority) + len(declaration.DriverID) +
			len(declaration.DriverConfig) + len(declaration.DeclarationDigest)
	}
	return total
}

func sourceDriverConfigBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func sourceAuthorityDeclarationIDs(
	declarations []SourceAuthorityDeclaration,
) []causal.SourceAuthorityID {
	authorities := make([]causal.SourceAuthorityID, len(declarations))
	for index, declaration := range declarations {
		authorities[index] = declaration.Authority
	}
	return authorities
}

func digestSourceAuthorityFleetValue(kind string, value any) ([32]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourceAuthorityFleetDigestPrefix))
	_, _ = hash.Write([]byte(kind))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(encoded)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func sourceAuthorityFleetStageSeed(request SourceAuthorityFleetReconcileRequest) ([32]byte, error) {
	identity := struct {
		Owner              SourceAuthorityFleetOwnerID
		ExpectedGeneration causal.Generation
		Generation         causal.Generation
		AuthorityCount     uint64
		AuthoritiesDigest  [32]byte
		DeclarationsDigest [32]byte
	}{
		Owner: request.Owner, ExpectedGeneration: request.ExpectedGeneration,
		Generation: request.Generation, AuthorityCount: request.AuthorityCount,
		AuthoritiesDigest:  request.AuthoritiesDigest,
		DeclarationsDigest: request.DeclarationsDigest,
	}
	return digestSourceAuthorityFleetValue("stage", identity)
}

func sourceAuthorityFleetRequestDigest(request SourceAuthorityFleetReconcileRequest) ([32]byte, error) {
	if err := request.Validate(); err != nil {
		return [32]byte{}, err
	}
	return digestSourceAuthorityFleetValue("page", request)
}

func advanceSourceAuthorityFleetStageDigest(stage, page [32]byte) [32]byte {
	var raw [64]byte
	copy(raw[:32], stage[:])
	copy(raw[32:], page[:])
	return sha256.Sum256(raw[:])
}

type sourceAuthorityFleetQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readSourceAuthorityFleetState(
	ctx context.Context,
	queryer sourceAuthorityFleetQueryer,
	owner SourceAuthorityFleetOwnerID,
) (SourceAuthorityFleetState, bool, error) {
	var state SourceAuthorityFleetState
	var generation uint64
	var authoritiesDigest, declarationsDigest, acknowledgementDigest []byte
	err := queryer.QueryRowContext(ctx, `
SELECT fleet.owner_id, fleet.generation, fleet.authority_count,
       fleet.authorities_digest, fleet.declarations_digest, fleet.acknowledgement_digest
FROM source_authority_fleet_heads head
JOIN source_authority_fleets fleet
  ON fleet.owner_id = head.owner_id AND fleet.generation = head.generation
WHERE head.owner_id = ?`, string(owner)).Scan(
		&state.Owner, &generation, &state.AuthorityCount, &authoritiesDigest,
		&declarationsDigest, &acknowledgementDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityFleetState{}, false, nil
	}
	if err != nil {
		return SourceAuthorityFleetState{}, false, err
	}
	state.Generation = causal.Generation(generation)
	if len(authoritiesDigest) != 32 || len(declarationsDigest) != 32 ||
		len(acknowledgementDigest) != 32 {
		return SourceAuthorityFleetState{}, false, ErrIntegrity
	}
	copy(state.AuthoritiesDigest[:], authoritiesDigest)
	copy(state.DeclarationsDigest[:], declarationsDigest)
	copy(state.AcknowledgementDigest[:], acknowledgementDigest)
	if err := state.Validate(); err != nil {
		return SourceAuthorityFleetState{}, false, ErrIntegrity
	}
	return state, true, nil
}

func readSourceAuthorityFleetReconcileState(
	ctx context.Context,
	queryer sourceAuthorityFleetQueryer,
	owner SourceAuthorityFleetOwnerID,
) (SourceAuthorityFleetReconcileState, bool, error) {
	var state SourceAuthorityFleetReconcileState
	var expected, generation uint64
	var complete int
	var authoritiesDigest, declarationsDigest, stageSeed, stageDigest []byte
	err := queryer.QueryRowContext(ctx, `
SELECT owner_id, expected_generation, generation, next_sequence, received_count,
       authority_count, authorities_digest, declarations_digest,
       stage_seed, stage_digest, complete
FROM source_authority_fleet_stages WHERE owner_id = ?`, string(owner)).Scan(
		&state.Owner, &expected, &generation, &state.NextSequence, &state.ReceivedCount,
		&state.AuthorityCount, &authoritiesDigest, &declarationsDigest,
		&stageSeed, &stageDigest, &complete,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceAuthorityFleetReconcileState{}, false, nil
	}
	if err != nil {
		return SourceAuthorityFleetReconcileState{}, false, err
	}
	state.ExpectedGeneration = causal.Generation(expected)
	state.Generation = causal.Generation(generation)
	state.Complete = complete != 0
	if len(authoritiesDigest) != 32 || len(declarationsDigest) != 32 ||
		len(stageSeed) != 32 || len(stageDigest) != 32 {
		return SourceAuthorityFleetReconcileState{}, false, ErrIntegrity
	}
	copy(state.AuthoritiesDigest[:], authoritiesDigest)
	copy(state.DeclarationsDigest[:], declarationsDigest)
	copy(state.StageSeed[:], stageSeed)
	copy(state.StageDigest[:], stageDigest)
	if err := state.Validate(); err != nil {
		return SourceAuthorityFleetReconcileState{}, false, ErrIntegrity
	}
	return state, true, nil
}

func equalSourceAuthorityFleetIdentity(
	state SourceAuthorityFleetReconcileState,
	request SourceAuthorityFleetReconcileRequest,
) bool {
	return state.Owner == request.Owner &&
		state.ExpectedGeneration == request.ExpectedGeneration &&
		state.Generation == request.Generation &&
		state.AuthorityCount == request.AuthorityCount &&
		state.AuthoritiesDigest == request.AuthoritiesDigest &&
		state.DeclarationsDigest == request.DeclarationsDigest
}

func scanSourceAuthorityRetirementReceipt(
	row *sql.Row,
) (SourceAuthorityRetirementReceipt, []byte, error) {
	var receipt SourceAuthorityRetirementReceipt
	var expected, generation uint64
	var stageDigest, receiptDigest, result []byte
	err := row.Scan(
		&receipt.Owner, &expected, &generation, &receipt.Authority,
		&stageDigest, &receiptDigest, &result,
	)
	if err != nil {
		return SourceAuthorityRetirementReceipt{}, nil, err
	}
	receipt.ExpectedGeneration = causal.Generation(expected)
	receipt.Generation = causal.Generation(generation)
	if len(stageDigest) != 32 || len(receiptDigest) != 32 {
		return SourceAuthorityRetirementReceipt{}, nil, ErrIntegrity
	}
	copy(receipt.StageDigest[:], stageDigest)
	copy(receipt.ReceiptDigest[:], receiptDigest)
	return receipt, result, nil
}

func sourceAuthorityFleetRowsDigests(
	ctx context.Context,
	tx *sql.Tx,
	owner SourceAuthorityFleetOwnerID,
	generation causal.Generation,
) (authoritiesDigest [32]byte, declarationsDigest [32]byte, err error) {
	rows, err := tx.QueryContext(ctx, `
SELECT source_authority, driver_id, driver_config, declaration_digest
FROM source_authority_fleet_stage_members
WHERE owner_id = ? AND generation = ?
ORDER BY source_authority`, string(owner), uint64(generation))
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()
	declarations := make([]SourceAuthorityDeclaration, 0)
	for rows.Next() {
		var declaration SourceAuthorityDeclaration
		var digest []byte
		if err := rows.Scan(
			&declaration.Authority, &declaration.DriverID, &declaration.DriverConfig, &digest,
		); err != nil {
			return [32]byte{}, [32]byte{}, err
		}
		if len(digest) != 32 {
			return [32]byte{}, [32]byte{}, ErrIntegrity
		}
		copy(declaration.DeclarationDigest[:], digest)
		declarations = append(declarations, declaration)
	}
	if err := rows.Err(); err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	authoritiesDigest, err = SourceAuthorityFleetDigest(sourceAuthorityDeclarationIDs(declarations))
	if err != nil {
		return [32]byte{}, [32]byte{}, err
	}
	declarationsDigest, err = SourceAuthorityFleetDeclarationsDigest(declarations)
	return authoritiesDigest, declarationsDigest, err
}

func sourceAuthorityFleetStateMatchesAcknowledgement(
	state SourceAuthorityFleetState,
	ack SourceAuthorityFleetAcknowledgement,
) error {
	digest, err := SourceAuthorityFleetAcknowledgementDigest(ack)
	if err != nil {
		return err
	}
	if state.Owner != ack.Owner || state.Generation != ack.Generation ||
		state.AuthorityCount != ack.AuthorityCount ||
		state.AuthoritiesDigest != ack.AuthoritiesDigest ||
		state.DeclarationsDigest != ack.DeclarationsDigest ||
		state.AcknowledgementDigest != digest {
		return ErrMutationConflict
	}
	return nil
}

func sourceAuthorityRetirementReceiptMatches(
	receipt SourceAuthorityRetirementReceipt,
	request SourceAuthorityRetireRequest,
) error {
	if err := receipt.Validate(request); err != nil {
		return err
	}
	expected, err := SourceAuthorityRetirementDigest(request)
	if err != nil {
		return err
	}
	if !bytes.Equal(receipt.ReceiptDigest[:], expected[:]) {
		return ErrMutationConflict
	}
	return nil
}
