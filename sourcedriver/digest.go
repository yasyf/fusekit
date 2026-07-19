package sourcedriver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

const digestDomain = "fusekit.sourcedriver.v1\x00"
const targetsDigestDomain = "fusekit.source-driver-targets.v1\x00"
const targetSetIDDomain = "fusekit.source-driver-target-set-id.v1\x00"

// TargetsDigest returns the exact digest of one sorted immutable target declaration.
func TargetsDigest(targets []TargetDeclaration) ([sha256.Size]byte, error) {
	if err := validateTargets(uint64(len(targets)), [sha256.Size]byte{}, targets, false); err != nil {
		return [sha256.Size]byte{}, err
	}
	return targetsDigest(targets)
}

func targetsDigest(targets []TargetDeclaration) ([sha256.Size]byte, error) {
	state, err := newTargetsDigestState(uint64(len(targets)))
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	for _, target := range targets {
		state = appendTargetDigestState(state, target)
	}
	return state, nil
}

func newTargetsDigestState(count uint64) ([sha256.Size]byte, error) {
	if count == 0 || count > MaxTargets {
		return [sha256.Size]byte{}, invalid("target declaration count is invalid")
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(targetsDigestDomain))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], count)
	_, _ = hash.Write(encoded[:])
	var state [sha256.Size]byte
	copy(state[:], hash.Sum(nil))
	return state, nil
}

func appendTargetDigestState(state [sha256.Size]byte, target TargetDeclaration) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(state[:])
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(target.Tenant)))
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write([]byte(target.Tenant))
	binary.BigEndian.PutUint64(encoded[:], uint64(target.Generation))
	_, _ = hash.Write(encoded[:])
	var next [sha256.Size]byte
	copy(next[:], hash.Sum(nil))
	return next
}

// NewTargetSetRef derives one immutable target-set reference from its complete declaration.
func NewTargetSetRef(
	authority causal.SourceAuthorityID,
	authorityGeneration causal.Generation,
	targetEpoch uint64,
	declarationDigest [sha256.Size]byte,
	targets []TargetDeclaration,
) (TargetSetRef, error) {
	digest, err := TargetsDigest(targets)
	if err != nil {
		return TargetSetRef{}, err
	}
	return NewTargetSetRefForDigest(
		authority, authorityGeneration, targetEpoch, declarationDigest, uint64(len(targets)), digest,
	)
}

// NewTargetSetRefForDigest derives one immutable target-set reference from a validated digest.
func NewTargetSetRefForDigest(
	authority causal.SourceAuthorityID,
	authorityGeneration causal.Generation,
	targetEpoch uint64,
	declarationDigest [sha256.Size]byte,
	targetCount uint64,
	targetsDigest [sha256.Size]byte,
) (TargetSetRef, error) {
	ref := TargetSetRef{
		AuthorityGeneration: authorityGeneration,
		TargetEpoch:         targetEpoch,
		DeclarationDigest:   declarationDigest,
		TargetCount:         targetCount,
		TargetsDigest:       targetsDigest,
	}
	var err error
	ref.ID, err = deriveTargetSetID(authority, ref)
	if err != nil {
		return TargetSetRef{}, err
	}
	return ref, nil
}

func deriveTargetSetID(authority causal.SourceAuthorityID, ref TargetSetRef) (TargetSetID, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || ref.AuthorityGeneration == 0 || ref.TargetEpoch == 0 ||
		ref.DeclarationDigest == ([sha256.Size]byte{}) || ref.TargetCount == 0 ||
		ref.TargetCount > MaxTargets || ref.TargetsDigest == ([sha256.Size]byte{}) {
		return TargetSetID{}, invalid("target set identity is invalid")
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(targetSetIDDomain))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(authority)))
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write([]byte(authority))
	binary.BigEndian.PutUint64(encoded[:], uint64(ref.AuthorityGeneration))
	_, _ = hash.Write(encoded[:])
	binary.BigEndian.PutUint64(encoded[:], ref.TargetEpoch)
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write(ref.DeclarationDigest[:])
	binary.BigEndian.PutUint64(encoded[:], ref.TargetCount)
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write(ref.TargetsDigest[:])
	sum := hash.Sum(nil)
	var id TargetSetID
	copy(id[:], sum[:len(id)])
	return id, nil
}

// NewTargetSetState returns the only valid empty resumable state for ref.
func NewTargetSetState(authority causal.SourceAuthorityID, ref TargetSetRef) (TargetSetState, error) {
	if err := ValidateTargetSetRef(authority, ref); err != nil {
		return TargetSetState{}, err
	}
	seed, err := newTargetsDigestState(ref.TargetCount)
	if err != nil {
		return TargetSetState{}, err
	}
	return TargetSetState{Ref: ref, RollingDigest: seed}, nil
}

// NewTargetSetPage constructs the only valid next declaration page.
func NewTargetSetPage(state TargetSetState, targets []TargetDeclaration) (TargetSetPage, error) {
	if err := validateTargetSetStateShape(state); err != nil {
		return TargetSetPage{}, err
	}
	if state.Complete || len(targets) == 0 || len(targets) > MaxTargetPageItems ||
		state.DeclaredCount+uint64(len(targets)) > state.Ref.TargetCount {
		return TargetSetPage{}, invalid("target set page count is invalid")
	}
	page := TargetSetPage{
		Ref: state.Ref, Sequence: state.NextPage, Targets: append([]TargetDeclaration(nil), targets...),
		PreviousDigest: state.RollingDigest,
	}
	prior := state.After
	digest := state.RollingDigest
	for _, target := range page.Targets {
		if err := validateTarget(target); err != nil ||
			(prior.Tenant != "" && compareTarget(prior, target) >= 0) {
			return TargetSetPage{}, invalid("target set page is not strictly ordered")
		}
		digest = appendTargetDigestState(digest, target)
		prior = target
	}
	page.Complete = state.DeclaredCount+uint64(len(page.Targets)) == state.Ref.TargetCount
	page.Digest = digest
	if page.Complete && page.Digest != page.Ref.TargetsDigest {
		return TargetSetPage{}, fmt.Errorf("%w: target set digest differs", ErrIntegrity)
	}
	pageDigest, err := targetSetPageDigest(page)
	if err != nil {
		return TargetSetPage{}, err
	}
	page.PageDigest = pageDigest
	return page, nil
}

// ApplyTargetSetPage validates and advances or exactly replays one page.
func ApplyTargetSetPage(state TargetSetState, page TargetSetPage) (TargetSetState, error) {
	if err := validateTargetSetStateShape(state); err != nil {
		return TargetSetState{}, err
	}
	if state.Ref != page.Ref || len(page.Targets) == 0 || len(page.Targets) > MaxTargetPageItems {
		return TargetSetState{}, ErrConflict
	}
	pageDigest, err := targetSetPageDigest(page)
	if err != nil || page.PageDigest == ([sha256.Size]byte{}) || page.PageDigest != pageDigest {
		return TargetSetState{}, fmt.Errorf("%w: target set page identity differs", ErrIntegrity)
	}
	if page.Sequence+1 == state.NextPage && page.PageDigest == state.LastPageDigest {
		return state, nil
	}
	if state.Complete || page.Sequence != state.NextPage || page.PreviousDigest != state.RollingDigest ||
		state.DeclaredCount+uint64(len(page.Targets)) > state.Ref.TargetCount {
		return TargetSetState{}, ErrConflict
	}
	prior := state.After
	digest := state.RollingDigest
	for _, target := range page.Targets {
		if err := validateTarget(target); err != nil ||
			(prior.Tenant != "" && compareTarget(prior, target) >= 0) {
			return TargetSetState{}, invalid("target set page is not strictly ordered")
		}
		digest = appendTargetDigestState(digest, target)
		prior = target
	}
	wantComplete := state.DeclaredCount+uint64(len(page.Targets)) == state.Ref.TargetCount
	if page.Digest != digest || page.Complete != wantComplete ||
		(page.Complete && page.Digest != page.Ref.TargetsDigest) {
		return TargetSetState{}, fmt.Errorf("%w: target set page proof differs", ErrIntegrity)
	}
	return TargetSetState{
		Ref: page.Ref, NextPage: page.Sequence + 1,
		DeclaredCount: state.DeclaredCount + uint64(len(page.Targets)), After: prior,
		RollingDigest: page.Digest, LastPageDigest: page.PageDigest, Complete: page.Complete,
	}, nil
}

func targetSetPageDigest(page TargetSetPage) ([sha256.Size]byte, error) {
	page.PageDigest = [sha256.Size]byte{}
	return canonicalDigest(struct {
		Domain string        `json:"domain"`
		Page   TargetSetPage `json:"page"`
	}{digestDomain + "target-set-page", page}, MaxPageBytes)
}

func validateTargetSetStateShape(state TargetSetState) error {
	seed, err := newTargetsDigestState(state.Ref.TargetCount)
	if err != nil {
		return err
	}
	if state.DeclaredCount > state.Ref.TargetCount ||
		(state.DeclaredCount == 0 && (state.NextPage != 0 || state.After != (TargetDeclaration{}) ||
			state.RollingDigest != seed || state.LastPageDigest != ([sha256.Size]byte{}) || state.Complete)) ||
		(state.DeclaredCount != 0 && (state.NextPage == 0 || state.After.Tenant == "" ||
			state.RollingDigest == ([sha256.Size]byte{}) || state.LastPageDigest == ([sha256.Size]byte{}))) ||
		state.Complete != (state.DeclaredCount == state.Ref.TargetCount) ||
		(state.Complete && !bytes.Equal(state.RollingDigest[:], state.Ref.TargetsDigest[:])) {
		return invalid("target set state is invalid")
	}
	return nil
}

// SnapshotPageDigest returns the deterministic digest of one immutable page.
func SnapshotPageDigest(revision RevisionToken, objects []Projection) ([sha256.Size]byte, error) {
	return canonicalDigest(struct {
		Domain   string        `json:"domain"`
		Revision RevisionToken `json:"revision"`
		Objects  []Projection  `json:"objects"`
	}{digestDomain + "snapshot", revision, nonNilProjections(objects)}, MaxPageBytes)
}

// ChangePageDigest returns the deterministic digest of one immutable delta page.
func ChangePageDigest(from, to RevisionToken, changes []Change) ([sha256.Size]byte, error) {
	return canonicalDigest(struct {
		Domain  string        `json:"domain"`
		From    RevisionToken `json:"from"`
		To      RevisionToken `json:"to"`
		Changes []Change      `json:"changes"`
	}{digestDomain + "changes", from, to, nonNilChanges(changes)}, MaxPageBytes)
}

// MutationRequestDigest returns the immutable identity of one operation body.
func MutationRequestDigest(request MutationRequest) ([sha256.Size]byte, error) {
	return canonicalDigest(struct {
		Domain  string          `json:"domain"`
		Request MutationRequest `json:"request"`
	}{digestDomain + "mutation-request", request}, MaxPageBytes)
}

// MutationReceiptDigest returns the exact replay proof for one durable state.
func MutationReceiptDigest(receipt MutationReceipt) ([sha256.Size]byte, error) {
	receipt.Digest = [sha256.Size]byte{}
	return canonicalDigest(struct {
		Domain  string          `json:"domain"`
		Receipt MutationReceipt `json:"receipt"`
	}{digestDomain + "mutation-receipt", receipt}, MaxPageBytes)
}

func cursorDigest(cursor PageCursor) ([sha256.Size]byte, error) {
	cursor.Digest = [sha256.Size]byte{}
	return canonicalDigest(struct {
		Domain string     `json:"domain"`
		Cursor PageCursor `json:"cursor"`
	}{digestDomain + "cursor", cursor}, MaxPageBytes)
}

func nonNilProjections(values []Projection) []Projection {
	if values == nil {
		return []Projection{}
	}
	return values
}

func nonNilChanges(values []Change) []Change {
	if values == nil {
		return []Change{}
	}
	return values
}
