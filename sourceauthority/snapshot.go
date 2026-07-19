package sourceauthority

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

const (
	SnapshotPlanPageLimit        = catalog.SourceSnapshotPageLimit
	SnapshotPlanInputLimit       = catalog.SourceSnapshotPageInputLimit
	SnapshotPlanObjectLimit      = catalog.SourceSnapshotPageObjectLimit
	SnapshotPlanPageByteLimit    = catalog.SourceSnapshotPageByteLimit
	SnapshotMaterializationLimit = catalog.SourceSnapshotPageLimit
	snapshotPlanCursorLimit      = 512
)

// SnapshotPlanCursor is policy-owned continuation state with a bounded wire representation.
type SnapshotPlanCursor string

// SnapshotPlanPage is one bounded, deterministic page of an authoritative plan.
type SnapshotPlanPage struct {
	Fence        Fence
	Next         SnapshotPlanCursor
	AffectedKeys []causal.LogicalKey
	Roots        []TenantRoot
	Reads        []MaterializationRequest
}

func validateSnapshotPlanPage(
	ctx context.Context,
	page SnapshotPlanPage,
	view snapshotView,
	cursor SnapshotPlanCursor,
) error {
	if !equalFence(page.Fence, view.fence) {
		return fmt.Errorf("%w: snapshot page changed its fence", ErrInvalidPlan)
	}
	if len(page.AffectedKeys) > SnapshotPlanPageLimit || len(page.Roots) > SnapshotPlanPageLimit ||
		len(page.Reads) > SnapshotMaterializationLimit ||
		len(cursor) > snapshotPlanCursorLimit || len(page.Next) > snapshotPlanCursorLimit ||
		(page.Next != "" && page.Next == cursor) {
		return fmt.Errorf("%w: snapshot plan page exceeds its bounds", ErrInvalidPlan)
	}
	if len(page.AffectedKeys)+len(page.Roots)+len(page.Reads) == 0 && page.Next != "" {
		return fmt.Errorf("%w: empty snapshot plan page did not terminate", ErrInvalidPlan)
	}
	if err := validatePlanRoots(page.Roots, view.tenants, false); err != nil {
		return err
	}
	for index := range page.Roots {
		if index > 0 && page.Roots[index-1].Tenant >= page.Roots[index].Tenant {
			return fmt.Errorf("%w: snapshot roots are not sorted and unique", ErrInvalidPlan)
		}
	}
	if err := validateAffectedKeysPage(page.AffectedKeys); err != nil {
		return err
	}
	inputs := 0
	var locators []catalog.SourceIndexLocator
	seen := make(map[LogicalID]struct{}, len(page.Reads))
	for _, request := range page.Reads {
		if request.Logical == "" || len(request.Inputs) == 0 || len(request.Payload) > maxMaterializationPayloadBytes {
			return fmt.Errorf("%w: incomplete snapshot read request", ErrInvalidPlan)
		}
		if _, duplicate := seen[request.Logical]; duplicate {
			return fmt.Errorf("%w: duplicate snapshot read logical identity", ErrInvalidPlan)
		}
		seen[request.Logical] = struct{}{}
		inputs += len(request.Inputs)
		for index, input := range request.Inputs {
			if index > 0 && (request.Inputs[index-1].Root > input.Root ||
				(request.Inputs[index-1].Root == input.Root && request.Inputs[index-1].Relative >= input.Relative)) {
				return fmt.Errorf("%w: snapshot inputs are not sorted and unique", ErrInvalidPlan)
			}
			locators = append(locators, catalog.SourceIndexLocator{
				RootID: string(input.Root), Relative: input.Relative,
			})
		}
	}
	if inputs > SnapshotPlanInputLimit {
		return fmt.Errorf("%w: snapshot plan page has too many inputs", ErrInvalidPlan)
	}
	entries, err := sourceSnapshotStageEntries(
		ctx, view.catalog, view.authority, view.snapshot, locators,
	)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry == nil {
			return fmt.Errorf("%w: read is outside the snapshot fence", ErrInvalidPlan)
		}
	}
	for index := range page.Reads {
		if index > 0 && page.Reads[index-1].Logical >= page.Reads[index].Logical {
			return fmt.Errorf("%w: snapshot reads are not sorted and unique", ErrInvalidPlan)
		}
	}
	payload, err := json.Marshal(page)
	if err != nil {
		return err
	}
	if len(payload) > SnapshotPlanPageByteLimit {
		return fmt.Errorf("%w: snapshot plan page exceeds its byte limit", ErrInvalidPlan)
	}
	return nil
}

func validateAffectedKeysPage(keys []causal.LogicalKey) error {
	for index, key := range keys {
		if key == "" || (index > 0 && keys[index-1] >= key) {
			return fmt.Errorf("%w: affected keys are not sorted and unique", ErrInvalidPlan)
		}
	}
	return nil
}
