package catalog

import (
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

func defaultCausalOrigin(kind MutationKind) CausalOrigin {
	switch kind {
	case MutationCreateTenant:
		return CausalOrigin{Cause: causal.CauseBootstrap}
	default:
		panic(fmt.Sprintf("catalog: mutation kind %d requires an explicit causal origin", kind))
	}
}

func validateCausalOrigin(origin CausalOrigin) error {
	switch origin.Cause {
	case causal.CauseProviderMutation, causal.CauseOnDemand:
		if origin.Domain == "" || origin.Generation == 0 {
			return fmt.Errorf("%w: domain-scoped causal origin is incomplete", ErrInvalidObject)
		}
	case causal.CauseDaemonWrite, causal.CauseExternalUnattributed, causal.CauseBootstrap:
		if origin.Domain != "" || origin.Generation != 0 {
			return fmt.Errorf("%w: non-provider causal origin carries a domain", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown causal origin %q", ErrInvalidObject, origin.Cause)
	}
	return nil
}

func validateSourceChange(change causal.ChangeSet) error {
	if change.SourceAuthority == "" || change.SourceRevision == 0 ||
		change.ChangeID == (causal.ChangeID{}) || change.OperationID == (causal.OperationID{}) ||
		len(change.AffectedKeys) == 0 || change.Cause == causal.CauseOnDemand {
		return fmt.Errorf("%w: incomplete authoritative source change", ErrInvalidObject)
	}
	if err := validateCausalOrigin(CausalOrigin{
		Cause: change.Cause, Domain: change.Origin, Generation: change.OriginGeneration,
	}); err != nil {
		return err
	}
	for index, key := range change.AffectedKeys {
		if key == "" || index > 0 && change.AffectedKeys[index-1] >= key {
			return fmt.Errorf("%w: source change keys are not sorted and unique", ErrInvalidObject)
		}
	}
	return nil
}
