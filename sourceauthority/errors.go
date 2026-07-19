package sourceauthority

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
)

// IsTransient reports whether an authority operation may be retried without
// hiding a terminal policy, integrity, schema, or authorization failure.
func IsTransient(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
		!errors.Is(err, ErrClosed) && !errors.Is(err, ErrQuarantined) && !errors.Is(err, ErrInvalidPlan) &&
		!errors.Is(err, catalog.ErrIntegrity) && !errors.Is(err, catalog.ErrSourceObserverConflict) &&
		!errors.Is(err, catalog.ErrInvalidObject) && !errors.Is(err, catalog.ErrInvalidTransition) &&
		!errors.Is(err, catalog.ErrMutationConflict) && !errors.Is(err, catalog.ErrGenerationMismatch) &&
		!errors.Is(err, catalog.ErrTenantOwnerMismatch) && !errors.Is(err, catalog.ErrSchemaMismatch) &&
		!errors.Is(err, catalog.ErrConflict)
}
