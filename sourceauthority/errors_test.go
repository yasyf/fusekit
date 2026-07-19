package sourceauthority

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestIsTransientRejectsTerminalAuthorityFailures(t *testing.T) {
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		ErrClosed,
		ErrQuarantined,
		ErrInvalidPlan,
		catalog.ErrIntegrity,
		catalog.ErrTenantOwnerMismatch,
		catalog.ErrSchemaMismatch,
	} {
		if IsTransient(err) {
			t.Errorf("IsTransient(%v) = true", err)
		}
	}
	if !IsTransient(errors.New("temporary source transport failure")) {
		t.Fatal("temporary source transport failure was terminal")
	}
}
