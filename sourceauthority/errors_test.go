package sourceauthority

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit"
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

func TestSourceTaskCallErrorClaimsNonDeliveryOnlyWhenProven(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{
			"canceled",
			context.Canceled,
			"sourceauthority: source task delivery is unknown: context canceled",
		},
		{
			"deadline",
			context.DeadlineExceeded,
			"sourceauthority: source task delivery is unknown: context deadline exceeded",
		},
		{
			"product failure",
			errors.New("source task child refused"),
			"sourceauthority: source task delivery is unknown: source task child refused",
		},
		{
			"lane closed",
			daemonkit.ErrLaneClosed,
			"sourceauthority: source task delivery is unknown: " + daemonkit.ErrLaneClosed.Error(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			classified := classifySourceTaskCallError(test.err)
			if got := classified.Error(); got != test.want {
				t.Fatalf("classifySourceTaskCallError() = %q, want %q", got, test.want)
			}
			if daemonkit.Undispatched(test.err) {
				t.Fatalf("%v was undispatched, so the case proves nothing", test.err)
			}
		})
	}
	if !errors.Is(classifySourceTaskCallError(context.Canceled), context.Canceled) {
		t.Fatal("classification lost the cancellation identity")
	}
	if !errors.Is(classifySourceTaskCallError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("classification lost the deadline identity")
	}
}
