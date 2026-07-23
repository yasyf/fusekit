package fusekit

import (
	"testing"

	"github.com/yasyf/fusekit/tenant"
)

func TestRootFileProviderSpecIsExactTenantAlias(t *testing.T) {
	want := tenant.FileProviderSpec{
		Enabled: true, PresentationInstanceID: "account-instance-1", DisplayName: "Account One",
	}
	var exported = want
	var roundTrip = exported
	if roundTrip != want {
		t.Fatalf("FileProviderSpec round trip = %+v, want %+v", roundTrip, want)
	}
}
