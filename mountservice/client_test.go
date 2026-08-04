package mountservice

import (
	"context"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

// TestClientCallBudgetsADeadlinelessCaller proves a verb dispatched on a
// context carrying no deadline still reaches the server: daemonkit's Call
// refuses one, and the native child reaches these verbs on its lifetime
// context — a signal-scoped gate that carries none.
func TestClientCallBudgetsADeadlinelessCaller(t *testing.T) {
	runtime := &fakeRuntime{}
	d := startMountServer(t, runtime, &recordingAuthorizer{owner: "trusted-owner"})
	client := newMountClient(t, d)
	id, err := catalog.NewTenantID("acct-91")
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	provisioned, err := client.ProvisionTenant(context.Background(), id, testDefinition(1))
	if err != nil {
		t.Fatalf("ProvisionTenant with a deadline-less caller: %v", err)
	}
	if provisioned.TenantID != "acct-91" || provisioned.Generation != 1 {
		t.Fatalf("ProvisionTenant response = %#v", provisioned)
	}
	if snapshot := runtime.snapshot(); snapshot.spec.ID != id || snapshot.spec.Generation != 1 {
		t.Fatalf("provisioned spec = %#v", snapshot.spec)
	}
}
