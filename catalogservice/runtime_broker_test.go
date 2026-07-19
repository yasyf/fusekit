package catalogservice

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

func TestRuntimeBrokerReconcilesRegisterRestartAndRemove(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store)
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), Identity{}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	if _, err := broker.OpenBroker(t.Context(), Identity{}, "principal"); err == nil {
		t.Fatal("duplicate broker session was accepted")
	}
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	register := nextBrokerCommand(t, session)
	if register.Kind != catalogproto.BrokerCommandKindRegisterDomain || register.Registration == nil {
		t.Fatalf("register command = %+v", register)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: register.Registration.DomainID, OwnerID: register.Registration.OwnerID,
		TenantID: register.Registration.TenantID, Generation: register.Registration.Generation,
		RootID: register.Registration.RootID, AccountInstanceID: register.Registration.AccountInstanceID,
		DisplayName: register.Registration.DisplayName, PublicPath: filepath.Join(t.TempDir(), "Domain"),
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: register.CommandID, Kind: register.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	domains := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &domains,
	}); err != nil {
		t.Fatal(err)
	}
	actual, err := store.FileProviderDomains(t.Context())
	if err != nil || len(actual) != 1 || !actual[0].Registered {
		t.Fatalf("registered domains = %+v, %v", actual, err)
	}
	session.Close(nil)

	restartedValue, err := broker.OpenBroker(t.Context(), Identity{}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	restarted := restartedValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, restarted)
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &domains,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveTenantProvision(t.Context(), provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}
	restarted.Close(nil)
	restartedValue, err = broker.OpenBroker(t.Context(), Identity{}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	restarted = restartedValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, restarted)
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &domains,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, restarted)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.DomainID == nil || *remove.DomainID != registered.DomainID {
		t.Fatalf("remove command = %+v", remove)
	}
	absent := true
	if err := restarted.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	restarted.Close(nil)
	broker.Close()
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknownAndBoundsQueue(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store)
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), Identity{}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	for index := 1; index < brokerCommandBuffer; index++ {
		if err := broker.enqueue(t.Context(), session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil); err != nil {
			t.Fatalf("fill command %d: %v", index, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := broker.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil); err == nil {
		t.Fatal("broker queue exceeded its fixed capacity")
	}
	session.Close(nil)
	broker.Close()
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknown(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store)
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), Identity{}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	_ = nextBrokerCommand(t, session)
	var change causal.ChangeID
	var operation causal.OperationID
	change[0] = 1
	operation[0] = 2
	domains, err := store.FileProviderDomains(t.Context())
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	outcome := make(chan convergence.Delivery, 1)
	go func() {
		delivery, _ := broker.Notify(t.Context(), convergence.Notification{
			SourceAuthority: "source", SourceRevision: 1, CatalogRevision: 1,
			ChangeID: change, OperationID: operation, Cause: causal.CauseDaemonWrite,
			AffectedKeys: []causal.LogicalKey{"object"}, Tenant: "tenant",
			Domain: domains[0].DomainID, Generation: 1, Revision: 1,
		})
		outcome <- delivery
	}()
	command := nextBrokerCommand(t, session)
	if command.Kind != catalogproto.BrokerCommandKindSignalDomain {
		t.Fatalf("signal command = %+v", command)
	}
	session.Close(nil)
	select {
	case delivery := <-outcome:
		if delivery != convergence.DeliveryUnknown {
			t.Fatalf("delivery = %v", delivery)
		}
	case <-time.After(time.Second):
		t.Fatal("Notify did not settle after session loss")
	}
	broker.Close()
}

func nextBrokerCommand(t *testing.T, session *runtimeBrokerSession) catalogproto.BrokerCommand {
	t.Helper()
	select {
	case command := <-session.Commands():
		return command
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for broker command")
		return catalogproto.BrokerCommand{}
	}
}

func brokerTestCatalog(t *testing.T) (*catalog.Catalog, catalog.TenantProvision) {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	provision := catalog.TenantProvision{
		OwnerID: "owner", Tenant: "tenant", PresentationRoot: filepath.Join(t.TempDir(), "presentation"),
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentFileProvider,
		FileProvider:  catalog.FileProviderPresentation{AccountInstanceID: "instance", DisplayName: "Tenant"}, Generation: 1,
	}
	provision, err = store.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	return store, provision
}
