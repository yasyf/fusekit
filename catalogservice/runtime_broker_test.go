package catalogservice

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
	"github.com/yasyf/fusekit/transportproto"
)

const testBrokerExecutable = "/Applications/TestBroker.app/Contents/MacOS/TestBroker"

func testRuntimeBrokerIdentity() BrokerIdentity {
	return BrokerIdentity{
		ProductBuild: "test-product-build", Executable: testBrokerExecutable,
		DesignatedRequirement:       `identifier "com.example.test-broker"`,
		EntitlementValidationDigest: [32]byte{1},
	}
}

func newTestRuntimeBroker(t *testing.T, store *catalog.Catalog) *RuntimeBroker {
	t.Helper()
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	broker.boot = func() (string, error) { return "test-cutover-boot", nil }
	broker.uptime = func() (time.Duration, error) { return 10 * time.Second, nil }
	return broker
}

func brokerPeerIdentity() Identity {
	return Identity{Build: transportproto.Build, Peer: wire.Peer{
		PID: 41, UID: 501, StartTime: "test-start", Boot: "test-boot", Comm: "TestBroker", Executable: testBrokerExecutable,
		Audit: make([]byte, 32),
	}}
}

func TestRuntimeBrokerCutoverReplaysAcrossLostSessionAndBindsFreshPeer(t *testing.T) {
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(broker.Close)
	instance := catalogproto.AccountInstanceID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	plan := catalogproto.DomainCutoverPlan{
		OperationID: "33333333333333333333333333333333", OwnerID: "owner-1",
		Accounts: []catalogproto.DomainCutoverAccount{{
			AccountID: 1, ImmutableIdentity: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			LegacyDomainID: "acct-01", AccountInstanceID: &instance,
		}},
	}
	identity := func(pid int, start string) Identity {
		return Identity{Build: transportproto.Build, Peer: wire.Peer{
			PID: pid, UID: 501, StartTime: start, Boot: "test-boot", Comm: "CCPoolStatus", Executable: testBrokerExecutable,
			Audit: make([]byte, 32),
		}}
	}
	preexistingValue, err := broker.OpenBroker(t.Context(), identity(40, "preexisting"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	preexisting := preexistingValue.(*runtimeBrokerSession)
	settleBrokerList(t, preexisting)
	proofs := make(chan struct {
		proof catalogproto.DomainAbsenceProof
		err   error
	}, 1)
	go func() {
		proof, err := broker.CutoverDomains(t.Context(), plan)
		proofs <- struct {
			proof catalogproto.DomainAbsenceProof
			err   error
		}{proof, err}
	}()
	select {
	case <-preexisting.done:
	case <-time.After(time.Second):
		t.Fatal("pre-cutover broker session was not retired")
	}
	firstValue, err := broker.OpenBroker(t.Context(), identity(41, "first"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*runtimeBrokerSession)
	settleBrokerList(t, first)
	command := nextBrokerCommand(t, first)
	if command.Kind != catalogproto.BrokerCommandKindCutoverDomains {
		t.Fatalf("cutover command = %+v", command)
	}
	first.Close(errors.New("lost response"))

	secondValue, err := broker.OpenBroker(t.Context(), identity(42, "second"), "principal")
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*runtimeBrokerSession)
	settleBrokerList(t, second)
	command = nextBrokerCommand(t, second)
	result := catalogproto.DomainCutoverResult{
		Plan: plan, FinalEnumerationRevision: 1, FinalEnumeratedAtUnixNano: time.Now().UnixNano(),
	}
	if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, CutoverResult: &result,
	}); err != nil {
		t.Fatal(err)
	}
	settled := <-proofs
	if settled.err != nil {
		t.Fatal(settled.err)
	}
	if settled.proof.BrokerPID != 42 || settled.proof.BrokerStartTime != "second" ||
		settled.proof.BrokerBoot != "test-boot" ||
		settled.proof.BrokerProductBuild != "test-product-build" ||
		settled.proof.BrokerExecutable != testBrokerExecutable ||
		settled.proof.BrokerDesignatedRequirement != `identifier "com.example.test-broker"` ||
		settled.proof.BrokerAuditTokenDigest == "" || settled.proof.BrokerEntitlementValidationDigest == "" ||
		settled.proof.Result.Plan.OperationID != plan.OperationID {
		t.Fatalf("proof = %+v", settled.proof)
	}
	recoveryKey := domainCutoverRecoveryKey(plan)
	if _, err := broker.RecoverDomainCutoverReceipt(t.Context(), recoveryKey); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("proved receipt recovery = %v, want not found", err)
	}
	broker.Close()
	restarted := newTestRuntimeBroker(t, store)
	otherInstance := catalogproto.AccountInstanceID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	retryPlan := plan
	retryPlan.OperationID = "44444444444444444444444444444444"
	retryPlan.Accounts = append([]catalogproto.DomainCutoverAccount(nil), plan.Accounts...)
	retryPlan.Accounts[0].AccountInstanceID = &otherInstance
	replayedProof, err := restarted.CutoverDomains(t.Context(), retryPlan)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(replayedProof, settled.proof) {
		t.Fatalf("replayed proved receipt = %+v, want %+v", replayedProof, settled.proof)
	}
	claim, err := restarted.ClaimDomainCutover(t.Context(), settled.proof)
	if err != nil {
		t.Fatal(err)
	}
	if claim.OperationID != plan.OperationID || claim.ProofDigest == "" || claim.ClaimedAtUnixNano <= 0 {
		t.Fatalf("claim = %+v", claim)
	}
	if _, err := restarted.ClaimDomainCutover(t.Context(), settled.proof); !errors.Is(err, catalog.ErrInvalidTransition) {
		t.Fatalf("replayed proof claim = %v", err)
	}
	restarted.Close()
	terminal := newTestRuntimeBroker(t, store)
	t.Cleanup(terminal.Close)
	receipt, err := terminal.RecoverDomainCutoverReceipt(t.Context(), recoveryKey)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(receipt.Proof, settled.proof) || receipt.Claim != claim {
		t.Fatalf("recovered receipt = %+v, want proof/claim %+v / %+v", receipt, settled.proof, claim)
	}
	recoveryKey.Accounts[0].ImmutableIdentity = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := terminal.RecoverDomainCutoverReceipt(t.Context(), recoveryKey); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("mismatched account-set recovery = %v", err)
	}
}

func TestRuntimeBrokerCutoverPersistsRandomNonceAndFencesRegistrationAcrossRestart(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	first := newTestRuntimeBroker(t, store)
	plan := catalogproto.DomainCutoverPlan{
		OperationID: "33333333333333333333333333333333", OwnerID: "owner",
		Accounts: []catalogproto.DomainCutoverAccount{{
			AccountID: 1, ImmutableIdentity: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			LegacyDomainID: "acct-01",
		}},
	}
	deadline, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := first.CutoverDomains(deadline, plan); !errors.Is(err, ErrBrokerStreamAbsent) {
		t.Fatalf("CutoverDomains without signed broker = %v", err)
	}
	first.Close()

	second := newTestRuntimeBroker(t, store)
	t.Cleanup(second.Close)
	replay := plan
	replay.OperationID = "44444444444444444444444444444444"
	proofs := make(chan catalogproto.DomainAbsenceProof, 1)
	errorsOut := make(chan error, 1)
	go func() {
		proof, err := second.CutoverDomains(t.Context(), replay)
		if err != nil {
			errorsOut <- err
			return
		}
		proofs <- proof
	}()
	sessionValue, err := second.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	if list.Kind != catalogproto.BrokerCommandKindListDomains {
		t.Fatalf("initial command = %+v", list)
	}
	actual := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	command := nextBrokerCommand(t, session)
	if command.Kind != catalogproto.BrokerCommandKindCutoverDomains || command.Cutover == nil ||
		command.Cutover.OperationID != plan.OperationID {
		t.Fatalf("durable cutover command = %+v", command)
	}
	result := catalogproto.DomainCutoverResult{
		Plan: *command.Cutover, FinalEnumerationRevision: 1, FinalEnumeratedAtUnixNano: time.Now().UnixNano(),
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, CutoverResult: &result,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case proof := <-proofs:
		if proof.Result.Plan.OperationID != plan.OperationID {
			t.Fatalf("proof operation = %q, want durable %q", proof.Result.Plan.OperationID, plan.OperationID)
		}
	case err := <-errorsOut:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("cutover replay did not settle")
	}
	select {
	case extra := <-session.commands:
		if extra.Kind == catalogproto.BrokerCommandKindRegisterDomain {
			t.Fatalf("cutover fence allowed desired domain registration for %s", provision.Tenant)
		}
	case <-time.After(25 * time.Millisecond):
	}
}

func TestRuntimeBrokerRejectsSignedPeerAtAnotherExecutablePath(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(broker.Close)
	identity := brokerPeerIdentity()
	identity.Peer.Executable = "/tmp/CCPoolStatus.app/Contents/MacOS/CCPoolStatus"
	if _, err := broker.OpenBroker(t.Context(), identity, "principal"); err == nil {
		t.Fatal("same signed identity at a non-fixed executable path was accepted")
	}
}

func settleBrokerList(t *testing.T, session *runtimeBrokerSession) {
	t.Helper()
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerReconcilesRegisterRestartAndRemove(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	if _, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal"); err == nil {
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

	restartedValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
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
	restartedValue, err = broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
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

func TestRuntimeBrokerLiveSessionRemovalWaitsForExactAbsentResult(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	initial := nextBrokerCommand(t, session)
	actual := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: initial.CommandID, Kind: initial.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}

	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	list := nextBrokerCommand(t, session)
	if list.Kind != catalogproto.BrokerCommandKindListDomains {
		t.Fatalf("removal reconcile command = %+v", list)
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before OS result: %v", err)
	default:
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before authoritative absence list: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
	state, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	if err != nil || !state.ConfirmedAbsent {
		t.Fatalf("removal state = %+v, %v", state, err)
	}
}

func TestRuntimeBrokerRemovalRecoversDisconnectAndLostResponse(t *testing.T) {
	tests := []struct {
		name       string
		firstReply bool
	}{
		{name: "disconnect before remove result", firstReply: false},
		{name: "lost successful remove response", firstReply: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, provision := brokerTestCatalog(t)
			registered := confirmBrokerDomain(t, store)
			broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(broker.Close)
			firstValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
			if err != nil {
				t.Fatal(err)
			}
			first := firstValue.(*runtimeBrokerSession)
			actual := []catalogproto.RegisteredDomain{registered}
			initial := nextBrokerCommand(t, first)
			if err := first.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: initial.CommandID, Kind: initial.Kind, Domains: &actual,
			}); err != nil {
				t.Fatal(err)
			}
			settled := make(chan error, 1)
			go func() {
				settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
			}()
			list := nextBrokerCommand(t, first)
			if err := first.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
			}); err != nil {
				t.Fatal(err)
			}
			_ = nextBrokerCommand(t, first)
			first.Close(nil)

			secondValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
			if err != nil {
				t.Fatal(err)
			}
			second := secondValue.(*runtimeBrokerSession)
			list = nextBrokerCommand(t, second)
			if test.firstReply {
				actual = []catalogproto.RegisteredDomain{}
			}
			if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
			}); err != nil {
				t.Fatal(err)
			}
			if !test.firstReply {
				remove := nextBrokerCommand(t, second)
				absent := true
				if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
					CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
				}); err != nil {
					t.Fatal(err)
				}
				list = nextBrokerCommand(t, second)
				empty := []catalogproto.RegisteredDomain{}
				if err := second.AcceptResult(t.Context(), catalogproto.BrokerResult{
					Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
					CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-settled; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeBrokerRemovalFencesRequestAndRetiresObservedIdentityDrift(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	wrong := confirmBrokerDomain(t, store)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	if err := broker.RemoveTenantDomain(t.Context(), "wrong-owner", provision.Tenant, provision.Generation); !errors.Is(err, catalog.ErrTenantOwnerMismatch) {
		t.Fatalf("wrong owner removal = %v", err)
	}
	if err := broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation+1); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("wrong generation removal = %v", err)
	}
	if _, err := store.BeginFileProviderDomainRemoval(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}

	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	waitForBrokerRemovalIntent(t, store, provision)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	wrong.Generation++
	actual := []catalogproto.RegisteredDomain{wrong}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.DomainID == nil || *remove.DomainID != wrong.DomainID {
		t.Fatalf("drifted domain removal = %+v", remove)
	}
	select {
	case err := <-settled:
		t.Fatalf("drifted domain was reported absent while still listed: %v", err)
	default:
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerRemovalWaitsForEveryMatchingStrayDomain(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	expected := confirmBrokerDomain(t, store)
	stray := expected
	stray.DomainID = distinctBrokerDomainID(expected.DomainID)
	stray.Generation++

	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	waitForBrokerRemovalIntent(t, store, provision)
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	actual := []catalogproto.RegisteredDomain{stray}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.DomainID == nil || *remove.DomainID != stray.DomainID {
		t.Fatalf("stray-domain removal = %+v", remove)
	}
	absent := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged while expected domain remains: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	actual = []catalogproto.RegisteredDomain{expected}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	remove = nextBrokerCommand(t, session)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.DomainID == nil || *remove.DomainID != expected.DomainID {
		t.Fatalf("expected-domain removal = %+v", remove)
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: remove.CommandID, Kind: remove.Kind, ConfirmedAbsent: &absent,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-settled:
		t.Fatalf("removal acknowledged before authoritative fleet absence: %v", err)
	default:
	}
	list = nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerAlreadyAbsentRemovalNeedsNoSessionRestart(t *testing.T) {
	store, provision := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broker.Close)
	if _, err := store.BeginFileProviderDomainRemoval(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}
	settled := make(chan error, 1)
	go func() {
		settled <- broker.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, session)
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &empty,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBrokerRemovalIntentRecoversAcrossRuntimeRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	provision := catalog.TenantProvision{
		OwnerID: "owner", Tenant: "restart-tenant", PresentationRoot: filepath.Join(t.TempDir(), "presentation"),
		BackingRoot: filepath.Join(t.TempDir(), "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentFileProvider,
		FileProvider:  catalog.FileProviderPresentation{AccountInstanceID: "restart-instance", DisplayName: "Restart"}, Generation: 9,
	}
	provision, err = store.ProvisionTenant(t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	registered := confirmBrokerDomain(t, store)
	registered.DomainID = distinctBrokerDomainID(registered.DomainID)
	registered.Generation++
	first, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	removeContext, cancel := context.WithCancel(t.Context())
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- first.RemoveTenantDomain(removeContext, provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("durable removal intent was not written")
		}
		time.Sleep(time.Millisecond)
	}
	firstSessionValue, err := first.OpenBroker(removeContext, brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	firstSession := firstSessionValue.(*runtimeBrokerSession)
	list := nextBrokerCommand(t, firstSession)
	actual := []catalogproto.RegisteredDomain{registered}
	if err := firstSession.AcceptResult(removeContext, catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	remove := nextBrokerCommand(t, firstSession)
	if remove.Kind != catalogproto.BrokerCommandKindRemoveDomain || remove.DomainID == nil || *remove.DomainID != registered.DomainID {
		t.Fatalf("stray removal before restart = %+v", remove)
	}
	firstSession.Close(nil)
	cancel()
	if err := <-firstResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("first removal = %v", err)
	}
	first.Close()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = catalog.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	second, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)
	settled := make(chan error, 1)
	go func() {
		settled <- second.RemoveTenantDomain(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	}()
	sessionValue, err := second.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	list = nextBrokerCommand(t, session)
	actual = []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: list.CommandID, Kind: list.Kind, Domains: &actual,
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-settled; err != nil {
		t.Fatal(err)
	}
}

func distinctBrokerDomainID(id catalogproto.DomainID) catalogproto.DomainID {
	value := []byte(id)
	if value[len(value)-1] == '0' {
		value[len(value)-1] = '1'
	} else {
		value[len(value)-1] = '0'
	}
	return catalogproto.DomainID(value)
}

func waitForBrokerRemovalIntent(t *testing.T, store *catalog.Catalog, provision catalog.TenantProvision) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := store.FileProviderDomainRemovalState(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("durable removal intent was not written")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknownAndBoundsQueue(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	domains, err := store.FileProviderDomains(t.Context())
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	registration := protocolDomainRegistration(domains[0])
	for index := 1; index < brokerCommandBuffer; index++ {
		if err := broker.enqueue(t.Context(), session, catalogproto.BrokerCommand{
			Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
		}, nil); err != nil {
			t.Fatalf("fill command %d: %v", index, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := broker.enqueue(ctx, session, catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
	}, nil); err == nil {
		t.Fatal("broker queue exceeded its fixed capacity")
	}
	session.Close(nil)
	broker.Close()
}

func TestRuntimeBrokerSessionLossMakesSentNotificationUnknown(t *testing.T) {
	store, _ := brokerTestCatalog(t)
	broker, err := NewRuntimeBroker(store, testRuntimeBrokerIdentity())
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
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

func confirmBrokerDomain(t *testing.T, store *catalog.Catalog) catalogproto.RegisteredDomain {
	t.Helper()
	domains, err := store.FileProviderDomains(t.Context())
	if err != nil || len(domains) != 1 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
	domain := domains[0]
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.Registered = true
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	return protocolRegisteredDomain(domain)
}

func protocolRegisteredDomain(domain catalog.FileProviderDomain) catalogproto.RegisteredDomain {
	return catalogproto.RegisteredDomain{
		DomainID: catalogproto.DomainID(domain.DomainID), OwnerID: catalogproto.OwnerID(domain.OwnerID),
		TenantID: catalogproto.TenantID(domain.Tenant), Generation: uint64(domain.Generation),
		RootID: catalogproto.ObjectID(domain.Root.String()), AccountInstanceID: catalogproto.AccountInstanceID(domain.AccountInstance),
		DisplayName: domain.DisplayName, PublicPath: domain.PublicPath,
	}
}
