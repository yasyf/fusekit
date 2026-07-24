package causal

import (
	"crypto/sha256"
	"testing"
)

func TestDeriveActivationChangeIDBindsEveryFieldAndCauseOrder(t *testing.T) {
	tenant := TenantID("acct-07")
	first := OperationID{1}
	second := OperationID{2}
	head := sha256.Sum256([]byte("catalog-head"))
	baseline, err := DeriveActivationChangeID(tenant, 3, 4, head, []OperationID{first, second})
	if err != nil {
		t.Fatal(err)
	}
	inputs := []struct {
		name         string
		tenant       TenantID
		generation   Generation
		revision     uint64
		head         [sha256.Size]byte
		publications []OperationID
	}{
		{name: "tenant", tenant: "acct-08", generation: 3, revision: 4, head: head, publications: []OperationID{first, second}},
		{name: "generation", tenant: tenant, generation: 4, revision: 4, head: head, publications: []OperationID{first, second}},
		{name: "revision", tenant: tenant, generation: 3, revision: 5, head: head, publications: []OperationID{first, second}},
		{name: "head", tenant: tenant, generation: 3, revision: 4, head: sha256.Sum256([]byte("other-head")), publications: []OperationID{first, second}},
		{name: "cause order", tenant: tenant, generation: 3, revision: 4, head: head, publications: []OperationID{second, first}},
	}
	for _, input := range inputs {
		t.Run(input.name, func(t *testing.T) {
			got, err := DeriveActivationChangeID(input.tenant, input.generation, input.revision, input.head, input.publications)
			if err != nil {
				t.Fatal(err)
			}
			if got == baseline {
				t.Fatalf("identity did not bind %s", input.name)
			}
		})
	}
	replayed, err := DeriveActivationChangeID(tenant, 3, 4, head, []OperationID{first, second})
	if err != nil || replayed != baseline {
		t.Fatalf("replay = %x, %v, want %x", replayed, err, baseline)
	}
}

func TestDeriveActivationChangeIDRejectsIncompleteInputs(t *testing.T) {
	head := sha256.Sum256([]byte("catalog-head"))
	publication := OperationID{1}
	tests := []struct {
		name         string
		tenant       TenantID
		generation   Generation
		revision     uint64
		head         [sha256.Size]byte
		publications []OperationID
	}{
		{name: "tenant", generation: 1, revision: 1, head: head, publications: []OperationID{publication}},
		{name: "generation", tenant: "acct-07", revision: 1, head: head, publications: []OperationID{publication}},
		{name: "revision", tenant: "acct-07", generation: 1, head: head, publications: []OperationID{publication}},
		{name: "head", tenant: "acct-07", generation: 1, revision: 1, publications: []OperationID{publication}},
		{name: "causes", tenant: "acct-07", generation: 1, revision: 1, head: head},
		{name: "empty cause", tenant: "acct-07", generation: 1, revision: 1, head: head, publications: []OperationID{{}}},
		{name: "duplicate cause", tenant: "acct-07", generation: 1, revision: 1, head: head, publications: []OperationID{publication, publication}},
		{name: "non-adjacent duplicate cause", tenant: "acct-07", generation: 1, revision: 1, head: head, publications: []OperationID{publication, {2}, publication}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DeriveActivationChangeID(test.tenant, test.generation, test.revision, test.head, test.publications); err == nil {
				t.Fatal("DeriveActivationChangeID succeeded")
			}
		})
	}
}

func TestValidateActivationEvent(t *testing.T) {
	event := activationEventForTest(t)
	if err := ValidateActivationEvent(event); err != nil {
		t.Fatalf("ValidateActivationEvent: %v", err)
	}

	wrongIdentity := event
	wrongIdentity.ActivationChangeID[0]++
	if err := ValidateActivationEvent(wrongIdentity); err == nil {
		t.Fatal("ValidateActivationEvent accepted a mismatched identity")
	}

	wrongOrder := event
	wrongOrder.Causes = append([]SourceCause(nil), event.Causes...)
	wrongOrder.Causes[0], wrongOrder.Causes[1] = wrongOrder.Causes[1], wrongOrder.Causes[0]
	if err := ValidateActivationEvent(wrongOrder); err == nil {
		t.Fatal("ValidateActivationEvent accepted unordered causes")
	}

	onDemand := event
	onDemand.Causes = append([]SourceCause(nil), event.Causes...)
	onDemand.Causes[0].Cause = CauseOnDemand
	if err := ValidateActivationEvent(onDemand); err == nil {
		t.Fatal("ValidateActivationEvent accepted an on-demand source publication")
	}
}

func activationEventForTest(t *testing.T) ActivationEvent {
	t.Helper()
	tenant := TenantID("acct-07")
	head := sha256.Sum256([]byte("catalog-head"))
	publications := []OperationID{{1}, {2}}
	id, err := DeriveActivationChangeID(tenant, 3, 4, head, publications)
	if err != nil {
		t.Fatal(err)
	}
	return ActivationEvent{
		ActivationChangeID: id,
		TenantID:           tenant,
		TenantGeneration:   3,
		ActivationRevision: 4,
		PresentationID:     "fp-acct-07",
		Backend:            BackendFileProvider,
		CatalogHead:        9,
		HeadDigest:         head,
		Causes: []SourceCause{
			{
				PublicationID: publications[0], ChangeID: ChangeID{1}, SourceRevision: 5,
				OperationID: OperationID{3}, Cause: CauseExternalUnattributed,
				AffectedKeysDigest: sha256.Sum256([]byte("first")),
			},
			{
				PublicationID: publications[1], ChangeID: ChangeID{2}, SourceRevision: 6,
				OperationID: OperationID{4}, Cause: CauseDaemonWrite,
				AffectedKeysDigest: sha256.Sum256([]byte("second")),
			},
		},
	}
}
