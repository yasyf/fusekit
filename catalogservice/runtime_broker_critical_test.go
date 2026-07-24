package catalogservice

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestRuntimeBrokerSchedulesExactCriticalMaterialization(t *testing.T) {
	broker, session, readiness, publicRoot := criticalRuntimeBroker(t)

	outcome := scheduleCriticalMaterialization(t, broker, readiness)
	command := nextBrokerCommand(t, session)
	if command.Protocol != catalogproto.Version || command.CommandID == 0 ||
		command.Kind != catalogproto.BrokerCommandKindMaterializeCritical ||
		command.Registration != nil || command.ObservedID != nil || command.Notification != nil ||
		command.AfterObservedID != nil || command.CriticalReadiness == nil {
		t.Fatalf("critical materialization command = %+v", command)
	}
	if !reflect.DeepEqual(*command.CriticalReadiness, readiness) {
		t.Fatalf("critical readiness = %+v, want %+v", *command.CriticalReadiness, readiness)
	}
	if command.CriticalReadiness.ReadProofDigest != nil {
		t.Fatalf("scheduling command carried read proof %q", *command.CriticalReadiness.ReadProofDigest)
	}
	if err := catalogproto.Validate(command); err != nil {
		t.Fatalf("critical materialization command validation: %v", err)
	}

	paths := criticalMaterializationPaths(readiness, publicRoot)
	acceptCriticalMaterialization(t, session, command, paths)
	result := awaitCriticalMaterialization(t, outcome)
	if result.err != nil {
		t.Fatalf("ScheduleCriticalMaterialization: %v", result.err)
	}
	if !reflect.DeepEqual(result.paths, paths) {
		t.Fatalf("materialization paths = %+v, want %+v", result.paths, paths)
	}
}

func TestRuntimeBrokerRejectsChangedCriticalMaterializationObjectSet(t *testing.T) {
	tests := []struct {
		name  string
		paths func(catalogproto.CriticalReadinessProof, string) []catalogproto.CriticalMaterializationPath
	}{
		{
			name: "missing",
			paths: func(readiness catalogproto.CriticalReadinessProof, publicRoot string) []catalogproto.CriticalMaterializationPath {
				return criticalMaterializationPaths(readiness, publicRoot)[:1]
			},
		},
		{
			name: "extra",
			paths: func(readiness catalogproto.CriticalReadinessProof, publicRoot string) []catalogproto.CriticalMaterializationPath {
				paths := criticalMaterializationPaths(readiness, publicRoot)
				return append(paths, catalogproto.CriticalMaterializationPath{
					ObjectID: catalogproto.ObjectID(strings.Repeat("4", 32)), Path: "/File Provider/Tenant/extra.json",
				})
			},
		},
		{
			name: "wrong object id",
			paths: func(readiness catalogproto.CriticalReadinessProof, publicRoot string) []catalogproto.CriticalMaterializationPath {
				paths := criticalMaterializationPaths(readiness, publicRoot)
				paths[1].ObjectID = catalogproto.ObjectID(strings.Repeat("4", 32))
				return paths
			},
		},
		{
			name: "duplicate object id",
			paths: func(readiness catalogproto.CriticalReadinessProof, publicRoot string) []catalogproto.CriticalMaterializationPath {
				paths := criticalMaterializationPaths(readiness, publicRoot)
				paths[1].ObjectID = paths[0].ObjectID
				return paths
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker, session, readiness, publicRoot := criticalRuntimeBroker(t)
			outcome := scheduleCriticalMaterialization(t, broker, readiness)
			command := nextBrokerCommand(t, session)
			acceptCriticalMaterialization(t, session, command, test.paths(readiness, publicRoot))
			result := awaitCriticalMaterialization(t, outcome)
			if !errors.Is(result.err, catalog.ErrIntegrity) {
				t.Fatalf("ScheduleCriticalMaterialization error = %v, want catalog integrity error", result.err)
			}
			if result.paths != nil {
				t.Fatalf("rejected materialization returned paths %+v", result.paths)
			}
		})
	}
}

func TestRuntimeBrokerCriticalMaterializationSessionLossResultRace(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		broker, session, readiness, publicRoot := criticalRuntimeBroker(t)
		outcome := scheduleCriticalMaterialization(t, broker, readiness)
		command := nextBrokerCommand(t, session)
		paths := criticalMaterializationPaths(readiness, publicRoot)
		scheduled := true
		result := catalogproto.BrokerResult{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			CommandID: command.CommandID, Kind: command.Kind,
			MaterializationScheduled: &scheduled, MaterializationPaths: &paths,
		}
		start := make(chan struct{})
		accepted := make(chan error, 1)
		go func() {
			<-start
			accepted <- session.AcceptResult(t.Context(), result)
		}()
		go func() {
			<-start
			session.Close(errors.New("critical materialization result frame lost"))
		}()
		close(start)

		materialization := awaitCriticalMaterialization(t, outcome)
		acceptErr := <-accepted
		if materialization.err == nil {
			if acceptErr != nil {
				t.Fatalf("iteration %d: successful schedule had rejected result: %v", iteration, acceptErr)
			}
			if !reflect.DeepEqual(materialization.paths, paths) {
				t.Fatalf("iteration %d: paths = %+v, want %+v", iteration, materialization.paths, paths)
			}
		} else {
			if materialization.paths != nil {
				t.Fatalf("iteration %d: lost session returned paths %+v", iteration, materialization.paths)
			}
			if !errors.Is(materialization.err, errBrokerSessionLost) &&
				!errors.Is(materialization.err, errBrokerDeliveryUnknown) {
				t.Fatalf("iteration %d: race error = %v", iteration, materialization.err)
			}
		}
		closeTestRuntimeBroker(t, broker)
	}
}

func TestRuntimeBrokerRejectsCriticalSchedulingProofFenceDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*catalogproto.CriticalReadinessProof)
	}{
		{
			name: "already read proven",
			mutate: func(readiness *catalogproto.CriticalReadinessProof) {
				digest := strings.Repeat("e", 64)
				readiness.ReadProofDigest = &digest
			},
		},
		{
			name: "committed lease",
			mutate: func(readiness *catalogproto.CriticalReadinessProof) {
				readiness.Lease.State = catalogproto.FileProviderLeaseStateCommitted
				readiness.Lease.SessionID = "session"
				readiness.Lease.ProcessIdentity = "signed-app:41:test-start"
			},
		},
		{
			name: "resolution fence",
			mutate: func(readiness *catalogproto.CriticalReadinessProof) {
				readiness.Lease.ResolutionDigest = strings.Repeat("f", 64)
			},
		},
		{
			name: "generation fence",
			mutate: func(readiness *catalogproto.CriticalReadinessProof) {
				readiness.Lease.Generation++
			},
		},
		{
			name: "presentation fence",
			mutate: func(readiness *catalogproto.CriticalReadinessProof) {
				readiness.Lease.PresentationInstanceID = "other-presentation"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			broker, session, readiness, _ := criticalRuntimeBroker(t)
			test.mutate(&readiness)
			paths, err := broker.ScheduleCriticalMaterialization(t.Context(), readiness)
			if err == nil {
				t.Fatal("ScheduleCriticalMaterialization accepted a changed scheduling proof")
			}
			if paths != nil {
				t.Fatalf("rejected proof returned paths %+v", paths)
			}
			select {
			case command := <-session.Commands():
				t.Fatalf("rejected proof emitted command %+v", command)
			case <-time.After(10 * time.Millisecond):
			}
		})
	}
}

type criticalMaterializationOutcome struct {
	paths []catalogproto.CriticalMaterializationPath
	err   error
}

func criticalRuntimeBroker(
	t *testing.T,
) (*RuntimeBroker, *runtimeBrokerSession, catalogproto.CriticalReadinessProof, string) {
	t.Helper()
	store, _ := brokerTestCatalog(t)
	registered := confirmBrokerDomain(t, store)
	broker := newTestRuntimeBroker(t, store)
	t.Cleanup(func() { closeTestRuntimeBroker(t, broker) })
	sessionValue, err := broker.OpenBroker(t.Context(), brokerPeerIdentity(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*runtimeBrokerSession)
	settleRegisteredBrokerList(t, session, registered)
	return broker, session, criticalReadinessProof(registered), registered.PublicPath
}

func criticalReadinessProof(domain catalogproto.RegisteredDomain) catalogproto.CriticalReadinessProof {
	policyDigest := strings.Repeat("a", 64)
	resolutionDigest := strings.Repeat("b", 64)
	return catalogproto.CriticalReadinessProof{
		PolicyDigest: policyDigest, ResolutionDigest: resolutionDigest,
		CatalogHead: 12, SourceRevision: 8, TenantGeneration: domain.Generation,
		DomainID: domain.DomainID, PresentationInstanceID: domain.PresentationInstanceID,
		RootID: domain.RootID, ActivationGeneration: "activation-test",
		ReadChallenge: strings.Repeat("e", 64),
		Lease: catalogproto.FileProviderLeaseReceipt{
			LeaseID: "critical-lease", TenantID: domain.TenantID, DomainID: domain.DomainID,
			Generation: domain.Generation, RootID: domain.RootID,
			PresentationInstanceID: domain.PresentationInstanceID,
			State:                  catalogproto.FileProviderLeaseStateProvisional,
			PolicyDigest:           policyDigest, ResolutionDigest: resolutionDigest,
			CatalogHead: 12, SourceAuthority: "source-main",
			SourcePublication: "33333333333333333333333333333333", SourceRevision: 8,
			ActivationGeneration: "activation-test", ExpiresUnixNano: uint64(time.Now().Add(time.Minute).UnixNano()),
		},
		Objects: []catalogproto.ResolvedCriticalObjectProof{
			{
				LogicalID: "settings", Role: "settings", ObjectID: catalogproto.ObjectID(strings.Repeat("2", 32)),
				ObjectRevision: 11, ContentRevision: 10, Size: 8, Hash: strings.Repeat("c", 64),
			},
			{
				LogicalID: "theme", Role: "theme", ObjectID: catalogproto.ObjectID(strings.Repeat("3", 32)),
				ObjectRevision: 12, ContentRevision: 11, Size: 9, Hash: strings.Repeat("d", 64),
			},
		},
	}
}

func criticalMaterializationPaths(
	readiness catalogproto.CriticalReadinessProof,
	publicRoot string,
) []catalogproto.CriticalMaterializationPath {
	return []catalogproto.CriticalMaterializationPath{
		{ObjectID: readiness.Objects[0].ObjectID, Path: filepath.Join(publicRoot, "settings.json")},
		{ObjectID: readiness.Objects[1].ObjectID, Path: filepath.Join(publicRoot, "theme.json")},
	}
}

func scheduleCriticalMaterialization(
	t *testing.T,
	broker *RuntimeBroker,
	readiness catalogproto.CriticalReadinessProof,
) <-chan criticalMaterializationOutcome {
	t.Helper()
	outcome := make(chan criticalMaterializationOutcome, 1)
	go func() {
		paths, err := broker.ScheduleCriticalMaterialization(t.Context(), readiness)
		outcome <- criticalMaterializationOutcome{paths: paths, err: err}
	}()
	return outcome
}

func acceptCriticalMaterialization(
	t *testing.T,
	session *runtimeBrokerSession,
	command catalogproto.BrokerCommand,
	paths []catalogproto.CriticalMaterializationPath,
) {
	t.Helper()
	scheduled := true
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind,
		MaterializationScheduled: &scheduled, MaterializationPaths: &paths,
	}); err != nil {
		t.Fatalf("AcceptResult: %v", err)
	}
}

func awaitCriticalMaterialization(
	t *testing.T,
	outcome <-chan criticalMaterializationOutcome,
) criticalMaterializationOutcome {
	t.Helper()
	select {
	case result := <-outcome:
		return result
	case <-time.After(time.Second):
		t.Fatal("ScheduleCriticalMaterialization did not settle")
		return criticalMaterializationOutcome{}
	}
}
