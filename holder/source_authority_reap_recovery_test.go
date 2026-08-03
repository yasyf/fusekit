package holder

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/sourceauthority"
)

func TestHolderActivationConsumesOnlyTheExactRecoveredRuntimeOwnerReceipt(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "reap-recovery", native)
	spec := testSourceAuthoritySpec("source")
	configureTestSourceFleet(&config, spec)
	config.authorityFactory = func(
		context.Context,
		sourceauthority.Config,
	) (managedAuthority, error) {
		return newTestAuthority(), nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	retired := sourceAuthorityRetiredProcessForTest("retired-holder")
	database, err := catalog.Open(t.Context(), config.Plan.Paths().Catalog)
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	seedSourceAuthorityOpenRuntimeForTest(t, database, spec, retired, [16]byte{1})
	if err := database.Close(); err != nil {
		t.Fatalf("Close seeded catalog: %v", err)
	}
	reapRecoverySeedOwnerRecord(t, config.Plan.Paths().ProcessStore, retired)

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	reapRecoveryRequireSettledOwner(t, config.Plan.Paths().ProcessStore, retired)
	closeRuntime(t, runtime, done)
}

func TestHolderActivationRecoversEveryAuthorityOwnedByOneReapedProcessBeforeAcknowledgement(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "multi-authority-reap-recovery", native)
	specs := []SourceAuthoritySpec{
		testSourceAuthoritySpec("source-a"),
		testSourceAuthoritySpec("source-b"),
	}
	configureTestSourceFleet(&config, specs...)
	config.authorityFactory = func(
		context.Context,
		sourceauthority.Config,
	) (managedAuthority, error) {
		return newTestAuthority(), nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	retired := sourceAuthorityRetiredProcessForTest("multi-authority-retired")
	database, err := catalog.Open(t.Context(), config.Plan.Paths().Catalog)
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	seedSourceAuthorityOpenRuntimesForTest(t, database, specs, retired, [16]byte{1})
	if err := database.Close(); err != nil {
		t.Fatalf("Close seeded catalog: %v", err)
	}
	reapRecoverySeedOwnerRecord(t, config.Plan.Paths().ProcessStore, retired)

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	reapRecoveryRequireSettledOwner(t, config.Plan.Paths().ProcessStore, retired)
	closeRuntime(t, runtime, done)
}

func TestHolderRegistersExactOwnerBeforeCatalogAndFencesSourceEpoch(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "exact-holder-owner", native)
	spec := testSourceAuthoritySpec("source")
	configureTestSourceFleet(&config, spec)
	config.authorityFactory = func(
		context.Context,
		sourceauthority.Config,
	) (managedAuthority, error) {
		return newTestAuthority(), nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	var registeredOwner catalog.ProcessRecord
	config.catalogManager = func(
		ctx context.Context,
		managerConfig catalogworker.ManagerConfig,
	) (*catalogworker.Manager, error) {
		ledger, openErr := openProcessLedger(config.Plan.Paths().ProcessStore)
		if openErr != nil {
			return nil, openErr
		}
		records := ledger.state.Records
		if len(records) != 1 {
			return nil, fmt.Errorf("runtime owners before catalog = %+v, want exactly one", records)
		}
		expectedOwner, captureErr := captureCurrentProcessRecord(
			recoveryid.SourceOwner, records[0].Generation,
		)
		if captureErr != nil {
			return nil, captureErr
		}
		if records[0] != expectedOwner {
			return nil, fmt.Errorf(
				"runtime owner before catalog = %+v, want source owner %+v",
				records[0], expectedOwner,
			)
		}
		registeredOwner = expectedOwner
		return testCatalogManager(ctx, managerConfig)
	}

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if registeredOwner == (catalog.ProcessRecord{}) {
		t.Fatal("catalog opened before exact holder owner registration")
	}
	closeRuntime(t, runtime, done)

	store, err := catalog.Open(t.Context(), config.Plan.Paths().Catalog)
	if err != nil {
		t.Fatalf("reopen catalog: %v", err)
	}
	defer func() { _ = store.Close() }()
	state, err := store.SourceAuthorityRuntimeStatus(t.Context(), catalog.SourceAuthorityRuntimeRef{
		Owner: "holder-test", Generation: 1, Authority: spec.Authority,
	})
	if err != nil {
		t.Fatalf("SourceAuthorityRuntimeStatus: %v", err)
	}
	if !state.Closed || state.Process == nil || *state.Process != registeredOwner {
		t.Fatalf("closed source runtime owner = %+v, want process %+v", state, registeredOwner)
	}
	ledger, err := openProcessLedger(config.Plan.Paths().ProcessStore)
	if err != nil {
		t.Fatalf("load clean-shutdown owner ledger: %v", err)
	}
	if len(ledger.state.Records) != 0 {
		t.Fatalf("clean shutdown retained holder owner records: %+v", ledger.state.Records)
	}
}

func TestSourceAuthorityRuntimeOwnerRequiresCompleteHolderProcessRecord(t *testing.T) {
	store := openHolderReapRecoveryCatalog(t)
	spec := testSourceAuthoritySpec("source")
	if _, err := newAuthorityRegistry(
		store,
		testSourceAuthorityFleet(spec),
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			return newTestAuthority(), nil
		},
		func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
			return testAuthorityExecutor{}, nil
		},
		nil,
		catalog.ProcessRecord{},
		time.Second,
	); err == nil {
		t.Fatal("source authority registry accepted a zero holder process record")
	}
}

func TestAuthorityRegistryRejectsDeclarationDriftBeforeExecutorIO(t *testing.T) {
	store := openHolderReapRecoveryCatalog(t)
	stored := testSourceAuthoritySpec("source")
	process := sourceAuthorityRetiredProcessForTest("holder-current")
	seedSourceAuthorityOpenRuntimeForTest(t, store, stored, process, [16]byte{1})

	changed := stored
	changed.DeclarationDigest = sha256.Sum256([]byte("changed declaration"))
	var executorCalls, runtimeCalls int
	registry, err := newAuthorityRegistry(
		store,
		testSourceAuthorityFleet(changed),
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			runtimeCalls++
			return newTestAuthority(), nil
		},
		func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
			executorCalls++
			return testAuthorityExecutor{}, nil
		},
		nil,
		process,
		time.Second,
	)
	if err != nil {
		t.Fatalf("newAuthorityRegistry: %v", err)
	}
	if err := registry.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("start with changed declaration = %v, want mutation conflict", err)
	}
	if executorCalls != 0 || runtimeCalls != 0 {
		t.Fatalf("declaration drift reached source I/O: executor=%d runtime=%d", executorCalls, runtimeCalls)
	}
}

func TestAuthorityRegistryRejectsOpenPriorRuntimeAfterGlobalReceiptRecovery(t *testing.T) {
	store := openHolderReapRecoveryCatalog(t)
	spec := testSourceAuthoritySpec("source")
	prior := sourceAuthorityRetiredProcessForTest("prior-holder")
	epoch := [16]byte{1}
	seedSourceAuthorityOpenRuntimeForTest(t, store, spec, prior, epoch)

	var executorCalls, runtimeCalls int
	registry, err := newAuthorityRegistry(
		store,
		testSourceAuthorityFleet(spec),
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			runtimeCalls++
			return newTestAuthority(), nil
		},
		func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
			executorCalls++
			return testAuthorityExecutor{}, nil
		},
		nil,
		sourceAuthorityRetiredProcessForTest("successor-holder"),
		time.Second,
	)
	if err != nil {
		t.Fatalf("newAuthorityRegistry: %v", err)
	}
	if err := registry.start(t.Context(), nil); !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("start with open prior runtime = %v, want mutation conflict", err)
	}
	if executorCalls != 0 || runtimeCalls != 0 {
		t.Fatalf("open prior runtime reached source I/O: executor=%d runtime=%d", executorCalls, runtimeCalls)
	}
	state, err := store.SourceAuthorityRuntimeStatus(t.Context(), catalog.SourceAuthorityRuntimeRef{
		Owner: "holder-test", Generation: 1, Authority: spec.Authority,
	})
	if err != nil {
		t.Fatalf("SourceAuthorityRuntimeStatus: %v", err)
	}
	if state.Closed || state.Epoch != epoch || state.Process == nil || *state.Process != prior {
		t.Fatalf("rejected prior runtime state changed: %+v", state)
	}
}

func reapRecoverySeedOwnerRecord(t *testing.T, path string, record catalog.ProcessRecord) {
	t.Helper()
	ledger, err := openProcessLedger(path)
	if err != nil {
		t.Fatalf("open seed process ledger: %v", err)
	}
	if err := ledger.Track(record); err != nil {
		t.Fatalf("seed retired runtime owner: %v", err)
	}
}

func reapRecoveryRequireSettledOwner(t *testing.T, path string, record catalog.ProcessRecord) {
	t.Helper()
	ledger, err := openProcessLedger(path)
	if err != nil {
		t.Fatalf("reopen process ledger: %v", err)
	}
	if pending := ledger.Receipts(recoveryid.SourceOwner, 0); len(pending) != 0 {
		t.Fatalf("activation retained applied owner receipts: %+v", pending)
	}
	if err := requireNoReceiptLiabilities(t.Context(), ledger); err != nil {
		t.Fatalf("activation acknowledged before every owned runtime settled: %v", err)
	}
	if slices.Contains(ledger.state.Records, record) {
		t.Fatalf("activation retained the reaped owner record: %+v", ledger.state.Records)
	}
}

func openHolderReapRecoveryCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	store, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("Open catalog: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func seedSourceAuthorityOpenRuntimeForTest(
	t *testing.T,
	store *catalog.Catalog,
	spec SourceAuthoritySpec,
	process catalog.ProcessRecord,
	epoch [16]byte,
) {
	t.Helper()
	seedSourceAuthorityOpenRuntimesForTest(t, store, []SourceAuthoritySpec{spec}, process, epoch)
}

func seedSourceAuthorityOpenRuntimesForTest(
	t *testing.T,
	store *catalog.Catalog,
	specs []SourceAuthoritySpec,
	process catalog.ProcessRecord,
	epoch [16]byte,
) {
	t.Helper()
	declarations := make([]catalog.SourceAuthorityDeclaration, len(specs))
	authorities := make([]causal.SourceAuthorityID, len(specs))
	for index, spec := range specs {
		authority, digest := sourceAuthorityIdentity(spec)
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: authority, DriverID: sourceAuthorityDriverID(spec),
			DriverConfig:      append([]byte(nil), sourceAuthorityDriverConfig(spec)...),
			DeclarationDigest: digest,
		}
		authorities[index] = authority
	}
	authorityDigest, err := catalog.SourceAuthorityFleetDigest(
		authorities,
	)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDigest: %v", err)
	}
	declarationDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatalf("SourceAuthorityFleetDeclarationsDigest: %v", err)
	}
	stage, err := store.ReconcileSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetReconcileRequest{
			Owner: "holder-test", Generation: 1, Declarations: declarations,
			Complete: true, AuthorityCount: uint64(len(declarations)),
			AuthoritiesDigest: authorityDigest, DeclarationsDigest: declarationDigest,
		},
	)
	if err != nil {
		t.Fatalf("ReconcileSourceAuthorityFleet: %v", err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner: "holder-test", Generation: 1, AuthorityCount: uint64(len(declarations)),
			AuthoritiesDigest: authorityDigest, DeclarationsDigest: declarationDigest,
			StageDigest: stage.StageDigest,
		},
	); err != nil {
		t.Fatalf("AcknowledgeSourceAuthorityFleet: %v", err)
	}
	for _, spec := range specs {
		authority, _ := sourceAuthorityIdentity(spec)
		if err := store.TakeoverSourceAuthorityRuntime(
			t.Context(),
			catalog.SourceAuthorityRuntimeTakeover{
				Ref: catalog.SourceAuthorityRuntimeRef{
					Owner: "holder-test", Generation: 1, Authority: authority,
				},
				Epoch: epoch, Process: process,
			},
		); err != nil {
			t.Fatalf("TakeoverSourceAuthorityRuntime: %v", err)
		}
	}
}

func sourceAuthorityRetiredProcessForTest(generation string) catalog.ProcessRecord {
	return catalog.ProcessRecord{
		PID: 4242, StartTime: "holder-start", Boot: "retired-holder-boot",
		Comm: "holder", Generation: holderOwnerGeneration(generation), RecoveryID: recoveryid.SourceOwner,
	}
}
