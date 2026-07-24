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

	"github.com/yasyf/daemonkit/proc"
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
	processStore := &proc.FileStore{Path: config.Plan.Paths().ProcessStore}
	if err := processStore.Add(t.Context(), retired); err != nil {
		t.Fatalf("seed retired runtime owner: %v", err)
	}

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)
	page, err := processStore.LoadReapReceipts(
		t.Context(), recoveryid.SourceOwner, proc.ReapReceiptCursor{}, proc.ReapReceiptPageLimit,
	)
	if err != nil {
		t.Fatalf("LoadReapReceipts: %v", err)
	}
	if page.More || len(page.Receipts) != 0 {
		t.Fatalf("activation retained applied owner receipts: more %t receipts %+v", page.More, page.Receipts)
	}
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
	processStore := &proc.FileStore{Path: config.Plan.Paths().ProcessStore}
	if err := processStore.Add(t.Context(), retired); err != nil {
		t.Fatalf("seed retired runtime owner: %v", err)
	}

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)
	page, err := processStore.LoadReapReceipts(
		t.Context(), recoveryid.SourceOwner, proc.ReapReceiptCursor{}, proc.ReapReceiptPageLimit,
	)
	if err != nil {
		t.Fatalf("LoadReapReceipts: %v", err)
	}
	if page.More || len(page.Receipts) != 0 {
		t.Fatalf("activation acknowledged before every owned runtime settled: more %t receipts %+v", page.More, page.Receipts)
	}
	closeRuntime(t, runtime, done)
}

func TestHolderRegistersExactAuthenticatedOwnerBeforeCatalogAndFencesSourceEpoch(t *testing.T) {
	identity, err := proc.CurrentIdentity()
	if err != nil {
		t.Skipf("authenticated current process identity unavailable: %v", err)
	}
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "exact-holder-owner", native)
	spec := testSourceAuthoritySpec("source")
	configureTestSourceFleet(&config, spec)
	config.currentIdentity = func() (proc.Identity, error) { return identity, nil }
	config.authorityFactory = func(
		context.Context,
		sourceauthority.Config,
	) (managedAuthority, error) {
		return newTestAuthority(), nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	currentGeneration, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	expectedOwner := proc.Record{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Comm: identity.Comm,
		Executable: identity.Executable, AuditToken: identity.AuditToken,
		Generation: currentGeneration, RecoveryID: recoveryid.SourceOwner,
	}
	registeredBeforeCatalog := false
	config.catalogManager = func(
		ctx context.Context,
		managerConfig catalogworker.ManagerConfig,
	) (*catalogworker.Manager, error) {
		records, loadErr := (&proc.FileStore{Path: config.Plan.Paths().ProcessStore}).Load(ctx)
		if loadErr != nil {
			return nil, loadErr
		}
		if len(records) != 1 || !slices.Contains(records, expectedOwner) {
			return nil, fmt.Errorf(
				"runtime owners before catalog = %+v, want source owner %+v",
				records, expectedOwner,
			)
		}
		registeredBeforeCatalog = true
		return testCatalogManager(ctx, managerConfig)
	}

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatalf("New holder runtime: %v", err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)
	if !registeredBeforeCatalog {
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
	if !state.Closed || state.Process == nil || *state.Process != expectedOwner {
		t.Fatalf("closed source runtime owner = %+v, want process %+v", state, expectedOwner)
	}
	records, err := (&proc.FileStore{Path: config.Plan.Paths().ProcessStore}).Load(t.Context())
	if err != nil {
		t.Fatalf("load clean-shutdown owner store: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("clean shutdown retained holder owner records: %+v", records)
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
		proc.Record{},
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
	process proc.Record,
	epoch [16]byte,
) {
	t.Helper()
	seedSourceAuthorityOpenRuntimesForTest(t, store, []SourceAuthoritySpec{spec}, process, epoch)
}

func seedSourceAuthorityOpenRuntimesForTest(
	t *testing.T,
	store *catalog.Catalog,
	specs []SourceAuthoritySpec,
	process proc.Record,
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

func sourceAuthorityRetiredProcessForTest(generation string) proc.Record {
	return proc.Record{
		PID: 4242, StartTime: "holder-start", Boot: "retired-holder-boot",
		Comm: "holder", Generation: holderOwnerGeneration(generation), RecoveryID: recoveryid.SourceOwner,
	}
}
