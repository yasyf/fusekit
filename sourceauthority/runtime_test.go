package sourceauthority

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/tenant"
)

const testAuthority causal.SourceAuthorityID = "sourceauthority-test"

func sourceAuthorityOwnerGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

type fakePathSource struct {
	mu            sync.Mutex
	entries       map[indexKey]PhysicalEntry
	bodies        map[indexKey][]byte
	scanCalls     int
	statCalls     int
	maxScanLimit  int
	maxReadBuffer int
	closedReaders int
	closedSources int
}

func newFakePathSource(count int) *fakePathSource {
	source := &fakePathSource{entries: make(map[indexKey]PhysicalEntry), bodies: make(map[indexKey][]byte)}
	for index := range count {
		name := fmt.Sprintf("file-%04d.json", index)
		body := []byte(fmt.Sprintf("value-%d", index))
		if index == 0 {
			body = bytes.Repeat([]byte("x"), 2<<20)
		}
		source.put("root", name, uint64(index+10), body)
	}
	return source
}

func (s *fakePathSource) put(root RootID, relative string, inode uint64, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := indexKey{root: root, relative: relative}
	fingerprint := sha256.Sum256(body)
	s.entries[key] = PhysicalEntry{
		Root: root, Relative: relative, Exists: true, Kind: PhysicalFile,
		Identity: FileIdentity{VolumeUUID: "volume", Inode: inode, BirthtimeSec: 100, BirthtimeNsec: int64(inode)},
		Mode:     0o600, Size: int64(len(body)), MetadataFingerprint: fingerprint, ContentFingerprint: fingerprint,
	}
	s.bodies[key] = append([]byte(nil), body...)
}

func (s *fakePathSource) RootIdentity(context.Context, RootSpec) (FileIdentity, error) {
	return FileIdentity{VolumeUUID: "volume", Inode: 1, BirthtimeSec: 1, BirthtimeNsec: 1}, nil
}

func (s *fakePathSource) Stat(_ context.Context, root RootSpec, relative string) (PhysicalEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statCalls++
	entry, found := s.entries[indexKey{root: root.ID, relative: relative}]
	if !found {
		return PhysicalEntry{Root: root.ID, Relative: relative}, nil
	}
	return entry, nil
}

func (s *fakePathSource) Scan(_ context.Context, _ []RootSpec, cursor ScanCursor, limit int) (ScanPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scanCalls++
	s.maxScanLimit = max(s.maxScanLimit, limit)
	start := 0
	if cursor != "" {
		value, err := strconv.Atoi(string(cursor))
		if err != nil {
			return ScanPage{}, err
		}
		start = value
	}
	entries := make([]PhysicalEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(left, right PhysicalEntry) int {
		if left.Root != right.Root {
			return compareString(string(left.Root), string(right.Root))
		}
		return compareString(left.Relative, right.Relative)
	})
	end := min(start+limit, len(entries))
	page := ScanPage{Entries: append([]PhysicalEntry(nil), entries[start:end]...)}
	if end < len(entries) {
		page.Next = ScanCursor(strconv.Itoa(end))
	}
	return page, nil
}

func (s *fakePathSource) BeginScan(context.Context, []RootSpec) (ScanSession, error) {
	return &fakeScanSession{source: s}, nil
}

type fakeScanSession struct {
	source *fakePathSource
	cursor ScanCursor
	closed bool
}

func (s *fakeScanSession) Next(ctx context.Context, limit int) (ScanPage, error) {
	if s.closed {
		return ScanPage{}, ErrClosed
	}
	page, err := s.source.Scan(ctx, nil, s.cursor, limit)
	if err == nil {
		s.cursor = page.Next
	}
	return page, err
}

func (s *fakeScanSession) Close() error {
	s.closed = true
	return nil
}

type measuredContent struct {
	source *fakePathSource
	body   []byte
}

type countedMutationContent struct {
	mu     sync.Mutex
	closes int
}

type repairTrackingExecutor struct {
	*fakeExecutor
	failCleanup        bool
	acks               []catalog.MutationID
	ackGenerations     []causal.Generation
	abandons           []catalog.MutationID
	abandonGenerations []causal.Generation
}

type expectationPageStore struct {
	Store
	record catalog.SourceMutationExpectationRecord
}

func (s expectationPageStore) SourceMutationExpectationsPage(
	_ context.Context,
	_ causal.SourceAuthorityID,
	after catalog.MutationID,
	_ int,
) (catalog.SourceMutationExpectationPage, error) {
	if after != (catalog.MutationID{}) {
		return catalog.SourceMutationExpectationPage{}, nil
	}
	return catalog.SourceMutationExpectationPage{
		Records: []catalog.SourceMutationExpectationRecord{s.record},
	}, nil
}

func (e *repairTrackingExecutor) AcknowledgeMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	generation causal.Generation,
	operation catalog.MutationID,
	_ Fingerprint,
) error {
	e.acks = append(e.acks, operation)
	e.ackGenerations = append(e.ackGenerations, generation)
	if e.failCleanup {
		e.failCleanup = false
		return errors.New("simulated cleanup crash")
	}
	return nil
}

func (e *repairTrackingExecutor) AbandonMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	generation causal.Generation,
	operation catalog.MutationID,
) error {
	e.abandons = append(e.abandons, operation)
	e.abandonGenerations = append(e.abandonGenerations, generation)
	if e.failCleanup {
		e.failCleanup = false
		return errors.New("simulated cleanup crash")
	}
	return nil
}

func (*countedMutationContent) Open(context.Context) (contentstream.Source, error) {
	return ownedTestContent(io.NopCloser(bytes.NewReader(nil))), nil
}

func (c *countedMutationContent) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *countedMutationContent) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func (c measuredContent) Open(context.Context) (contentstream.Source, error) {
	return ownedTestContent(&measuredReader{source: c.source, reader: bytes.NewReader(c.body)}), nil
}

func (c measuredContent) Close() error {
	c.source.mu.Lock()
	c.source.closedSources++
	c.source.mu.Unlock()
	return nil
}

type measuredReader struct {
	source *fakePathSource
	reader *bytes.Reader
}

type closeErrorContent struct {
	body []byte
	err  error
}

func (c closeErrorContent) Open(context.Context) (contentstream.Source, error) {
	return ownedTestContent(closeErrorReader{Reader: bytes.NewReader(c.body), err: c.err}), nil
}

func (closeErrorContent) Close() error { return nil }

type closeErrorReader struct {
	*bytes.Reader
	err error
}

func (r closeErrorReader) Close() error { return r.err }

func (r *measuredReader) Read(buffer []byte) (int, error) {
	r.source.mu.Lock()
	r.source.maxReadBuffer = max(r.source.maxReadBuffer, len(buffer))
	r.source.mu.Unlock()
	return r.reader.Read(buffer)
}

func (r *measuredReader) Close() error {
	r.source.mu.Lock()
	r.source.closedReaders++
	r.source.mu.Unlock()
	return nil
}

type fakePolicy struct {
	source           *fakePathSource
	failSnapshot     int
	tenants          []tenant.TenantSpec
	rootsCalls       int
	deltaStarted     chan struct{}
	deltaRelease     chan struct{}
	deltaStartedOnce sync.Once
	mutationPlan     func(MutationRequest) (MutationPlan, error)
	snapshotPlan     func(context.Context, SnapshotView, SnapshotPlanCursor, int) (SnapshotPlanPage, error)
	deltaCalls       int
	failDeltaAt      int
}

func (p *fakePolicy) Roots(context.Context, []tenant.TenantSpec) ([]RootSpec, error) {
	p.rootsCalls++
	return []RootSpec{{Authority: testAuthority, ID: "root", Path: "/source/root", Kind: RootDirectory, Generation: 1}}, nil
}

func (p *fakePolicy) PlanSnapshot(
	ctx context.Context,
	view SnapshotView,
	cursor SnapshotPlanCursor,
	limit int,
) (SnapshotPlanPage, error) {
	if p.snapshotPlan != nil {
		return p.snapshotPlan(ctx, view, cursor, limit)
	}
	if p.failSnapshot > 0 {
		p.failSnapshot--
		return SnapshotPlanPage{}, ErrSourceChanged
	}
	if cursor != "" || limit <= 0 {
		return SnapshotPlanPage{}, ErrInvalidPlan
	}
	plan := SnapshotPlanPage{Fence: view.Fence(), AffectedKeys: []causal.LogicalKey{"settings"}}
	for _, spec := range view.Tenants() {
		plan.Roots = append(plan.Roots, TenantRoot{Tenant: spec.ID, Generation: spec.Generation, Logical: LogicalID("root:" + spec.ID)})
	}
	page, err := view.Scan(ctx, "", min(limit, 73))
	if err != nil {
		return SnapshotPlanPage{}, err
	}
	for _, entry := range page.Entries {
		if len(plan.Reads) == 3 {
			break
		}
		plan.Reads = append(plan.Reads, MaterializationRequest{
			Logical: LogicalID("file:" + entry.Relative), Inputs: []PathRef{{Root: entry.Root, Relative: entry.Relative}},
		})
	}
	return plan, nil
}

func (p *fakePolicy) PlanDelta(_ context.Context, view IndexView, batch EventBatch) (DeltaPlan, error) {
	p.deltaCalls++
	if p.failDeltaAt != 0 && p.deltaCalls == p.failDeltaAt {
		return DeltaPlan{}, ErrSourceChanged
	}
	if p.deltaStarted != nil {
		p.deltaStartedOnce.Do(func() { close(p.deltaStarted) })
		<-p.deltaRelease
	}
	plan := DeltaPlan{Fence: view.Fence(), AffectedKeys: []causal.LogicalKey{"settings"}}
	for _, spec := range view.Tenants() {
		plan.Roots = append(plan.Roots, TenantRoot{Tenant: spec.ID, Generation: spec.Generation, Logical: LogicalID("root:" + spec.ID)})
	}
	seen := make(map[string]struct{})
	for _, event := range batch.Events {
		if _, duplicate := seen[event.Relative]; duplicate {
			continue
		}
		seen[event.Relative] = struct{}{}
		if entry, found := view.Entry(event.Root, event.Relative); found && entry.Physical.Exists {
			plan.Reads = append(plan.Reads, MaterializationRequest{
				Logical: LogicalID("file:" + event.Relative), Inputs: []PathRef{{Root: event.Root, Relative: event.Relative}},
			})
		}
	}
	return plan, nil
}

func (p *fakePolicy) Materialize(_ context.Context, task MaterializerTask) (Materialization, error) {
	p.source.mu.Lock()
	body := append([]byte(nil), p.source.bodies[indexKey{root: task.Inputs[0].Physical.Root, relative: task.Inputs[0].Physical.Relative}]...)
	p.source.mu.Unlock()
	value := Materialization{Logical: task.Logical, Fingerprint: sha256.Sum256(body)}
	for _, spec := range p.tenants {
		value.Objects = append(value.Objects, Projection{
			Tenant: spec.ID, Generation: spec.Generation, Parent: LogicalID("root:" + spec.ID),
			Name: path.Base(task.Inputs[0].Physical.Relative), Kind: catalog.KindFile, Mode: 0o600,
			Content: measuredContent{source: p.source, body: body}, Visibility: catalog.Visibility{Mount: true},
		})
	}
	return value, nil
}

func (p *fakePolicy) PlanMutation(_ context.Context, request MutationRequest) (MutationPlan, error) {
	if p.mutationPlan == nil {
		return MutationPlan{}, errors.New("mutation planning not configured")
	}
	return p.mutationPlan(request)
}

type fakeBackend struct {
	mu      sync.Mutex
	streams []*fakeStream
	opens   chan struct{}
}

type fakeExecutor struct {
	*fakePathSource
	*fakeBackend
	policy          *fakePolicy
	inspectMutation func(context.Context, MutationInspectionRequest) (MutationInspection, error)

	closeMu      sync.Mutex
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeErr     error
	closeOnce    sync.Once
	closeCalls   int
}

func (e *fakeExecutor) InspectMutation(
	ctx context.Context,
	request MutationInspectionRequest,
) (MutationInspection, error) {
	if e.inspectMutation != nil {
		return e.inspectMutation(ctx, request)
	}
	return MutationInspection{State: MutationInspectionNotFound}, nil
}

func (e *fakeExecutor) Materialize(ctx context.Context, task MaterializationTask) (Materialization, error) {
	inputs := make([]MaterializerInput, len(task.Expected))
	for index := range task.Expected {
		inputs[index].Physical = task.Expected[index]
	}
	return e.policy.Materialize(ctx, MaterializerTask{
		Fence: task.Fence, Tenants: task.Tenants, Logical: task.Request.Logical,
		Payload: append([]byte(nil), task.Request.Payload...), Inputs: inputs,
	})
}

func (e *fakeExecutor) ApplyMutation(ctx context.Context, task MutationTask) (MutationReceipt, error) {
	effects := make([]PhysicalEntry, len(task.Expected))
	roots := make(map[RootID]RootSpec)
	for _, root := range task.Roots {
		roots[root.ID] = root
	}
	for index, effect := range task.Expected {
		entry, err := e.Stat(ctx, roots[effect.Path.Root], effect.Path.Relative)
		if err != nil {
			return MutationReceipt{}, err
		}
		effects[index] = entry
	}
	payload, _ := json.Marshal(effects)
	return MutationReceipt{OperationID: task.OperationID, Effects: effects, Digest: sha256.Sum256(payload)}, nil
}

func (*fakeExecutor) AcknowledgeMutation(
	context.Context,
	causal.SourceAuthorityID,
	causal.Generation,
	catalog.MutationID,
	Fingerprint,
) error {
	return nil
}

func (*fakeExecutor) AbandonMutation(context.Context, causal.SourceAuthorityID, causal.Generation, catalog.MutationID) error {
	return nil
}

func (*fakeExecutor) MutationTerminalProofPage(
	context.Context,
	causal.SourceAuthorityID,
	catalog.MutationID,
	int,
) (MutationTerminalProofPage, error) {
	return MutationTerminalProofPage{}, nil
}

func (*fakeExecutor) ForgetMutation(context.Context, causal.SourceAuthorityID, MutationTerminalProof) error {
	return nil
}

func (e *fakeExecutor) Close() error {
	e.closeMu.Lock()
	e.closeCalls++
	entered, release, err := e.closeEntered, e.closeRelease, e.closeErr
	e.closeMu.Unlock()
	if entered != nil {
		e.closeOnce.Do(func() { close(entered) })
	}
	if release != nil {
		<-release
	}
	return err
}

func newFakeBackend() *fakeBackend { return &fakeBackend{opens: make(chan struct{}, 16)} }

func (b *fakeBackend) Open(_ context.Context, _ []RootSpec, resume []StreamCheckpoint, sink DurableEventSink) (EventStream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := len(b.streams) + 1
	checkpoint := StreamCheckpoint{Identity: StreamIdentity(fmt.Sprintf("stream-%d", index)), RootEpoch: RootEpoch(fmt.Sprintf("epoch-%d", index))}
	if len(resume) > 0 && index == 1 {
		checkpoint = resume[0]
	}
	stream := &fakeStream{sink: sink, checkpoints: []StreamCheckpoint{checkpoint}}
	b.streams = append(b.streams, stream)
	b.opens <- struct{}{}
	return stream, nil
}

func (b *fakeBackend) latest() *fakeStream {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.streams[len(b.streams)-1]
}

type fakeStream struct {
	mu          sync.Mutex
	sink        DurableEventSink
	checkpoints []StreamCheckpoint
	active      bool
	closed      bool
	flushHook   func(context.Context) error
}

func (s *fakeStream) Checkpoints() []StreamCheckpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCheckpoints(s.checkpoints)
}

func (s *fakeStream) Activate(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.active = true
	return nil
}

func (s *fakeStream) Flush(ctx context.Context) ([]StreamCheckpoint, error) {
	s.mu.Lock()
	hook := s.flushHook
	s.flushHook = nil
	s.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	return s.Checkpoints(), nil
}

func (s *fakeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeStream) emit(ctx context.Context, batch EventBatch) error {
	s.mu.Lock()
	if !s.active || s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	sink := s.sink
	s.mu.Unlock()
	err := sink(ctx, batch)
	if err == nil && batch.Cursor != 0 {
		s.mu.Lock()
		s.checkpoints[0].Cursor = batch.Cursor
		s.mu.Unlock()
	}
	return err
}

type manualClock struct {
	requests chan chan time.Time
}

func newManualClock() *manualClock { return &manualClock{requests: make(chan chan time.Time, 16)} }

func (c *manualClock) After(time.Duration) <-chan time.Time {
	result := make(chan time.Time, 1)
	c.requests <- result
	return result
}

func (c *manualClock) fire(t *testing.T) {
	t.Helper()
	select {
	case timer := <-c.requests:
		timer <- time.Now()
	case <-time.After(2 * time.Second):
		t.Fatal("retry was not scheduled")
	}
}

func testRuntime(t *testing.T, count int, clock RetryClock, failSnapshot int) (*Runtime, *catalog.Catalog, *fakePathSource, *fakeBackend) {
	t.Helper()
	store, err := catalog.Open(t.Context(), path.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	spec := tenant.TenantSpec{
		OwnerID: "owner", ID: "tenant", Mount: tenant.MountSpec{PresentationRoot: "/present/tenant"},
		Backing: tenant.BackingSpec{Root: "/backing/tenant"},
		Content: tenant.ContentSource{ID: string(testAuthority)}, Generation: 1,
		Traits: tenant.TenantTraits{Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount},
	}
	testTenants := []tenant.TenantSpec{spec}
	if _, err := store.ProvisionTenant(t.Context(), catalog.TenantProvision{
		OwnerID: string(spec.OwnerID), Tenant: spec.ID,
		Mount: catalog.MountPresentation{PresentationRoot: spec.Mount.PresentationRoot}, BackingRoot: spec.Backing.Root,
		ContentSourceID: spec.Content.ID, Access: spec.Traits.Access, CasePolicy: spec.Traits.CaseSensitivity,
		Presentations: spec.Traits.Presentations, Generation: spec.Generation,
	}); err != nil {
		t.Fatal(err)
	}
	source := newFakePathSource(count)
	backend := newFakeBackend()
	policy := &fakePolicy{source: source, failSnapshot: failSnapshot, tenants: testTenants}
	executor := &fakeExecutor{fakePathSource: source, fakeBackend: backend, policy: policy}
	fleetOwner := catalog.SourceAuthorityFleetOwnerID("sourceauthority-test")
	fleetDigest, err := catalog.SourceAuthorityFleetDigest([]causal.SourceAuthorityID{testAuthority})
	if err != nil {
		t.Fatal(err)
	}
	declarationDigest := sha256.Sum256([]byte("sourceauthority-test-declaration"))
	declarations := []catalog.SourceAuthorityDeclaration{{
		Authority: testAuthority, DriverID: "sourceauthority-test-driver",
		DeclarationDigest: declarationDigest,
	}}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	fleetStage, err := store.ReconcileSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
		Owner: fleetOwner, Generation: 1, Declarations: declarations,
		Complete: true, AuthorityCount: 1, AuthoritiesDigest: fleetDigest,
		DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetAcknowledgement{
		Owner: fleetOwner, Generation: 1, AuthorityCount: 1,
		AuthoritiesDigest: fleetDigest, DeclarationsDigest: declarationsDigest,
		StageDigest: fleetStage.StageDigest,
	}); err != nil {
		t.Fatal(err)
	}
	runtimeEpoch := [16]byte{1}
	runtimeProcess := proc.Record{
		PID: 4242, StartTime: "sourceauthority-start", Boot: "sourceauthority-boot",
		Comm: "holder", Generation: sourceAuthorityOwnerGeneration("sourceauthority-generation"), RecoveryID: recoveryid.SourceOwner,
	}
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: fleetOwner, Generation: 1, Authority: testAuthority,
	}
	if err := store.TakeoverSourceAuthorityRuntime(t.Context(), catalog.SourceAuthorityRuntimeTakeover{
		Ref: ref, Epoch: runtimeEpoch, Process: runtimeProcess,
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(t.Context(), Config{
		Store: store, Authority: testAuthority, FleetOwner: fleetOwner, FleetGeneration: 1,
		DriverID:          "sourceauthority-test-driver",
		DeclarationDigest: declarationDigest, RuntimeEpoch: runtimeEpoch,
		RuntimeProcess: runtimeProcess,
		Policy:         policy, Executor: executor,
		Tenants: testTenants, RetryClock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		runtime.Cancel()
		_ = runtime.Wait(context.Background())
		_ = store.Close()
	})
	return runtime, store, source, backend
}

func configureSourceObserverForTest(
	t *testing.T,
	store *catalog.Catalog,
	configuration observerConfiguration,
) {
	t.Helper()
	if configuration.FleetOwner == "" {
		configuration.FleetOwner = "sourceauthority-test"
		configuration.FleetGeneration = 1
	}
	if _, err := store.SourceAuthorityFleetHead(t.Context(), configuration.FleetOwner); errors.Is(err, catalog.ErrNotFound) {
		digest, digestErr := catalog.SourceAuthorityFleetDigest(
			[]causal.SourceAuthorityID{configuration.Authority},
		)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		declarationDigest := sha256.Sum256([]byte("observer:" + configuration.Authority))
		declarations := []catalog.SourceAuthorityDeclaration{{
			Authority: configuration.Authority, DriverID: "sourceauthority-test-driver",
			DeclarationDigest: declarationDigest,
		}}
		declarationsDigest, digestErr := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		stage, reconcileErr := store.ReconcileSourceAuthorityFleet(
			t.Context(),
			catalog.SourceAuthorityFleetReconcileRequest{
				Owner: configuration.FleetOwner, Generation: configuration.FleetGeneration,
				Declarations: declarations, Complete: true, AuthorityCount: 1,
				AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
			},
		)
		if reconcileErr != nil {
			t.Fatal(reconcileErr)
		}
		if _, reconcileErr = store.AcknowledgeSourceAuthorityFleet(
			t.Context(),
			catalog.SourceAuthorityFleetAcknowledgement{
				Owner: configuration.FleetOwner, Generation: configuration.FleetGeneration,
				AuthorityCount: 1, AuthoritiesDigest: digest,
				DeclarationsDigest: declarationsDigest, StageDigest: stage.StageDigest,
			},
		); reconcileErr != nil {
			t.Fatal(reconcileErr)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{catalog: store}
	if _, err := runtime.configureSourceObserver(t.Context(), configuration); err != nil {
		t.Fatal(err)
	}
}

func loadSourceObserverFenceForTest(
	t *testing.T,
	store *catalog.Catalog,
	authority causal.SourceAuthorityID,
) catalog.SourceObserverState {
	t.Helper()
	runtime := &Runtime{catalog: store}
	state, err := runtime.loadSourceObserverFence(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func loadSourceObserverControlForTest(
	ctx context.Context,
	store *catalog.Catalog,
	authority causal.SourceAuthorityID,
) (catalog.SourceObserverState, error) {
	runtime := &Runtime{catalog: store}
	state, err := runtime.loadSourceObserverFence(ctx, authority)
	if err != nil {
		return catalog.SourceObserverState{}, err
	}
	var after uint64
	for {
		page, err := store.SourceObserverInboxPage(
			ctx, authority, after, state.Stream.LastReceived, catalog.SourceObserverInboxPageLimit,
		)
		if err != nil {
			return catalog.SourceObserverState{}, err
		}
		state.Inbox = append(state.Inbox, page.Records...)
		if page.Next == 0 {
			return state, nil
		}
		if page.Next <= after {
			return catalog.SourceObserverState{}, fmt.Errorf("non-monotonic source observer inbox page")
		}
		after = page.Next
	}
}

func stageRepairSnapshotForTest(
	t *testing.T,
	store *catalog.Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
	operation causal.OperationID,
) catalog.SourceSnapshotStageRef {
	t.Helper()
	tenantID, err := catalog.NewTenantID("sourceauthority-repair")
	if err != nil {
		t.Fatal(err)
	}
	root := path.Join("/private/tmp/fusekit-sourceauthority-tests", string(authority))
	provision, err := store.ProvisionTenant(t.Context(), catalog.TenantProvision{
		OwnerID: "sourceauthority-test", Tenant: tenantID,
		Mount: catalog.MountPresentation{PresentationRoot: path.Join(root, "presentation")}, BackingRoot: path.Join(root, "backing"),
		ContentSourceID: string(authority), Access: catalog.TenantReadWrite,
		CasePolicy: catalog.CaseSensitive, Presentations: catalog.PresentMount, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rootBinding, err := store.ReserveSourceAuthorityBinding(
		t.Context(), authority, "sourceauthority-repair-root", "sourceauthority-repair-root",
	)
	if err != nil {
		t.Fatal(err)
	}
	state := loadSourceObserverFenceForTest(t, store, authority)
	fence := Fence{
		Authority: authority, AuthorityGeneration: 1,
		Streams: checkpointsFromCatalog(state.Checkpoints), Inbox: InboxSequence(state.Stream.LastReceived),
		RootDigest: Fingerprint(state.Stream.RootDigest), FleetDigest: Fingerprint(state.Stream.FleetDigest),
	}
	fenceDigest, err := digestJSON(fence)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := store.SourceWatermark(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	changeID := causal.ChangeID(operation)
	if changeID == (causal.ChangeID{}) {
		changeID[0] = 1
	}
	identity := catalog.SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1,
		Snapshot: snapshot, FenceDigest: fenceDigest,
		Change: causal.ChangeSet{
			SourceAuthority: authority, SourceRevision: revision + 1, ChangeID: changeID,
			OperationID: operation, Cause: causal.CauseExternalUnattributed,
		},
	}
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := store.AppendSourceSnapshotPublication(t.Context(), identity, catalog.SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
		Roots: []catalog.SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			LogicalID: rootBinding.LogicalID, RootKey: rootBinding.SourceKey,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestRuntimePagesSnapshotStreamsContentAndAppliesDelta(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 300, nil, 0)
	barrierCtx, cancelBarrier := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelBarrier()
	result, err := runtime.Barrier(barrierCtx, "tenant", 1)
	if err != nil || result.Target.SourceRevision != 1 || result.Source.SourceRevision != 1 {
		state, stateErr := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
		t.Fatalf("initial Barrier = %+v, %v; observer=%+v load=%v", result, err, state, stateErr)
	}
	source.mu.Lock()
	if source.scanCalls < 2 || source.maxScanLimit > snapshotScanPageSize || source.maxReadBuffer > 64<<10 ||
		source.closedReaders != 3 || source.closedSources != 3 {
		t.Fatalf("bounded snapshot metrics = scans %d limit %d read %d reader closes %d source closes %d", source.scanCalls, source.maxScanLimit, source.maxReadBuffer, source.closedReaders, source.closedSources)
	}
	source.mu.Unlock()
	source.put("root", "file-0001.json", 11, []byte("changed"))
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	batch := EventBatch{
		Stream: checkpoint.Identity, Predecessor: checkpoint.Cursor, Cursor: 50, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: "file-0001.json", Kind: EventModified, ID: 50}},
	}
	if err := stream.emit(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	deltaBarrierCtx, cancelDeltaBarrier := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelDeltaBarrier()
	result, err = runtime.Barrier(deltaBarrierCtx, "tenant", 1)
	if err != nil || result.Target.SourceRevision != 2 || result.Source.SourceRevision != 2 {
		state, stateErr := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
		pending, pendingErr := store.PendingSourcePublicationStage(t.Context(), testAuthority)
		t.Fatalf(
			"delta Barrier = %+v, %v; observer=%+v load=%v pending=%+v pending_load=%v checkpoint=%+v",
			result, err, state, stateErr, pending, pendingErr, stream.Checkpoints(),
		)
	}
	watermark, err := store.SourceWatermark(t.Context(), testAuthority)
	if err != nil || watermark != 2 {
		t.Fatalf("watermark = %d, %v", watermark, err)
	}
}

func TestIncrementalOneObjectInTenThousandUsesConstantKeyedSourceQueries(t *testing.T) {
	runtime, _, source, backend := testRuntime(t, 10_000, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	source.scanCalls = 0
	source.statCalls = 0
	source.mu.Unlock()
	source.put("root", "file-9999.json", 10_009, []byte("changed"))
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	if err := stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, Predecessor: checkpoint.Cursor, Cursor: checkpoint.Cursor + 1, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: "file-9999.json", Kind: EventModified, ID: checkpoint.Cursor + 1}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.scanCalls != 0 || source.statCalls != 3 {
		t.Fatalf("one-object delta source queries = scans %d stats %d, want 0 scans and 3 keyed stats", source.scanCalls, source.statCalls)
	}
}

func TestMutationLocatorReturnsEveryPhysicalInputForLogicalIdentity(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 2, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	policy := runtime.policy.(*fakePolicy)
	policy.snapshotPlan = func(_ context.Context, view SnapshotView, cursor SnapshotPlanCursor, _ int) (SnapshotPlanPage, error) {
		if cursor != "" {
			return SnapshotPlanPage{}, ErrInvalidPlan
		}
		plan := SnapshotPlanPage{Fence: view.Fence(), AffectedKeys: []causal.LogicalKey{"settings"}}
		for _, spec := range view.Tenants() {
			plan.Roots = append(plan.Roots, TenantRoot{Tenant: spec.ID, Generation: spec.Generation, Logical: LogicalID("root:" + spec.ID)})
		}
		plan.Reads = []MaterializationRequest{{
			Logical: "combined", Inputs: []PathRef{
				{Root: "root", Relative: "file-0000.json"},
				{Root: "root", Relative: "file-0001.json"},
			},
		}}
		return plan, nil
	}
	if err := store.RequireSourceObserverSnapshot(t.Context(), testAuthority); err != nil {
		t.Fatal(err)
	}
	runtime.signal()
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	binding := requireSourceBindingLookup(t, store, "combined")
	revision, err := store.SourceWatermark(t.Context(), testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := runtime.resolveMutationLocator(t.Context(), catalog.SourceLocator{
		SourceAuthority: testAuthority, SourceKey: binding.SourceKey, SourceRevision: revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(locator.Bindings) != 2 || locator.Bindings[0].Physical.Relative != "file-0000.json" ||
		locator.Bindings[1].Physical.Relative != "file-0001.json" {
		t.Fatalf("physical mutation bindings = %+v", locator.Bindings)
	}
}

func TestSnapshotPlanCursorCycleQuarantinesBeforeCursorReuse(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 2, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	policy := runtime.policy.(*fakePolicy)
	calls := 0
	policy.snapshotPlan = func(_ context.Context, view SnapshotView, cursor SnapshotPlanCursor, _ int) (SnapshotPlanPage, error) {
		calls++
		page := SnapshotPlanPage{Fence: view.Fence()}
		switch cursor {
		case "":
			page.AffectedKeys = []causal.LogicalKey{"page-a"}
			for _, spec := range view.Tenants() {
				page.Roots = append(page.Roots, TenantRoot{
					Tenant: spec.ID, Generation: spec.Generation, Logical: LogicalID("root:" + spec.ID),
				})
			}
			page.Next = "page-a"
		case "page-a":
			page.AffectedKeys = []causal.LogicalKey{"page-b"}
			page.Next = "page-c"
		case "page-c":
			page.AffectedKeys = []causal.LogicalKey{"page-c"}
			page.Next = "page-a"
		default:
			t.Fatalf("PlanSnapshot called after cursor cycle was complete: %q", cursor)
		}
		return page, nil
	}
	if err := store.RequireSourceObserverSnapshot(t.Context(), testAuthority); err != nil {
		t.Fatal(err)
	}
	runtime.signal()
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("cursor cycle barrier = %v, want quarantine", err)
	}
	if calls != 3 {
		t.Fatalf("PlanSnapshot calls = %d, want 3 before detecting the cursor cycle", calls)
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil || state.Stream.Mode != catalog.SourceObserverQuarantined ||
		!strings.Contains(state.Stream.Quarantine, "snapshot plan cursor cycle") {
		t.Fatalf("cursor cycle durable state = %+v, %v", state, err)
	}
}

func TestBuildPublicationSharesOneVerifiedStageAcrossIdenticalTenantProjections(t *testing.T) {
	runtime, store, source, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	head, err := store.TopologyHead(t.Context(), "owner")
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.TopologySnapshot(t.Context(), catalog.TopologySnapshotRequest{
		Owner: "owner", Revision: head.Revision, Limit: catalog.TopologyPageLimit,
	})
	if err != nil || len(page.Tenants) != 1 || page.Next != (catalog.TopologyCursor{}) {
		t.Fatalf("TopologySnapshot = %+v, %v", page, err)
	}
	second := page.Tenants[0]
	second.Tenant = "tenant-two"
	second.Root = catalog.ObjectID{}
	second.Mount.PresentationRoot = "/present/tenant-two"
	second.BackingRoot = "/backing/tenant-two"
	second, err = store.ProvisionTenant(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	specs := runtime.currentTenants()
	secondSpec := specs[0]
	secondSpec.ID = second.Tenant
	secondSpec.Mount.PresentationRoot = second.Mount.PresentationRoot
	secondSpec.Backing.Root = second.BackingRoot
	secondSpec.Generation = second.Generation
	if err := runtime.Reconfigure(t.Context(), append(specs, secondSpec)); err != nil {
		t.Fatal(err)
	}
	if err := store.RequireSourceObserverSnapshot(t.Context(), testAuthority); err != nil {
		t.Fatal(err)
	}
	runtime.signal()
	if _, err := runtime.Barrier(t.Context(), page.Tenants[0].Tenant, page.Tenants[0].Generation); err != nil {
		t.Fatal(err)
	}
	body := []byte("byte-identical")
	fingerprint := sha256.Sum256(body)
	logical := LogicalID("shared-logical")
	roots := []TenantRoot{
		{Tenant: page.Tenants[0].Tenant, Generation: page.Tenants[0].Generation, Logical: LogicalID("root:" + page.Tenants[0].Tenant)},
		{Tenant: second.Tenant, Generation: second.Generation, Logical: LogicalID("root:" + second.Tenant)},
	}
	materialized := []Materialization{{
		Logical: logical, Fingerprint: fingerprint,
		Objects: []Projection{
			{Tenant: page.Tenants[0].Tenant, Generation: page.Tenants[0].Generation, Parent: roots[0].Logical, Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600, Content: measuredContent{source: source, body: body}, Visibility: catalog.Visibility{Mount: true}},
			{Tenant: second.Tenant, Generation: second.Generation, Parent: roots[1].Logical, Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600, Content: measuredContent{source: source, body: body}, Visibility: catalog.Visibility{Mount: true}},
		},
	}}
	watermark, err := store.SourceWatermark(t.Context(), testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	publication, _, err := runtime.buildPublication(t.Context(), catalog.SourceDelta, watermark,
		[]causal.LogicalKey{"settings"}, roots, materialized, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(publication.Tenants) != 2 || len(publication.Tenants[0].Objects) != 1 || len(publication.Tenants[1].Objects) != 1 {
		t.Fatalf("publication targets = %+v", publication.Tenants)
	}
	first := publication.Tenants[0].Objects[0].Content
	secondRef := publication.Tenants[1].Objects[0].Content
	if first != secondRef {
		t.Fatalf("identical projections used distinct stages: %x != %x", first.Stage, secondRef.Stage)
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.applyStagedPublications(t.Context(), []sourcePublication{publication}, stagedObserverSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: testAuthority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Operation: publication.Change.OperationID,
		},
	}); err != nil {
		t.Fatalf("apply staged shared content: %v", err)
	}
	if err := store.ReleaseUnclaimedContent(t.Context(), []catalog.ContentRef{first}); err != nil {
		t.Fatalf("consumed shared stage retry: %v", err)
	}
}

func TestBuildPublicationReaderCloseFailureReleasesOwnedStage(t *testing.T) {
	t.Parallel()
	databasePath := path.Join(t.TempDir(), "catalog.sqlite")
	store, err := catalog.Open(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	configureSourceObserverForTest(t, store, observerConfiguration{
		Authority: testAuthority, Stream: "stream", RootEpoch: "epoch",
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
		Roots: []catalog.SourceObserverRootRecord{{
			ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1,
			BirthSec: 1, BirthNsec: 1, Kind: uint8(RootDirectory),
		}},
		Checkpoints: []catalog.SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}},
	})
	runtime := &Runtime{catalog: store, authority: testAuthority}
	closeFailure := errors.New("reader close failed after staging")
	_, _, err = runtime.buildPublication(
		t.Context(), catalog.SourceDelta, 0, []causal.LogicalKey{"settings"},
		[]TenantRoot{{Tenant: "tenant", Generation: 1, Logical: "root"}},
		[]Materialization{{
			Logical: "settings", Fingerprint: Fingerprint{1},
			Objects: []Projection{{
				Tenant: "tenant", Generation: 1, Parent: "root", Name: "settings.json",
				Kind: catalog.KindFile, Mode: 0o600,
				Content:    closeErrorContent{body: []byte("staged"), err: closeFailure},
				Visibility: catalog.Visibility{Mount: true},
			}},
		}}, nil, nil, nil,
	)
	if !errors.Is(err, closeFailure) {
		t.Fatalf("buildPublication error = %v, want close failure", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	var stages int
	if err := database.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM content_stages").Scan(&stages); err != nil {
		t.Fatal(err)
	}
	if stages != 0 {
		t.Fatalf("reader close failure leaked %d content stages", stages)
	}
}

func TestRuntimeReopensZeroCursorDiscontinuity(t *testing.T) {
	runtime, store, _, backend := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	before, err := store.SourceWatermark(t.Context(), testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	err = stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: ".", Kind: EventMetadata, Flags: FlagRootChanged}},
	})
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("zero-cursor sink = %v, want ErrSnapshotRequired", err)
	}
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		state, stateErr := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
		t.Fatalf("Barrier after discontinuity: %v; observer=%+v load=%v", err, state, stateErr)
	}
	if runtime.currentStream() == stream || backend.latest() == stream {
		state, stateErr := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
		t.Fatalf("barrier settled before the reopened stream was installed: observer=%+v load=%v", state, stateErr)
	}
	after, err := store.SourceWatermark(t.Context(), testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("snapshot repair watermark = %d, want exact successor %d", after, before+1)
	}
}

func TestRuntimeRetriesSnapshotInstabilityOnActorClock(t *testing.T) {
	clock := newManualClock()
	runtime, _, _, _ := testRuntime(t, 1, clock, defaultSnapshotAttempts)
	clock.fire(t)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRootsRejectsOverlap(t *testing.T) {
	roots := []RootSpec{
		{Authority: testAuthority, ID: "parent", Path: "/source", Kind: RootDirectory, Generation: 1},
		{Authority: testAuthority, ID: "child", Path: "/source/child", Kind: RootDirectory, Generation: 1},
	}
	if err := validateRoots(testAuthority, roots); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("validateRoots(overlap) = %v", err)
	}
}

func TestWaitJoinsCanceledActorBeforeReturningContextError(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	policy := runtime.policy.(*fakePolicy)
	policy.deltaStarted = make(chan struct{})
	policy.deltaRelease = make(chan struct{})
	source.put("root", "file-0000.json", 10, []byte("changed"))
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	if err := stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, Predecessor: checkpoint.Cursor, Cursor: 1, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-policy.deltaStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("delta planning did not start")
	}
	runtime.Cancel()
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Wait(waitCtx) }()
	select {
	case err := <-done:
		t.Fatalf("Wait returned before actor joined: %v", err)
	default:
	}
	close(policy.deltaRelease)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait error = %v, want context cancellation after join", err)
	}
	requireRuntimeFenceClosed(t, store, runtime)
}

func TestCloseJoinsOnceAndReplaysExactTerminalResult(t *testing.T) {
	runtime, _, _, _ := testRuntime(t, 0, nil, 0)
	executor := runtime.executor.(*fakeExecutor)
	terminalErr := errors.New("executor terminal failure")
	executor.closeEntered = make(chan struct{})
	executor.closeRelease = make(chan struct{})
	executor.closeErr = terminalErr
	closeCtx, cancelClose := context.WithCancel(t.Context())
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- runtime.Close(closeCtx) }()
	go func() { second <- runtime.Close(context.Background()) }()
	<-executor.closeEntered
	cancelClose()
	select {
	case err := <-first:
		t.Fatalf("canceled Close returned before executor settlement: %v", err)
	case err := <-second:
		t.Fatalf("concurrent Close returned before executor settlement: %v", err)
	default:
	}
	close(executor.closeRelease)
	if err := <-first; !errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("canceled Close = %v, want caller cancellation and terminal failure", err)
	}
	if err := <-second; errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("concurrent Close = %v, want terminal failure", err)
	}
	if err := runtime.Close(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Close = %v, want terminal failure", err)
	}
	if err := runtime.Wait(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("Wait = %v, want terminal failure", err)
	}
	executor.closeMu.Lock()
	closeCalls := executor.closeCalls
	executor.closeMu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("executor closes = %d, want 1", closeCalls)
	}
}

func TestWaitCancellationCancelsAndJoinsExactTerminalResult(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 0, nil, 0)
	executor := runtime.executor.(*fakeExecutor)
	terminalErr := errors.New("executor terminal failure")
	executor.closeEntered = make(chan struct{})
	executor.closeRelease = make(chan struct{})
	executor.closeErr = terminalErr
	waitCtx, cancelWait := context.WithCancel(t.Context())
	cancelWait()
	result := make(chan error, 1)
	go func() { result <- runtime.Wait(waitCtx) }()
	<-executor.closeEntered
	select {
	case err := <-result:
		t.Fatalf("Wait returned before executor settlement: %v", err)
	default:
	}
	close(executor.closeRelease)
	if err := <-result; !errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("canceled Wait = %v, want caller cancellation and terminal failure", err)
	}
	if err := runtime.Wait(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Wait = %v, want terminal failure", err)
	}
	executor.closeMu.Lock()
	closeCalls := executor.closeCalls
	executor.closeMu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("executor closes = %d, want 1", closeCalls)
	}
	requireRuntimeFenceClosed(t, store, runtime)
}

func TestRuntimeFenceCloseAcceptsExactLostResponse(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 0, nil, 0)
	lost := &lostRuntimeCloseResponseStore{Store: store, err: errors.New("lost close response")}
	probe := &Runtime{
		catalog: lost, authority: runtime.authority,
		fleetOwner: runtime.fleetOwner, fleetGeneration: runtime.fleetGeneration,
		runtimeEpoch: runtime.runtimeEpoch, clock: wallRetryClock{},
	}
	if err := probe.closeRuntimeFence(); err != nil {
		t.Fatalf("closeRuntimeFence after committed lost response: %v", err)
	}
	if lost.calls != 1 {
		t.Fatalf("CloseSourceAuthorityRuntime calls = %d, want 1 exact committed attempt", lost.calls)
	}
	runtime.Cancel()
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireRuntimeFenceClosed(t, store, runtime)
}

func TestSourceMutationCommittedRejectsInvalidIdentityAndSignalsExactAuthority(t *testing.T) {
	runtime := &Runtime{authority: "source", wake: make(chan struct{}, 1)}
	operation := catalog.MutationID{1}
	if err := runtime.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: operation, SourceID: "other",
	}); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("mismatched commit = %v, want integrity", err)
	}
	if err := runtime.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		SourceID: "source",
	}); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("empty operation commit = %v, want integrity", err)
	}
	select {
	case <-runtime.wake:
		t.Fatal("invalid commit signaled the runtime")
	default:
	}
	if err := runtime.SourceMutationCommitted(t.Context(), tenant.SourceMutationCommit{
		OperationID: operation, SourceID: "source",
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runtime.wake:
	default:
		t.Fatal("valid commit did not signal the runtime")
	}
}

func TestNewRuntimeRejectsDeclarationMismatchBeforePolicyIOAndClosesEpoch(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 0, nil, 0)
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	nextEpoch := [16]byte{2}
	nextProcess := runtime.runtimeProcess
	nextProcess.Generation = sourceAuthorityOwnerGeneration("sourceauthority-next-generation")
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: runtime.fleetOwner, Generation: runtime.fleetGeneration,
		Authority: runtime.authority,
	}
	if err := store.TakeoverSourceAuthorityRuntime(
		t.Context(),
		catalog.SourceAuthorityRuntimeTakeover{
			Ref: ref, ExpectedEpoch: runtime.runtimeEpoch,
			Epoch: nextEpoch, Process: nextProcess,
		},
	); err != nil {
		t.Fatal(err)
	}
	policy := &fakePolicy{source: source, tenants: runtime.currentTenants()}
	executor := &fakeExecutor{
		fakePathSource: source, fakeBackend: backend, policy: policy,
	}
	_, err := NewRuntime(t.Context(), Config{
		Store: store, Authority: runtime.authority,
		FleetOwner: runtime.fleetOwner, FleetGeneration: runtime.fleetGeneration,
		DriverID:          runtime.driverID,
		DeclarationDigest: [32]byte{0xff}, RuntimeEpoch: nextEpoch,
		RuntimeProcess: nextProcess, Policy: policy, Executor: executor,
		Tenants: runtime.currentTenants(),
	})
	if !errors.Is(err, catalog.ErrMutationConflict) {
		t.Fatalf("declaration mismatch = %v, want ErrMutationConflict", err)
	}
	if policy.rootsCalls != 0 {
		t.Fatalf("policy Roots calls = %d, want zero before declaration verification", policy.rootsCalls)
	}
	state, err := store.SourceAuthorityRuntimeStatus(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Closed || state.Epoch != nextEpoch || state.Process == nil || *state.Process != nextProcess {
		t.Fatalf("rejected runtime epoch was not settled = %+v", state)
	}
}

type lostRuntimeCloseResponseStore struct {
	Store
	err   error
	calls int
}

func (s *lostRuntimeCloseResponseStore) CloseSourceAuthorityRuntime(
	ctx context.Context,
	fence catalog.SourceAuthorityRuntimeFence,
) error {
	s.calls++
	if err := s.Store.CloseSourceAuthorityRuntime(ctx, fence); err != nil {
		return err
	}
	return s.err
}

func requireRuntimeFenceClosed(t *testing.T, store Store, runtime *Runtime) {
	t.Helper()
	state, err := store.SourceAuthorityRuntimeStatus(t.Context(), catalog.SourceAuthorityRuntimeRef{
		Owner: runtime.fleetOwner, Generation: runtime.fleetGeneration, Authority: runtime.authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.Epoch != runtime.runtimeEpoch || !state.Closed {
		t.Fatalf("runtime fence after terminal settlement = %+v", state)
	}
}

func TestApplySourceMutationClosesContentBeforeActorHandoff(t *testing.T) {
	runtime, _, _, _ := testRuntime(t, 0, nil, 0)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	content := &countedMutationContent{}
	if _, err := runtime.ApplySourceMutation(canceled, tenant.SourceMutationStep{}, tenant.SourceMutationOperation{}, content); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled apply = %v, want context.Canceled", err)
	}
	if count := content.closeCount(); count != 1 {
		t.Fatalf("pre-canceled content closes = %d, want 1", count)
	}

	runtime.Cancel()
	if err := runtime.Wait(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait: %v", err)
	}
	closedContent := &countedMutationContent{}
	if _, err := runtime.ApplySourceMutation(context.Background(), tenant.SourceMutationStep{}, tenant.SourceMutationOperation{}, closedContent); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed runtime apply = %v, want ErrClosed", err)
	}
	if count := closedContent.closeCount(); count != 1 {
		t.Fatalf("closed runtime content closes = %d, want 1", count)
	}
}

func TestRequestErrorDoesNotPoisonLaterBarrier(t *testing.T) {
	runtime, _, _, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ApplySourceMutation(t.Context(), tenant.SourceMutationStep{}, tenant.SourceMutationOperation{}, nil); err == nil {
		t.Fatal("incomplete mutation completion unexpectedly succeeded")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := runtime.Barrier(ctx, "tenant", 1); err != nil {
		t.Fatalf("Barrier after unrelated request error: %v", err)
	}
}

func TestSettledFenceWaitsForMutationLiability(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	checkpoints, through, err := runtime.captureStreamFence(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	operation := catalog.MutationID{23}
	origin := catalog.CausalOrigin{Cause: causal.CauseDaemonWrite}
	payload := mutationEnvelopeForRuntimeTest(t, operation, 1, origin)
	if err := store.PutSourceMutationExpectation(t.Context(), catalog.SourceMutationExpectationRecord{
		Operation: operation, Authority: testAuthority, Tenant: "tenant", Generation: 1,
		Origin: origin, Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := runtime.settledFence(t.Context(), checkpoints, through); err != nil || ready {
		t.Fatalf("settled fence with mutation liability = ready %v, err %v", ready, err)
	}
}

func TestTransientMutationFenceReadRetriesOnActorClock(t *testing.T) {
	clock := newManualClock()
	runtime, _, _, _ := testRuntime(t, 1, clock, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	attempted := make(chan struct{}, 8)
	allow := make(chan struct{})
	runtime.mutationFence = func(context.Context) (bool, error) {
		select {
		case <-allow:
			return false, nil
		default:
			attempted <- struct{}{}
			return false, errors.New("temporary mutation fence read")
		}
	}
	result := make(chan error, 1)
	go func() { result <- runtime.Reconfigure(context.Background(), runtime.currentTenants()) }()
	select {
	case <-attempted:
	case <-time.After(2 * time.Second):
		t.Fatal("mutation fence was not read")
	}
	close(allow)
	clock.fire(t)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconfigure did not resume after retry")
	}
}

func TestQueuedReconfiguresDrainAfterMutationFenceClears(t *testing.T) {
	runtime, _, _, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	calls := make(chan struct{}, 8)
	allow := make(chan struct{})
	runtime.mutationFence = func(context.Context) (bool, error) {
		calls <- struct{}{}
		select {
		case <-allow:
			return false, nil
		default:
			return true, nil
		}
	}
	results := make(chan error, 2)
	specs := runtime.currentTenants()
	go func() { results <- runtime.Reconfigure(context.Background(), specs) }()
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("first reconfigure was not fenced")
	}
	go func() { results <- runtime.Reconfigure(context.Background(), specs) }()
	select {
	case <-calls:
	case <-time.After(2 * time.Second):
		t.Fatal("second reconfigure was not queued")
	}
	close(allow)
	runtime.signal()
	for range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queued reconfigure starved")
		}
	}
}

func TestTerminalPublicationStageQuarantinesAndReleasesContent(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	ref, err := store.StageContent(t.Context(), bytes.NewReader([]byte("bad")))
	if err != nil {
		t.Fatal(err)
	}
	changeID, operationID, err := newCausalIDs()
	if err != nil {
		t.Fatal(err)
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	binding := requireSourceBindingLookup(t, store, "root:tenant")
	rootKey := binding.SourceKey
	publication := sourcePublication{
		Mode: catalog.SourceDelta, Predecessor: 1,
		Change: causal.ChangeSet{SourceAuthority: testAuthority, SourceRevision: 2, ChangeID: changeID, OperationID: operationID,
			Cause: causal.CauseExternalUnattributed, AffectedKeys: []causal.LogicalKey{"settings"}},
		Tenants: []catalog.SourceTenant{{Tenant: "tenant", Generation: 1, RootKey: rootKey, Objects: []catalog.SourceObject{{
			Key: "invalid-parent", Parent: "missing", Name: "settings.json", Kind: catalog.KindFile, Mode: 0o600,
			ContentRevision: 2, Content: ref, Visibility: catalog.Visibility{Mount: true},
		}}}},
	}
	settlement := stagedObserverSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: testAuthority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Operation: operationID,
		},
	}
	if err := runtime.applyStagedPublications(t.Context(), []sourcePublication{publication}, settlement); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("terminal staged publication error = %v, want quarantine", err)
	}
	state, err = loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	pending, pendingErr := store.PendingSourcePublicationStage(t.Context(), testAuthority)
	if err != nil || state.Stream.Mode != catalog.SourceObserverQuarantined || pendingErr != nil || pending == nil {
		t.Fatalf("quarantined observer = %+v, %v", state, err)
	}
	if err := store.AbortSourcePublicationStage(t.Context(), pending.Authority, pending.Operation); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.PendingSourcePublicationStage(t.Context(), testAuthority); err != nil || pending != nil {
		t.Fatalf("aborted staged publication = %+v, %v", pending, err)
	}
}

func requireSourceBindingLookup(
	t *testing.T,
	store *catalog.Catalog,
	logical string,
) catalog.SourceAuthorityBindingRecord {
	t.Helper()
	request, err := catalog.NewSourceAuthorityBindingLookupRequest(
		testAuthority,
		0,
		[]string{logical},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.SourceAuthorityBindingLookup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Logical != logical || page.Entries[0].Record == nil {
		t.Fatalf("source binding lookup for %q = %+v", logical, page)
	}
	return *page.Entries[0].Record
}

func requireSourcePhysicalLookup(
	t *testing.T,
	store *catalog.Catalog,
	locator catalog.SourceIndexLocator,
) catalog.SourcePhysicalIndexRecord {
	t.Helper()
	request, err := catalog.NewSourcePhysicalIndexLookupRequest(
		testAuthority,
		0,
		[]catalog.SourceIndexLocator{locator},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.SourcePhysicalIndexLookup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Locator != locator || page.Entries[0].Record == nil {
		t.Fatalf("source physical lookup for %+v = %+v", locator, page)
	}
	return *page.Entries[0].Record
}

func TestIncrementalRenamePreservesLogicalBinding(t *testing.T) {
	runtime, store, source, _ := testRuntime(t, 0, nil, 0)
	source.put("root", "old.json", 42, []byte("value"))
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	record := requireSourcePhysicalLookup(
		t, store, catalog.SourceIndexLocator{RootID: "root", Relative: "old.json"},
	)
	record.Logical = []string{"logical"}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	_, settlementOperation, err := newCausalIDs()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.applyStagedPublications(t.Context(), nil, stagedObserverSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: testAuthority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Through: state.Stream.LastApplied, Operation: settlementOperation,
		},
		Index: []catalog.SourcePhysicalIndexRecord{record},
	}); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	delete(source.entries, indexKey{root: "root", relative: "old.json"})
	delete(source.bodies, indexKey{root: "root", relative: "old.json"})
	source.mu.Unlock()
	source.put("root", "new.json", 42, []byte("value"))
	state, err = loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	view, upserts, deletes, _, err := runtime.incrementalView(t.Context(), state, EventBatch{
		Stream: StreamIdentity(state.Stream.Stream), RootEpoch: RootEpoch(state.Stream.RootEpoch), Predecessor: 1, Cursor: 2,
		Events: []PathEvent{{Root: "root", Relative: "new.json", Kind: EventRenamed, ID: 2}},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	renamed, found := view.Entry("root", "new.json")
	if !found || !slices.Equal(renamed.Logical, []LogicalID{"logical"}) || len(upserts) != 1 ||
		len(deletes) != 1 || deletes[0].Relative != "old.json" {
		t.Fatalf("rename view=%+v upserts=%+v deletes=%+v", renamed, upserts, deletes)
	}
}

func TestIncrementalAtomicReplaceRetainsLocatorBinding(t *testing.T) {
	runtime, store, source, _ := testRuntime(t, 0, nil, 0)
	source.put("root", "settings.json", 42, []byte("old"))
	old, _ := source.Stat(t.Context(), RootSpec{ID: "root"}, "settings.json")
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	record := requireSourcePhysicalLookup(
		t, store, catalog.SourceIndexLocator{RootID: "root", Relative: "settings.json"},
	)
	record.Logical = []string{"logical"}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	_, settlementOperation, err := newCausalIDs()
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.applyStagedPublications(t.Context(), nil, stagedObserverSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: testAuthority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Through: state.Stream.LastApplied, Operation: settlementOperation,
		},
		Index: []catalog.SourcePhysicalIndexRecord{record},
	}); err != nil {
		t.Fatal(err)
	}
	source.put("root", "settings.json", 84, []byte("new"))
	state, err = loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil {
		t.Fatal(err)
	}
	view, upserts, deletes, _, err := runtime.incrementalView(t.Context(), state, EventBatch{
		Stream: StreamIdentity(state.Stream.Stream), RootEpoch: RootEpoch(state.Stream.RootEpoch), Predecessor: 1, Cursor: 2,
		Events: []PathEvent{{Root: "root", Relative: "settings.json", Kind: EventRenamed, ID: 2}},
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	replaced, found := view.Entry("root", "settings.json")
	if !found || replaced.Physical.Identity == old.Identity || !slices.Equal(replaced.Logical, []LogicalID{"logical"}) ||
		len(upserts) != 1 || len(deletes) != 0 {
		t.Fatalf("replace view=%+v upserts=%+v deletes=%+v", replaced, upserts, deletes)
	}
}

func TestFullSnapshotDuplicatePhysicalIdentityQuarantines(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 2, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	first := source.entries[indexKey{root: "root", relative: "file-0000.json"}]
	second := source.entries[indexKey{root: "root", relative: "file-0001.json"}]
	second.Identity = first.Identity
	source.entries[indexKey{root: "root", relative: "file-0001.json"}] = second
	source.mu.Unlock()
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	if err := stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch,
		Events: []PathEvent{{Root: "root", Relative: ".", Kind: EventMetadata, Flags: FlagRootChanged}},
	}); !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("discontinuity = %v, want snapshot", err)
	}
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("barrier after duplicate identity = %v, want quarantine", err)
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil || state.Stream.Mode != catalog.SourceObserverQuarantined {
		t.Fatalf("duplicate identity observer = %+v, %v", state.Stream, err)
	}
}

func TestMutationCompletionAndEchoUnblockReconfigure(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	old, err := source.Stat(t.Context(), runtime.currentRoots()[0], "file-0000.json")
	if err != nil {
		t.Fatal(err)
	}
	mutationID := testSourceMutationID(1)
	head, err := store.Head(t.Context(), "tenant")
	if err != nil {
		t.Fatal(err)
	}
	step := tenant.SourceMutationStep{
		TenantID: "tenant", Generation: 1, OperationID: mutationID, SourceID: string(testAuthority),
		SourceMetadata: "settings", Kind: catalog.MutationRevise, ExpectedHead: head,
		Origin: catalog.CausalOrigin{Cause: causal.CauseProviderMutation, Domain: "domain", Generation: 1},
	}
	policy := runtime.policy.(*fakePolicy)
	policy.mutationPlan = func(MutationRequest) (MutationPlan, error) {
		return MutationPlan{
			Program: MutationProgram{Actions: []MutationAction{{
				Kind: MutationAtomicWriteFile, Path: PathRef{Root: "root", Relative: "file-0000.json"},
				Mode: 0o600, Data: []byte("provider-write"),
			}}},
			Effects: []ExpectedEffect{{
				Path: PathRef{Root: "root", Relative: "file-0000.json"}, Before: physicalState(old),
				Outcome: MutationPresent, Kind: PhysicalFile,
			}},
		}, nil
	}
	operation, err := runtime.PrepareSourceMutation(t.Context(), step)
	if err != nil {
		t.Fatal(err)
	}
	reconfigured := make(chan error, 1)
	go func() { reconfigured <- runtime.Reconfigure(context.Background(), runtime.currentTenants()) }()
	select {
	case err := <-reconfigured:
		t.Fatalf("reconfigure passed active mutation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	source.put("root", "file-0000.json", 10, []byte("provider-write"))
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	stream.mu.Lock()
	stream.flushHook = func(ctx context.Context) error {
		return stream.emit(ctx, EventBatch{
			Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch, Predecessor: checkpoint.Cursor, Cursor: 1,
			Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: 1}},
		})
	}
	stream.mu.Unlock()
	if _, err := runtime.ApplySourceMutation(t.Context(), step, operation, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reconfigured:
		t.Fatalf("reconfigure passed a physical receipt before catalog commit: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	records := sourceMutationExpectationsForTest(t, store)
	err = nil
	if err != nil || len(records) != 1 || records[0].State != 2 {
		t.Fatalf("receipt-only mutation records = %+v, %v", records, err)
	}
	runtime.Cancel()
	if err := <-reconfigured; !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked reconfigure after cancel = %v, want ErrClosed", err)
	}
}

func TestMutationEchoPartitionPreservesStreamAndEventOrdinalsAcrossRetainedRange(t *testing.T) {
	store, err := catalog.Open(t.Context(), path.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	configureSourceObserverForTest(t, store, observerConfiguration{
		Authority: testAuthority, Stream: "stream-set", RootEpoch: "epoch-set",
		RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
		Roots: []catalog.SourceObserverRootRecord{{
			ID: "other-root", Generation: 1, Path: "/other", VolumeUUID: "volume", Inode: 2, Kind: uint8(RootDirectory),
		}, {
			ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume", Inode: 1, Kind: uint8(RootDirectory),
		}},
		Checkpoints: []catalog.SourceObserverCheckpointRecord{
			{Stream: "stream-a", RootEpoch: "epoch-a"}, {Stream: "stream-b", RootEpoch: "epoch-b"},
		},
	})
	if err := store.BeginSourceSnapshotStage(t.Context(), testAuthority, "initial"); err != nil {
		t.Fatal(err)
	}
	initialOperation := causal.OperationID{1}
	initialRef := stageRepairSnapshotForTest(t, store, testAuthority, "initial", initialOperation)
	if _, err := store.PromoteSourceSnapshot(t.Context(), initialRef, catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: testAuthority, Stream: "stream-set", RootEpoch: "epoch-set", Operation: initialOperation,
		},
		Snapshot: initialRef,
	}); err != nil {
		t.Fatal(err)
	}
	batches := []EventBatch{
		{Stream: "stream-a", RootEpoch: "epoch-a", Predecessor: 0, Cursor: 1, Events: []PathEvent{
			{Root: "root", Relative: "settings.json", Kind: EventModified, ID: 1, Ordinal: 1},
		}},
		{Stream: "stream-b", RootEpoch: "epoch-b", Predecessor: 0, Cursor: 1, Events: []PathEvent{
			{Root: "other-root", Relative: "other.json", Kind: EventModified, ID: 1, Ordinal: 2},
		}},
		{Stream: "stream-a", RootEpoch: "epoch-a", Predecessor: 1, Cursor: 2, Events: []PathEvent{
			{Root: "root", Relative: "settings.json", Kind: EventModified, ID: 2, Ordinal: 4},
		}},
	}
	for _, batch := range batches {
		payload, err := json.Marshal(batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AppendSourceObserverInbox(t.Context(), catalog.SourceObserverInboxRecord{
			Authority: testAuthority, Stream: string(batch.Stream), RootEpoch: string(batch.RootEpoch),
			NativePredecessor: uint64(batch.Predecessor), NativeCursor: uint64(batch.Cursor), EventCount: uint64(len(batch.Events)),
			Digest: sha256.Sum256(payload), Payload: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	operation := testSourceMutationID(2)
	after := PhysicalEntry{
		Root: "root", Relative: "settings.json", Exists: true, Kind: PhysicalFile,
		Identity: FileIdentity{VolumeUUID: "volume", Inode: 8, BirthtimeSec: 1},
	}
	envelope, err := json.Marshal(mutationEnvelope{Start: 0})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := json.Marshal(durableMutationReceipt{
		Operation: operation, Origin: catalog.CausalOrigin{Cause: causal.CauseProviderMutation, Domain: "domain", Generation: 1},
		Start: 0, End: 3, Effects: []observedEffect{{
			Path: PathRef{Root: "root", Relative: "settings.json"}, After: physicalState(after),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		catalog: expectationPageStore{
			Store: store,
			record: catalog.SourceMutationExpectationRecord{
				Operation: operation, Authority: testAuthority, State: catalog.SourceMutationExpectationArmed,
				Payload: envelope, Receipt: receipt,
			},
		},
		authority: testAuthority,
	}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		{root: "root", relative: "settings.json"}: {Physical: after},
	}}
	partial, err := runtime.correlateMutationEcho(t.Context(), 1, batches[0], view)
	if err != nil {
		t.Fatal(err)
	}
	if len(partial.provider) != 0 || len(partial.external) != 0 {
		t.Fatalf("partial correlation = %+v", partial)
	}
	final, err := runtime.correlateMutationEcho(t.Context(), 3, batches[2], view)
	if err != nil {
		t.Fatal(err)
	}
	if len(final.provider) != 2 || final.provider[0].Stream != "stream-a" || final.provider[1].RootEpoch != "epoch-a" ||
		final.provider[0].Predecessor != 0 || final.provider[1].Cursor != 2 || len(final.provider[0].Events) != 1 || len(final.provider[1].Events) != 1 ||
		final.provider[0].Events[0].Ordinal != 1 || final.provider[1].Events[0].Ordinal != 4 ||
		len(final.external) != 1 || final.external[0].Stream != "stream-b" || final.external[0].Events[0].Ordinal != 2 ||
		len(final.matched) != 1 || final.origin == nil || final.origin.Cause != causal.CauseProviderMutation {
		t.Fatalf("final correlation = %+v", final)
	}
	view.entries[indexKey{root: "root", relative: "settings.json"}] = IndexedEntry{}
	mismatch, err := runtime.correlateMutationEcho(t.Context(), 3, batches[2], view)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatch.provider) != 0 || mismatch.origin != nil || len(mismatch.mismatched) != 1 ||
		len(mismatch.external) != 3 || mismatch.external[0].Stream != "stream-a" || mismatch.external[1].Stream != "stream-b" ||
		mismatch.external[2].Cursor != 2 || mismatch.external[0].Events[0].Ordinal != 1 ||
		mismatch.external[1].Events[0].Ordinal != 2 || mismatch.external[2].Events[0].Ordinal != 4 {
		t.Fatalf("mismatched correlation = %+v", mismatch)
	}
}

func TestCausalPartitionMakesMixedInputsExternalAndIsDeterministic(t *testing.T) {
	providerPath := PathRef{Root: "root", Relative: "provider.json"}
	externalPath := PathRef{Root: "root", Relative: "external.json"}
	external := DeltaPlan{
		AffectedKeys: []causal.LogicalKey{"external"},
		Reads: []MaterializationRequest{
			{Logical: "z-external", Inputs: []PathRef{externalPath}},
		},
		Deletes: []Delete{{Logical: "z-delete"}, {Logical: "a-delete"}},
	}
	provider := DeltaPlan{
		AffectedKeys: []causal.LogicalKey{"provider"},
		Reads: []MaterializationRequest{
			{Logical: "z-provider", Inputs: []PathRef{providerPath}},
			{Logical: "a-mixed", Inputs: []PathRef{providerPath, externalPath}},
		},
		Deletes: []Delete{{Logical: "m-provider-delete"}},
	}
	externalPaths := map[indexKey]struct{}{{root: "root", relative: "external.json"}: {}}
	providerPaths := map[indexKey]struct{}{{root: "root", relative: "provider.json"}: {}}

	wantExternal, wantProvider, err := partitionCausalPlans(external, provider, externalPaths, providerPaths)
	if err != nil {
		t.Fatal(err)
	}
	if got := []LogicalID{wantExternal.Reads[0].Logical, wantExternal.Reads[1].Logical}; !slices.Equal(got, []LogicalID{"a-mixed", "z-external"}) {
		t.Fatalf("external reads = %v", got)
	}
	if len(wantProvider.Reads) != 1 || wantProvider.Reads[0].Logical != "z-provider" {
		t.Fatalf("provider reads = %+v", wantProvider.Reads)
	}
	if !slices.Equal(wantExternal.AffectedKeys, []causal.LogicalKey{"external", "provider"}) {
		t.Fatalf("external affected keys = %v", wantExternal.AffectedKeys)
	}
	if got := []LogicalID{wantExternal.Deletes[0].Logical, wantExternal.Deletes[1].Logical}; !slices.Equal(got, []LogicalID{"a-delete", "z-delete"}) {
		t.Fatalf("external deletes = %v", got)
	}
	for range 100 {
		gotExternal, gotProvider, err := partitionCausalPlans(external, provider, externalPaths, providerPaths)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(gotExternal, wantExternal) || !reflect.DeepEqual(gotProvider, wantProvider) {
			t.Fatalf("causal partition was not repeatable: external=%+v provider=%+v", gotExternal, gotProvider)
		}
	}
}

func TestPublishEmptyFleetAbortsSnapshotStageBeforePublicationHandoff(t *testing.T) {
	runtime, store, _, _ := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	runtime.Cancel()
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	state := loadSourceObserverFenceForTest(t, store, testAuthority)
	if err := store.BeginSourcePublicationStage(t.Context(), catalog.SourcePublicationStageIdentity{
		Authority: testAuthority, FleetOwner: runtime.fleetOwner,
		FleetGeneration: runtime.fleetGeneration, DriverID: runtime.driverID,
		DeclarationDigest: runtime.declarationDigest, Operation: causal.OperationID{9},
		Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
		Through: state.Stream.LastApplied, Predecessor: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.publishEmptyFleet(t.Context()); !errors.Is(err, catalog.ErrSourceObserverConflict) {
		t.Fatalf("publishEmptyFleet = %v, want publication stage conflict", err)
	}
	if err := store.AbortSourcePublicationStage(t.Context(), testAuthority, causal.OperationID{9}); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginSourceSnapshotStage(t.Context(), testAuthority, "stage-after-conflict"); err != nil {
		t.Fatalf("snapshot stage leaked before publication handoff: %v", err)
	}
	if err := store.AbortSourceSnapshotStage(t.Context(), testAuthority, "stage-after-conflict"); err != nil {
		t.Fatal(err)
	}
}

func TestInboxOverflowSignalsAutomaticExternalSnapshotRepairWithoutPayloadRegrowth(t *testing.T) {
	runtime, store, source, backend := testRuntime(t, 1, nil, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	operation := catalog.MutationID{9}
	origin := catalog.CausalOrigin{Cause: causal.CauseProviderMutation, Domain: "domain", Generation: 1}
	expectation := mutationEnvelopeForRuntimeTest(t, operation, 1, origin)
	if err := store.PutSourceMutationExpectation(t.Context(), catalog.SourceMutationExpectationRecord{
		Operation: operation, Authority: testAuthority, Tenant: "tenant", Generation: 1,
		Origin: origin,
		Digest: sha256.Sum256(expectation), Payload: expectation,
	}); err != nil {
		t.Fatal(err)
	}
	repairReceipt, err := json.Marshal(durableMutationReceipt{
		Authority: testAuthority, AuthorityGeneration: 1, Operation: operation, Origin: origin, Digest: Fingerprint{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteSourceMutationExpectation(t.Context(), testAuthority, operation, repairReceipt); err != nil {
		t.Fatal(err)
	}
	runtime.executor.(*fakeExecutor).inspectMutation = func(
		_ context.Context,
		request MutationInspectionRequest,
	) (MutationInspection, error) {
		return MutationInspection{
			State: MutationInspectionTerminal, ExpectationDigest: request.ExpectationDigest, Intent: Fingerprint{1},
			Terminal: &MutationTerminalProof{
				Authority: request.Authority, AuthorityGeneration: request.AuthorityGeneration,
				Operation: request.Operation, Outcome: MutationAcknowledged, Digest: Fingerprint{1},
			},
		}, nil
	}
	policy := runtime.policy.(*fakePolicy)
	policy.deltaStarted = make(chan struct{})
	policy.deltaRelease = make(chan struct{})
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	source.put("root", "file-0000.json", 10, []byte("first"))
	if err := stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch, Predecessor: checkpoint.Cursor, Cursor: checkpoint.Cursor + 1,
		Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: checkpoint.Cursor + 1}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-policy.deltaStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("delta did not block before overflow")
	}
	seedBatch := EventBatch{
		Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch, Predecessor: checkpoint.Cursor + 1, Cursor: checkpoint.Cursor + 2,
		Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: checkpoint.Cursor + 2}},
	}
	seedPayload, err := json.Marshal(seedBatch)
	if err != nil {
		t.Fatal(err)
	}
	const inboxEventLimit = 1 << 20
	if _, err := store.AppendSourceObserverInbox(t.Context(), catalog.SourceObserverInboxRecord{
		Authority: testAuthority, Stream: string(seedBatch.Stream), RootEpoch: string(seedBatch.RootEpoch),
		NativePredecessor: uint64(seedBatch.Predecessor), NativeCursor: uint64(seedBatch.Cursor),
		EventCount: inboxEventLimit - 1, Digest: sha256.Sum256(seedPayload), Payload: seedPayload,
	}); err != nil {
		t.Fatal(err)
	}
	stream.mu.Lock()
	stream.checkpoints[0].Cursor = seedBatch.Cursor
	stream.mu.Unlock()
	source.put("root", "file-0000.json", 10, []byte("overflow"))
	if err := stream.emit(t.Context(), EventBatch{
		Stream: seedBatch.Stream, RootEpoch: seedBatch.RootEpoch, Predecessor: seedBatch.Cursor, Cursor: seedBatch.Cursor + 1,
		Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: seedBatch.Cursor + 1}},
	}); err != nil {
		t.Fatalf("durably coalesced overflow stopped the stream: %v", err)
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil || state.Stream.Mode != catalog.SourceObserverSnapshotRequired || len(state.Inbox) != 0 {
		t.Fatalf("coalesced overflow state = %+v, %v", state, err)
	}
	close(policy.deltaRelease)
	barrier, err := runtime.Barrier(t.Context(), "tenant", 1)
	if err != nil {
		t.Fatal(err)
	}
	expectations := sourceMutationExpectationsForTest(t, store)
	if len(expectations) != 0 {
		t.Fatalf("snapshot repair retained overflow expectation = %+v", expectations)
	}
	if barrier.Source.Cause != causal.CauseExternalUnattributed {
		t.Fatalf("overflow repair cause = %q, want external_unattributed", barrier.Source.Cause)
	}
}

func sourceMutationExpectationsForTest(
	t *testing.T,
	store *catalog.Catalog,
) []catalog.SourceMutationExpectationRecord {
	t.Helper()
	var result []catalog.SourceMutationExpectationRecord
	var after catalog.MutationID
	for {
		page, err := store.SourceMutationExpectationsPage(
			t.Context(), testAuthority, after, catalog.SourceMutationExpectationPageLimit,
		)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, page.Records...)
		if page.Next == (catalog.MutationID{}) {
			return result
		}
		if bytes.Compare(page.Next[:], after[:]) <= 0 {
			t.Fatalf("non-monotonic expectation page = %x after %x", page.Next, after)
		}
		after = page.Next
	}
}

func testSourceMutationID(seed byte) catalog.MutationID {
	var id catalog.MutationID
	id[7] = 1
	id[8] = seed
	return id
}

func mutationEnvelopeForRuntimeTest(
	t *testing.T,
	operation catalog.MutationID,
	authorityGeneration causal.Generation,
	origin catalog.CausalOrigin,
) []byte {
	t.Helper()
	step := tenant.SourceMutationStep{
		TenantID: "tenant", Generation: 1, OperationID: operation,
		SourceID: string(testAuthority), ExpectedHead: 1, Origin: origin,
	}
	payload, err := json.Marshal(mutationEnvelope{
		Request:   MutationRequest{Step: step},
		Operation: tenant.SourceMutationOperation{OperationID: operation, SourceID: string(testAuthority)},
		Fence:     Fence{Authority: testAuthority, AuthorityGeneration: authorityGeneration},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestPublishedSnapshotRepairRetainsJournalCleanupProofAcrossRestart(t *testing.T) {
	for _, receiptComplete := range []bool{false, true} {
		name := "incomplete-journal"
		if receiptComplete {
			name = "receipt-journal"
		}
		t.Run(name, func(t *testing.T) {
			catalogPath := path.Join(t.TempDir(), "catalog.sqlite")
			store, err := catalog.Open(t.Context(), catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			configureSourceObserverForTest(t, store, observerConfiguration{
				Authority: testAuthority, Stream: "stream", RootEpoch: "epoch",
				RootDigest: [32]byte{1}, FleetDigest: [32]byte{2},
				Roots: []catalog.SourceObserverRootRecord{{
					ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume", Inode: 1, Kind: uint8(RootDirectory),
				}},
				Checkpoints: []catalog.SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}},
			})
			operation := catalog.MutationID{17}
			const originalGeneration causal.Generation = 3
			origin := catalog.CausalOrigin{Cause: causal.CauseProviderMutation, Domain: "domain", Generation: 1}
			expectation := mutationEnvelopeForRuntimeTest(t, operation, originalGeneration, origin)
			if err := store.PutSourceMutationExpectation(t.Context(), catalog.SourceMutationExpectationRecord{
				Operation: operation, Authority: testAuthority, Tenant: "tenant", Generation: 1,
				Origin: origin,
				Digest: sha256.Sum256(expectation), Payload: expectation,
			}); err != nil {
				t.Fatal(err)
			}
			if receiptComplete {
				receipt, err := json.Marshal(durableMutationReceipt{
					Authority: testAuthority, AuthorityGeneration: originalGeneration,
					Operation: operation, Origin: origin, Digest: Fingerprint{4},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.CompleteSourceMutationExpectation(t.Context(), testAuthority, operation, receipt); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.RequireSourceObserverSnapshot(t.Context(), testAuthority); err != nil {
				t.Fatal(err)
			}
			if err := store.BeginSourceSnapshotStage(t.Context(), testAuthority, "repair"); err != nil {
				t.Fatal(err)
			}
			repairOperation := causal.OperationID{9}
			repairRef := stageRepairSnapshotForTest(t, store, testAuthority, "repair", repairOperation)
			if _, err := store.PromoteSourceSnapshot(t.Context(), repairRef, catalog.SourceSnapshotSettlement{
				Fence: catalog.SourceObserverSettlement{
					Authority: testAuthority, Stream: "stream", RootEpoch: "epoch", Operation: repairOperation,
				},
				Snapshot: repairRef, MismatchAllActive: true,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = catalog.Open(t.Context(), catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			records := sourceMutationExpectationsForTest(t, store)
			if len(records) != 1 || records[0].State != catalog.SourceMutationExpectationRepairPublished {
				t.Fatalf("restart repair proof = %+v", records)
			}
			executor := &repairTrackingExecutor{
				fakeExecutor: &fakeExecutor{fakePathSource: newFakePathSource(0), fakeBackend: newFakeBackend()},
				failCleanup:  true,
			}
			executor.inspectMutation = func(
				_ context.Context,
				request MutationInspectionRequest,
			) (MutationInspection, error) {
				if !receiptComplete {
					return MutationInspection{
						State:             MutationInspectionActiveUnapplied,
						ExpectationDigest: request.ExpectationDigest, Intent: Fingerprint{1},
					}, nil
				}
				return MutationInspection{
					State:             MutationInspectionTerminal,
					ExpectationDigest: request.ExpectationDigest, Intent: Fingerprint{1},
					Terminal: &MutationTerminalProof{
						Authority: request.Authority, AuthorityGeneration: request.AuthorityGeneration,
						Operation: request.Operation, Outcome: MutationAcknowledged, Digest: Fingerprint{4},
					},
				}, nil
			}
			runtime := &Runtime{
				catalog: store, authority: testAuthority, fleetGeneration: 7, executor: executor,
			}
			if err := runtime.cleanupPublishedMutationRepairs(t.Context()); err == nil {
				t.Fatal("simulated cleanup crash was ignored")
			}
			records = sourceMutationExpectationsForTest(t, store)
			if len(records) != 1 || records[0].State != catalog.SourceMutationExpectationRepairPublished {
				t.Fatalf("cleanup crash lost durable proof = %+v", records)
			}
			if err := runtime.cleanupPublishedMutationRepairs(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := runtime.cleanupPublishedMutationRepairs(t.Context()); err != nil {
				t.Fatal(err)
			}
			records = sourceMutationExpectationsForTest(t, store)
			if len(records) != 0 {
				t.Fatalf("completed repair proof = %+v", records)
			}
			if receiptComplete {
				if len(executor.acks) != 2 || len(executor.abandons) != 0 {
					t.Fatalf("receipt cleanup calls = ack %v abandon %v", executor.acks, executor.abandons)
				}
				if !reflect.DeepEqual(executor.ackGenerations, []causal.Generation{originalGeneration, originalGeneration}) {
					t.Fatalf("receipt cleanup generations = %v", executor.ackGenerations)
				}
			} else if len(executor.abandons) != 2 || len(executor.acks) != 0 {
				t.Fatalf("incomplete cleanup calls = ack %v abandon %v", executor.acks, executor.abandons)
			} else if !reflect.DeepEqual(executor.abandonGenerations, []causal.Generation{originalGeneration, originalGeneration}) {
				t.Fatalf("incomplete cleanup generations = %v", executor.abandonGenerations)
			}
		})
	}
}

func TestBarrierUsesCapturedInboxSequenceWhileLaterEventsContinue(t *testing.T) {
	clock := newManualClock()
	runtime, store, source, backend := testRuntime(t, 1, clock, 0)
	if _, err := runtime.Barrier(t.Context(), "tenant", 1); err != nil {
		t.Fatal(err)
	}
	policy := runtime.policy.(*fakePolicy)
	policy.failDeltaAt = 2
	stream := backend.latest()
	checkpoint := stream.Checkpoints()[0]
	source.put("root", "file-0000.json", 10, []byte("one"))
	if err := stream.emit(t.Context(), EventBatch{
		Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch, Predecessor: 0, Cursor: 1,
		Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	captured := make(chan struct{})
	stream.mu.Lock()
	stream.flushHook = func(context.Context) error {
		close(captured)
		return nil
	}
	stream.mu.Unlock()
	barrier := make(chan error, 1)
	go func() {
		_, err := runtime.Barrier(context.Background(), "tenant", 1)
		barrier <- err
	}()
	select {
	case <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("Barrier did not capture its flush fence")
	}
	for cursor := EventID(2); cursor <= 10; cursor++ {
		source.put("root", "file-0000.json", 10, []byte(fmt.Sprintf("value-%d", cursor)))
		if err := stream.emit(t.Context(), EventBatch{
			Stream: checkpoint.Identity, RootEpoch: checkpoint.RootEpoch, Predecessor: cursor - 1, Cursor: cursor,
			Events: []PathEvent{{Root: "root", Relative: "file-0000.json", Kind: EventModified, ID: cursor}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-barrier:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Barrier starved behind later inbox arrivals")
	}
	state, err := loadSourceObserverControlForTest(t.Context(), store, testAuthority)
	if err != nil || state.Stream.LastApplied < 1 || state.Stream.LastReceived != 10 {
		t.Fatalf("captured barrier state = %+v, %v", state.Stream, err)
	}
}
