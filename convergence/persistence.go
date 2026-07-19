package convergence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// CatalogStateStore is the narrow normalized convergence state owner.
type CatalogStateStore interface {
	ConvergenceEngineHead(context.Context) (catalog.ConvergenceEngineHeader, error)
	PageConvergenceEngine(context.Context, catalog.ConvergenceEngineCursor) (catalog.ConvergenceEnginePage, error)
	StageConvergenceEngineMutation(context.Context, catalog.ConvergenceEngineStage) error
	PublishConvergenceEngineMutation(context.Context, catalog.ConvergenceEngineOperation) (catalog.ConvergenceEngineHeader, error)
	DiscardUnpublishedConvergenceEngineMutations(context.Context, uint64) error
	ClaimConvergenceOutbox(context.Context) (*causal.OutboxClaim, error)
	PageConvergenceOutbox(context.Context, causal.OutboxClaim) (causal.OutboxPage, error)
	SettleConvergenceOutbox(context.Context, causal.OutboxSettlement) error
}

// CatalogPersistence persists only changed normalized records through exact staged CAS.
type CatalogPersistence struct {
	store CatalogStateStore

	mu      sync.Mutex
	loaded  bool
	version uint64
	current State
}

// NewCatalogPersistence binds persistence to the required catalog state owner.
func NewCatalogPersistence(store CatalogStateStore) (*CatalogPersistence, error) {
	if store == nil {
		return nil, errors.New("convergence: catalog state store is nil")
	}
	return &CatalogPersistence{store: store}, nil
}

// ClaimOutbox durably claims the oldest catalog commit not yet applied to the engine.
func (p *CatalogPersistence) ClaimOutbox(ctx context.Context) (*causal.OutboxClaim, error) {
	return p.store.ClaimConvergenceOutbox(ctx)
}

// PageOutbox reads one fixed-size, exact page of a durable claim.
func (p *CatalogPersistence) PageOutbox(ctx context.Context, claim causal.OutboxClaim) (causal.OutboxPage, error) {
	return p.store.PageConvergenceOutbox(ctx, claim)
}

// SettleOutbox retires one exact terminal claim after its causal state is durable.
func (p *CatalogPersistence) SettleOutbox(ctx context.Context, settlement causal.OutboxSettlement) error {
	return p.store.SettleConvergenceOutbox(ctx, settlement)
}

// Load reconstructs normalized state through fixed-size pages.
func (p *CatalogPersistence) Load(ctx context.Context) (State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loaded {
		return cloneState(p.current), nil
	}
	state := emptyState()
	cursor := catalog.ConvergenceEngineCursor{}
	var version uint64
	pending := make(map[DomainID]catalog.ConvergenceEngineDomain)
	seen := make(map[catalog.ConvergenceEngineCursor]struct{})
	for {
		if _, duplicate := seen[cursor]; duplicate {
			return State{}, errors.New("convergence: normalized state cursor cycled")
		}
		seen[cursor] = struct{}{}
		page, err := p.store.PageConvergenceEngine(ctx, cursor)
		if err != nil {
			return State{}, fmt.Errorf("convergence: page normalized state: %w", err)
		}
		if len(seen) == 1 {
			version = page.Header.Version
			state.Revision = Revision(page.Header.Revision)
		} else if page.Header.Version != version || Revision(page.Header.Revision) != state.Revision {
			return State{}, errors.New("convergence: normalized state changed during recovery")
		}
		for _, head := range page.Heads {
			state.SourceHeads[SourceAuthorityID(head.Authority)] = Revision(head.Head)
			state.DedupFloors[SourceAuthorityID(head.Authority)] = Revision(head.DedupFloor)
		}
		for _, record := range page.Domains {
			domain := domainFromRecord(record)
			if _, duplicate := state.Domains[domain.Domain]; duplicate {
				return State{}, fmt.Errorf("convergence: duplicate normalized domain %q", domain.Domain)
			}
			state.Domains[domain.Domain] = domain
			pending[domain.Domain] = record
		}
		for _, record := range page.Changes {
			id := ChangeID(record.Change.ChangeID)
			if _, duplicate := state.Changes[id]; duplicate {
				return State{}, fmt.Errorf("convergence: duplicate normalized change %x", id)
			}
			state.Changes[id] = AppliedChange{
				Change: cloneChange(record.Change), EngineRevision: Revision(record.EngineRevision),
				AffectedCount: record.AffectedCount, AffectedDigest: record.AffectedDigest,
			}
		}
		if page.Outbox != nil {
			if state.Outbox != nil {
				return State{}, errors.New("convergence: duplicate normalized outbox")
			}
			state.Outbox = &OutboxProgress{
				Change: cloneChange(page.Outbox.Change), Cursor: page.Outbox.Cursor,
				EngineRevision: Revision(page.Outbox.EngineRevision),
				CommitCount:    page.Outbox.CommitCount,
				AffectedCount:  page.Outbox.AffectedCount,
				AffectedDigest: page.Outbox.AffectedDigest,
			}
			if page.Outbox.Settlement != nil {
				settlement := *page.Outbox.Settlement
				state.Outbox.Settlement = &settlement
			}
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	for id, record := range pending {
		domain := state.Domains[id]
		if !record.PendingSent.IsZero() {
			domain.Pending = &Pending{
				Notification: notificationAt(state, domain, domain.Notified, domain.NotifiedCatalogRevision, domain.NotifiedChange),
				SentAt:       record.PendingSent,
			}
		}
		if !record.QuarantineSince.IsZero() {
			domain.Quarantine = &Quarantine{
				Notification: notificationAt(state, domain, domain.Notified, domain.NotifiedCatalogRevision, domain.NotifiedChange),
				Since:        record.QuarantineSince, Until: record.QuarantineUntil,
			}
		}
		state.Domains[id] = domain
	}
	if err := p.store.DiscardUnpublishedConvergenceEngineMutations(ctx, version); err != nil {
		return State{}, fmt.Errorf("convergence: discard abandoned normalized state staging: %w", err)
	}
	p.loaded, p.version, p.current = true, version, cloneState(state)
	return cloneState(state), nil
}

// Save stages bounded normalized deltas and publishes them under one exact CAS.
func (p *CatalogPersistence) Save(ctx context.Context, state State) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.loaded {
		return errors.New("convergence: normalized state must be loaded before save")
	}
	pages := convergenceDeltaPages(p.current, state)
	operation, err := catalog.NewConvergenceEngineOperation()
	if err != nil {
		return err
	}
	for index, page := range pages {
		stage := catalog.ConvergenceEngineStage{
			Operation: operation, ExpectedVersion: p.version, TargetRevision: causal.Revision(state.Revision),
			Sequence: uint32(index), PageCount: uint32(len(pages)), Page: page,
		}
		if err := retryNormalizedStage(ctx, p.store, stage); err != nil {
			return fmt.Errorf("convergence: stage normalized state page %d: %w", index, err)
		}
	}
	header, err := retryNormalizedPublish(ctx, p.store, operation)
	if err != nil {
		return fmt.Errorf("convergence: publish normalized state: %w", err)
	}
	if header.Version != p.version+1 || Revision(header.Revision) != state.Revision {
		return errors.New("convergence: normalized state publish returned the wrong fence")
	}
	p.version, p.current = header.Version, cloneState(state)
	return nil
}

func retryNormalizedStage(
	ctx context.Context,
	store CatalogStateStore,
	stage catalog.ConvergenceEngineStage,
) error {
	err := store.StageConvergenceEngineMutation(ctx, stage)
	if err == nil || ctx.Err() != nil {
		return err
	}
	return store.StageConvergenceEngineMutation(ctx, stage)
}

func retryNormalizedPublish(
	ctx context.Context,
	store CatalogStateStore,
	operation catalog.ConvergenceEngineOperation,
) (catalog.ConvergenceEngineHeader, error) {
	header, err := store.PublishConvergenceEngineMutation(ctx, operation)
	if err == nil || ctx.Err() != nil {
		return header, err
	}
	return store.PublishConvergenceEngineMutation(ctx, operation)
}

func emptyState() State {
	return State{
		SourceHeads: make(map[SourceAuthorityID]Revision),
		DedupFloors: make(map[SourceAuthorityID]Revision),
		Domains:     make(map[DomainID]DomainState),
		Changes:     make(map[ChangeID]AppliedChange),
	}
}

func domainFromRecord(record catalog.ConvergenceEngineDomain) DomainState {
	return DomainState{
		Tenant: TenantID(record.Tenant), Domain: DomainID(record.Domain), Generation: Generation(record.Generation),
		Fingerprint: Fingerprint(record.Fingerprint), Applicable: cloneChange(record.Applicable),
		CatalogRevision:         CatalogRevision(record.CatalogRevision),
		NotifiedCatalogRevision: CatalogRevision(record.NotifiedCatalogRevision),
		ObservedCatalogRevision: CatalogRevision(record.ObservedCatalogRevision),
		Desired:                 Revision(record.Desired), Notified: Revision(record.Notified), Observed: Revision(record.Observed),
		DesiredChange: cloneChange(record.DesiredChange), NotifiedChange: cloneChange(record.NotifiedChange),
		ObservedChange: cloneChange(record.ObservedChange), Demanded: record.Demanded, Forced: record.Forced,
	}
}

func domainRecord(domain DomainState) catalog.ConvergenceEngineDomain {
	return catalog.ConvergenceEngineDomain{
		Tenant: causal.TenantID(domain.Tenant), Domain: causal.DomainID(domain.Domain),
		Generation: causal.Generation(domain.Generation), Fingerprint: [32]byte(domain.Fingerprint),
		CatalogRevision:         causal.CatalogRevision(domain.CatalogRevision),
		NotifiedCatalogRevision: causal.CatalogRevision(domain.NotifiedCatalogRevision),
		ObservedCatalogRevision: causal.CatalogRevision(domain.ObservedCatalogRevision),
		Desired:                 causal.Revision(domain.Desired), Notified: causal.Revision(domain.Notified), Observed: causal.Revision(domain.Observed),
		Demanded: domain.Demanded, Forced: domain.Forced,
		Applicable:     cloneCausalChange(changeHeader(domain.Applicable)),
		DesiredChange:  cloneCausalChange(changeHeader(domain.DesiredChange)),
		NotifiedChange: cloneCausalChange(changeHeader(domain.NotifiedChange)),
		ObservedChange: cloneCausalChange(changeHeader(domain.ObservedChange)),
		PendingSent:    pendingSent(domain), QuarantineSince: quarantineSince(domain), QuarantineUntil: quarantineUntil(domain),
	}
}

func pendingSent(domain DomainState) (value time.Time) {
	if domain.Pending != nil {
		return domain.Pending.SentAt
	}
	return value
}

func quarantineSince(domain DomainState) (value time.Time) {
	if domain.Quarantine != nil {
		return domain.Quarantine.Since
	}
	return value
}

func quarantineUntil(domain DomainState) (value time.Time) {
	if domain.Quarantine != nil {
		return domain.Quarantine.Until
	}
	return value
}

func notificationAt(
	state State,
	domain DomainState,
	revision Revision,
	catalogRevision CatalogRevision,
	change ChangeSet,
) Notification {
	full := change
	if applied, ok := state.Changes[change.ChangeID]; ok {
		full = applied.Change
	}
	clone := domain
	clone.Desired, clone.CatalogRevision, clone.DesiredChange = revision, catalogRevision, full
	return notificationFor(state, clone)
}

func convergenceDeltaPages(before, after State) []catalog.ConvergenceEngineDeltaPage {
	builder := deltaPageBuilder{pages: []catalog.ConvergenceEngineDeltaPage{{}}}
	headKeys := unionSortedKeys(before.SourceHeads, after.SourceHeads)
	for _, authority := range headKeys {
		head, exists := after.SourceHeads[authority]
		if !exists {
			builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
				page.DeleteHeads = append(page.DeleteHeads, causal.SourceAuthorityID(authority))
			})
			continue
		}
		if head != before.SourceHeads[authority] || after.DedupFloors[authority] != before.DedupFloors[authority] {
			record := catalog.ConvergenceEngineHead{
				Authority: causal.SourceAuthorityID(authority), Head: causal.Revision(head),
				DedupFloor: causal.Revision(after.DedupFloors[authority]),
			}
			builder.add(func(page *catalog.ConvergenceEngineDeltaPage) { page.UpsertHeads = append(page.UpsertHeads, record) })
		}
	}
	domainKeys := unionSortedKeys(before.Domains, after.Domains)
	for _, id := range domainKeys {
		domain, exists := after.Domains[id]
		if !exists {
			builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
				page.DeleteDomains = append(page.DeleteDomains, causal.DomainID(id))
			})
			continue
		}
		if !reflect.DeepEqual(domain, before.Domains[id]) {
			record := domainRecord(domain)
			builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
				page.UpsertDomains = append(page.UpsertDomains, record)
			})
		}
	}
	changeKeys := unionSortedChangeKeys(before.Changes, after.Changes)
	for _, id := range changeKeys {
		change, exists := after.Changes[id]
		if !exists {
			builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
				page.DeleteChanges = append(page.DeleteChanges, causal.ChangeID(id))
			})
			continue
		}
		if reflect.DeepEqual(change, before.Changes[id]) {
			continue
		}
		record := catalog.ConvergenceEngineChange{
			Change: cloneCausalChange(changeHeader(change.Change)), EngineRevision: causal.Revision(change.EngineRevision),
			AffectedCount: change.AffectedCount, AffectedDigest: change.AffectedDigest,
		}
		builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
			page.UpsertChanges = append(page.UpsertChanges, record)
		})
	}
	if !reflect.DeepEqual(before.Outbox, after.Outbox) {
		builder.add(func(page *catalog.ConvergenceEngineDeltaPage) {
			page.ResetOutbox = true
			if after.Outbox != nil {
				record := catalog.ConvergenceEngineOutbox{
					Change: cloneCausalChange(changeHeader(after.Outbox.Change)), Cursor: after.Outbox.Cursor,
					EngineRevision: causal.Revision(after.Outbox.EngineRevision),
					CommitCount:    after.Outbox.CommitCount,
					AffectedCount:  after.Outbox.AffectedCount,
					AffectedDigest: after.Outbox.AffectedDigest,
				}
				if after.Outbox.Settlement != nil {
					settlement := *after.Outbox.Settlement
					record.Settlement = &settlement
				}
				page.Outbox = &record
			}
		})
	}
	return builder.pages
}

type deltaPageBuilder struct {
	pages []catalog.ConvergenceEngineDeltaPage
	count int
}

func (b *deltaPageBuilder) add(appendRecord func(*catalog.ConvergenceEngineDeltaPage)) {
	if b.count == catalog.ConvergenceEnginePageLimit {
		b.pages = append(b.pages, catalog.ConvergenceEngineDeltaPage{})
		b.count = 0
	}
	appendRecord(&b.pages[len(b.pages)-1])
	b.count++
}

func unionSortedKeys[K ~string, V any](left, right map[K]V) []K {
	keys := make(map[K]struct{}, len(left)+len(right))
	for key := range left {
		keys[key] = struct{}{}
	}
	for key := range right {
		keys[key] = struct{}{}
	}
	result := make([]K, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	slices.Sort(result)
	return result
}

func unionSortedChangeKeys(left, right map[ChangeID]AppliedChange) []ChangeID {
	keys := make(map[ChangeID]struct{}, len(left)+len(right))
	for key := range left {
		keys[key] = struct{}{}
	}
	for key := range right {
		keys[key] = struct{}{}
	}
	result := make([]ChangeID, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	slices.SortFunc(result, func(a, b ChangeID) int { return slices.Compare(a[:], b[:]) })
	return result
}
