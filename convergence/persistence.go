package convergence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/yasyf/fusekit/causal"
)

const durableStateSchema = 4

// CatalogStateStore is the catalog-owned opaque convergence state slot.
type CatalogStateStore interface {
	LoadConvergenceState(context.Context) ([]byte, error)
	SaveConvergenceState(context.Context, []byte) error
	ClaimConvergenceOutbox(context.Context) (*causal.OutboxBatch, error)
	SettleConvergenceOutbox(context.Context, causal.ChangeID) error
}

// ClaimOutbox durably claims the oldest catalog commit not yet applied to the engine.
func (p *CatalogPersistence) ClaimOutbox(ctx context.Context) (*causal.OutboxBatch, error) {
	return p.store.ClaimConvergenceOutbox(ctx)
}

// SettleOutbox retires one catalog commit after its causal state is durable.
func (p *CatalogPersistence) SettleOutbox(ctx context.Context, change causal.ChangeID) error {
	return p.store.SettleConvergenceOutbox(ctx, change)
}

// CatalogPersistence serializes convergence state through the catalog's sole WAL owner.
type CatalogPersistence struct {
	store CatalogStateStore
}

// NewCatalogPersistence binds persistence to the required catalog state owner.
func NewCatalogPersistence(store CatalogStateStore) (*CatalogPersistence, error) {
	if store == nil {
		return nil, errors.New("convergence: catalog state store is nil")
	}
	return &CatalogPersistence{store: store}, nil
}

// Load returns the last complete durable state or an empty state on first use.
func (p *CatalogPersistence) Load(ctx context.Context) (State, error) {
	payload, err := p.store.LoadConvergenceState(ctx)
	if err != nil {
		return State{}, err
	}
	if len(payload) == 0 {
		return State{}, nil
	}
	return decodeDurableState(payload)
}

// Save atomically replaces the complete durable convergence state.
func (p *CatalogPersistence) Save(ctx context.Context, state State) error {
	payload, err := encodeDurableState(state)
	if err != nil {
		return err
	}
	return p.store.SaveConvergenceState(ctx, payload)
}

type durableState struct {
	SchemaVersion int                            `json:"schema_version"`
	Revision      Revision                       `json:"revision"`
	SourceHeads   map[SourceAuthorityID]Revision `json:"source_heads"`
	DedupFloors   map[SourceAuthorityID]Revision `json:"dedup_floors"`
	Domains       []DomainState                  `json:"domains"`
	Changes       []AppliedChange                `json:"changes"`
}

func encodeDurableState(state State) ([]byte, error) {
	domains := make([]DomainState, 0, len(state.Domains))
	for _, domain := range state.Domains {
		domains = append(domains, cloneState(State{Domains: map[DomainID]DomainState{domain.Domain: domain}}).Domains[domain.Domain])
	}
	slices.SortFunc(domains, func(a, b DomainState) int { return compareString(string(a.Domain), string(b.Domain)) })
	changes := make([]AppliedChange, 0, len(state.Changes))
	for _, applied := range state.Changes {
		applied.Change = cloneChange(applied.Change)
		changes = append(changes, applied)
	}
	slices.SortFunc(changes, func(a, b AppliedChange) int {
		return slices.Compare(a.Change.ChangeID[:], b.Change.ChangeID[:])
	})
	payload, err := json.Marshal(durableState{
		SchemaVersion: durableStateSchema,
		Revision:      state.Revision, SourceHeads: state.SourceHeads, DedupFloors: state.DedupFloors,
		Domains: domains, Changes: changes,
	})
	if err != nil {
		return nil, fmt.Errorf("convergence: encode durable state: %w", err)
	}
	return payload, nil
}

func decodeDurableState(payload []byte) (State, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var encoded durableState
	if err := decoder.Decode(&encoded); err != nil {
		return State{}, fmt.Errorf("convergence: decode durable state: %w", err)
	}
	if err := requireDurableJSONEOF(decoder); err != nil {
		return State{}, err
	}
	if encoded.SchemaVersion != durableStateSchema {
		return State{}, fmt.Errorf("convergence: payload schema %d, want %d", encoded.SchemaVersion, durableStateSchema)
	}
	state := State{
		Revision:    encoded.Revision,
		SourceHeads: make(map[SourceAuthorityID]Revision, len(encoded.SourceHeads)),
		DedupFloors: make(map[SourceAuthorityID]Revision, len(encoded.DedupFloors)),
		Domains:     make(map[DomainID]DomainState, len(encoded.Domains)),
		Changes:     make(map[ChangeID]AppliedChange, len(encoded.Changes)),
	}
	for authority, head := range encoded.SourceHeads {
		state.SourceHeads[authority] = head
	}
	for authority, floor := range encoded.DedupFloors {
		state.DedupFloors[authority] = floor
	}
	for _, domain := range encoded.Domains {
		if _, exists := state.Domains[domain.Domain]; exists {
			return State{}, fmt.Errorf("convergence: duplicate durable domain %q", domain.Domain)
		}
		state.Domains[domain.Domain] = domain
	}
	for _, applied := range encoded.Changes {
		if _, exists := state.Changes[applied.Change.ChangeID]; exists {
			return State{}, fmt.Errorf("convergence: duplicate durable change %x", applied.Change.ChangeID)
		}
		state.Changes[applied.Change.ChangeID] = applied
	}
	return cloneState(state), nil
}

func requireDurableJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("convergence: durable state has trailing JSON")
		}
		return fmt.Errorf("convergence: decode durable state trailer: %w", err)
	}
	return nil
}
