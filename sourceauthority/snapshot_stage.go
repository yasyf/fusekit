package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

const (
	SnapshotPlanOutputByteLimit           int64 = 1 << 30
	snapshotMaterializationBatchLimit           = 128
	snapshotMaterializationBatchByteLimit       = 128 << 10
)

type snapshotStageState struct {
	fleet       map[catalog.TenantID]catalog.Generation
	roots       map[catalog.TenantID]TenantRoot
	rootsLoaded map[catalog.TenantID]struct{}
	bindings    map[LogicalID]catalog.SourceAuthorityBindingRecord
}

func newSnapshotStageState() *snapshotStageState {
	return &snapshotStageState{
		fleet:       make(map[catalog.TenantID]catalog.Generation),
		roots:       make(map[catalog.TenantID]TenantRoot),
		rootsLoaded: make(map[catalog.TenantID]struct{}),
		bindings:    make(map[LogicalID]catalog.SourceAuthorityBindingRecord),
	}
}

func (s *snapshotStageState) initializeFleet(specs []tenant.TenantSpec) {
	if len(s.fleet) != 0 {
		return
	}
	for _, spec := range specs {
		s.fleet[spec.ID] = spec.Generation
	}
}

func (s *snapshotStageState) ensureBindings(
	ctx context.Context,
	runtime *Runtime,
	logicals []LogicalID,
) error {
	pending := make([]LogicalID, 0, len(logicals))
	seen := make(map[LogicalID]struct{}, len(logicals))
	for _, logical := range logicals {
		if _, loaded := s.bindings[logical]; loaded {
			continue
		}
		if _, duplicate := seen[logical]; duplicate {
			continue
		}
		seen[logical] = struct{}{}
		pending = append(pending, logical)
	}
	for start := 0; start < len(pending); start += maxSourceAuthorityLogicals {
		end := min(start+maxSourceAuthorityLogicals, len(pending))
		found, err := runtime.sourceAuthorityBindings(ctx, pending[start:end])
		if err != nil {
			return err
		}
		for _, logical := range pending[start:end] {
			binding, exists := found[logical]
			if !exists {
				binding = catalog.SourceAuthorityBindingRecord{
					Authority: runtime.authority, LogicalID: string(logical),
					SourceKey: snapshotSourceKey(runtime.authority, logical),
				}
			}
			s.bindings[logical] = binding
		}
	}
	return nil
}

func (s *snapshotStageState) ensureRoots(
	ctx context.Context,
	store Store,
	authority causal.SourceAuthorityID,
	snapshot string,
	ids []catalog.TenantID,
) error {
	pending := make([]catalog.TenantID, 0, len(ids))
	seen := make(map[catalog.TenantID]struct{}, len(ids))
	for _, id := range ids {
		if _, allowed := s.fleet[id]; !allowed {
			continue
		}
		if _, loaded := s.rootsLoaded[id]; loaded {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		pending = append(pending, id)
	}
	for start := 0; start < len(pending); start += catalog.SourceKeyedLookupLimit {
		end := min(start+catalog.SourceKeyedLookupLimit, len(pending))
		request, err := catalog.NewSourceSnapshotRootLookupRequest(
			authority, snapshot, uint32(start), pending[start:end],
		)
		if err != nil {
			return err
		}
		page, err := store.SourceSnapshotRootLookup(ctx, request)
		if err != nil {
			return err
		}
		if err := page.Validate(request); err != nil {
			return err
		}
		for _, entry := range page.Entries {
			s.rootsLoaded[entry.Tenant] = struct{}{}
			if entry.Root == nil {
				continue
			}
			generation := s.fleet[entry.Tenant]
			if entry.Root.Generation != generation {
				return fmt.Errorf(
					"%w: staged snapshot root generation left the tenant fleet", ErrInvalidPlan,
				)
			}
			s.roots[entry.Tenant] = TenantRoot{
				Tenant: entry.Root.Tenant, Generation: entry.Root.Generation,
				Logical: LogicalID(entry.Root.LogicalID),
			}
		}
	}
	return nil
}

func (s *snapshotStageState) rootForTenant(id catalog.TenantID) (TenantRoot, bool, error) {
	generation, allowed := s.fleet[id]
	if !allowed {
		return TenantRoot{}, false, nil
	}
	root, found := s.roots[id]
	if !found {
		return TenantRoot{}, false, nil
	}
	if root.Generation != generation {
		return TenantRoot{}, false, fmt.Errorf(
			"%w: staged snapshot root generation left the tenant fleet", ErrInvalidPlan,
		)
	}
	return root, true, nil
}

func (r *Runtime) stageSnapshotPlanPage(
	ctx context.Context,
	view snapshotView,
	identity catalog.SourceSnapshotIdentity,
	cursor SnapshotPlanCursor,
	plan SnapshotPlanPage,
	state *snapshotStageState,
) (result catalog.SourceSnapshotStageRef, resultErr error) {
	if state == nil {
		return catalog.SourceSnapshotStageRef{}, fmt.Errorf("%w: missing snapshot stage state", ErrInvalidPlan)
	}
	state.initializeFleet(view.tenants)
	if err := validateSnapshotPlanPage(ctx, plan, view, cursor); err != nil {
		return catalog.SourceSnapshotStageRef{}, err
	}
	page := catalog.SourceSnapshotPublicationPage{
		Cursor: string(cursor), Next: string(plan.Next),
		AffectedKeys: append([]causal.LogicalKey(nil), plan.AffectedKeys...),
		Roots:        make([]catalog.SourceSnapshotRoot, 0, len(plan.Roots)),
		Bindings:     make([]catalog.SourceSnapshotBinding, 0, len(plan.Reads)),
	}
	var staged []catalog.ContentRef
	owned := true
	defer func() {
		if owned && len(staged) != 0 {
			resultErr = errors.Join(resultErr, r.releaseUnclaimedContent(ctx, staged))
		}
	}()
	initialLogicals := make([]LogicalID, 0, len(plan.Roots)+len(plan.Reads))
	for _, root := range plan.Roots {
		initialLogicals = append(initialLogicals, root.Logical)
	}
	for _, request := range plan.Reads {
		initialLogicals = append(initialLogicals, request.Logical)
	}
	if err := state.ensureBindings(ctx, r, initialLogicals); err != nil {
		return catalog.SourceSnapshotStageRef{}, err
	}
	ensureBinding := func(logical LogicalID) (catalog.SourceAuthorityBindingRecord, error) {
		binding, found := state.bindings[logical]
		if !found {
			return catalog.SourceAuthorityBindingRecord{}, fmt.Errorf(
				"%w: source binding was not batch-loaded", ErrInvalidPlan,
			)
		}
		return binding, nil
	}
	for _, root := range plan.Roots {
		binding, err := ensureBinding(root.Logical)
		if err != nil {
			return catalog.SourceSnapshotStageRef{}, err
		}
		page.Roots = append(page.Roots, catalog.SourceSnapshotRoot{
			Tenant: root.Tenant, Generation: root.Generation, LogicalID: string(root.Logical), RootKey: binding.SourceKey,
		})
		if existing, found := state.roots[root.Tenant]; found &&
			(existing.Generation != root.Generation || existing.Logical != root.Logical) {
			return catalog.SourceSnapshotStageRef{}, fmt.Errorf(
				"%w: snapshot tenant root changed across pages", ErrInvalidPlan,
			)
		}
		state.roots[root.Tenant] = root
		state.rootsLoaded[root.Tenant] = struct{}{}
	}
	stagedByContent := make(map[struct {
		hash catalog.ContentHash
		size int64
	}]catalog.ContentRef)
	var outputBytes int64
	for start := 0; start < len(plan.Reads); {
		end, err := snapshotMaterializationBatchEnd(plan.Reads, start)
		if err != nil {
			return catalog.SourceSnapshotStageRef{}, err
		}
		requests := plan.Reads[start:end]
		values, err := r.materializeSnapshot(ctx, view, requests)
		if err != nil {
			return catalog.SourceSnapshotStageRef{}, err
		}
		if len(values) != len(requests) {
			return catalog.SourceSnapshotStageRef{}, errors.Join(
				fmt.Errorf("%w: snapshot materializer returned the wrong page cardinality", ErrInvalidPlan),
				closeMaterializations(values),
			)
		}
		materializedLogicals := make([]LogicalID, 0, len(values))
		materializedTenants := make([]catalog.TenantID, 0, len(values))
		for _, value := range values {
			materializedLogicals = append(materializedLogicals, value.Logical)
			for _, projection := range value.Objects {
				materializedLogicals = append(materializedLogicals, projection.Parent)
				materializedTenants = append(materializedTenants, projection.Tenant)
			}
		}
		if err := state.ensureBindings(ctx, r, materializedLogicals); err != nil {
			return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
		}
		if err := state.ensureRoots(
			ctx, r.catalog, r.authority, view.snapshot, materializedTenants,
		); err != nil {
			return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
		}
		for index, value := range values {
			request := requests[index]
			binding, err := ensureBinding(value.Logical)
			if err != nil {
				return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
			}
			inputs := make([]catalog.SourceIndexLocator, len(request.Inputs))
			for index, input := range request.Inputs {
				inputs[index] = catalog.SourceIndexLocator{RootID: string(input.Root), Relative: input.Relative}
			}
			page.Bindings = append(page.Bindings, catalog.SourceSnapshotBinding{
				LogicalID: string(value.Logical), SourceKey: binding.SourceKey,
				Fingerprint: value.Fingerprint, Inputs: inputs,
			})
			for _, projection := range value.Objects {
				root, found, rootErr := state.rootForTenant(projection.Tenant)
				if rootErr != nil {
					return catalog.SourceSnapshotStageRef{}, errors.Join(rootErr, closeMaterializations(values))
				}
				if !found || root.Generation != projection.Generation {
					return catalog.SourceSnapshotStageRef{}, errors.Join(
						fmt.Errorf("%w: snapshot projection escaped the tenant fleet", ErrInvalidPlan),
						closeMaterializations(values),
					)
				}
				object := catalog.SourceObject{
					Key: binding.SourceKey, Name: projection.Name, Kind: projection.Kind, Mode: projection.Mode,
					LinkTarget: projection.LinkTarget, Visibility: projection.Visibility,
				}
				if projection.Parent != root.Logical {
					parent, err := ensureBinding(projection.Parent)
					if err != nil {
						return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
					}
					object.Parent = parent.SourceKey
				}
				switch projection.Kind {
				case catalog.KindFile:
					if projection.Content == nil || projection.LinkTarget != "" {
						return catalog.SourceSnapshotStageRef{}, errors.Join(
							fmt.Errorf("%w: file projection has invalid content", ErrInvalidPlan),
							closeMaterializations(values),
						)
					}
					reader, err := projection.Content.Open(ctx)
					if err != nil {
						return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
					}
					if reader == nil {
						return catalog.SourceSnapshotStageRef{}, errors.Join(
							fmt.Errorf("%w: content source returned a nil stream", ErrInvalidPlan),
							closeMaterializations(values),
						)
					}
					ref, stageErr := r.stageContent(ctx, reader)
					if stageErr == nil {
						staged = append(staged, ref)
					}
					if stageErr != nil {
						return catalog.SourceSnapshotStageRef{}, errors.Join(stageErr, closeMaterializations(values))
					}
					outputBytes += ref.Size
					if outputBytes < 0 || outputBytes > SnapshotPlanOutputByteLimit {
						return catalog.SourceSnapshotStageRef{}, errors.Join(
							fmt.Errorf("%w: snapshot page exceeds the output byte limit", ErrInvalidPlan),
							closeMaterializations(values),
						)
					}
					contentKey := struct {
						hash catalog.ContentHash
						size int64
					}{hash: ref.Hash, size: ref.Size}
					if shared, found := stagedByContent[contentKey]; found {
						if err := r.releaseUnclaimedContent(ctx, []catalog.ContentRef{ref}); err != nil {
							return catalog.SourceSnapshotStageRef{}, errors.Join(err, closeMaterializations(values))
						}
						ref = shared
					} else {
						stagedByContent[contentKey] = ref
					}
					object.ContentRevision = catalog.Revision(identity.Change.SourceRevision)
					object.Content = ref
				case catalog.KindSymlink:
					if projection.Content != nil || projection.LinkTarget == "" {
						return catalog.SourceSnapshotStageRef{}, errors.Join(
							fmt.Errorf("%w: symlink projection has invalid content", ErrInvalidPlan),
							closeMaterializations(values),
						)
					}
					object.ContentRevision = catalog.Revision(identity.Change.SourceRevision)
				case catalog.KindDirectory:
					if projection.Content != nil || projection.LinkTarget != "" {
						return catalog.SourceSnapshotStageRef{}, errors.Join(
							fmt.Errorf("%w: directory projection has invalid content", ErrInvalidPlan),
							closeMaterializations(values),
						)
					}
				default:
					return catalog.SourceSnapshotStageRef{}, errors.Join(
						fmt.Errorf("%w: invalid snapshot projection kind", ErrInvalidPlan),
						closeMaterializations(values),
					)
				}
				page.Objects = append(page.Objects, catalog.SourceSnapshotProjection{
					Tenant: projection.Tenant, Generation: projection.Generation,
					LogicalID: string(value.Logical), Object: object,
				})
				if len(page.Objects) > SnapshotPlanObjectLimit {
					return catalog.SourceSnapshotStageRef{}, errors.Join(
						fmt.Errorf("%w: snapshot page exceeds the object limit", ErrInvalidPlan),
						closeMaterializations(values),
					)
				}
			}
		}
		if err := closeMaterializations(values); err != nil {
			return catalog.SourceSnapshotStageRef{}, err
		}
		start = end
	}
	result, err := r.catalog.AppendSourceSnapshotPublication(ctx, identity, page)
	if err != nil {
		return catalog.SourceSnapshotStageRef{}, err
	}
	owned = false
	return result, nil
}

func snapshotMaterializationBatchEnd(
	requests []MaterializationRequest,
	start int,
) (int, error) {
	bytes := 0
	end := start
	for end < len(requests) && end-start < snapshotMaterializationBatchLimit {
		encoded, err := json.Marshal(requests[end])
		if err != nil {
			return 0, err
		}
		if len(encoded) > snapshotMaterializationBatchByteLimit {
			return 0, fmt.Errorf(
				"%w: snapshot materialization request exceeds task-page byte limit", ErrInvalidPlan,
			)
		}
		if end > start && bytes+len(encoded) > snapshotMaterializationBatchByteLimit {
			break
		}
		bytes += len(encoded)
		end++
	}
	if end == start {
		return 0, fmt.Errorf("%w: empty snapshot materialization task page", ErrInvalidPlan)
	}
	return end, nil
}

func snapshotSourceKey(authority causal.SourceAuthorityID, logical LogicalID) catalog.SourceObjectKey {
	digest := sha256.Sum256([]byte("fusekit.source.snapshot.key-v1\x00" + string(authority) + "\x00" + string(logical)))
	return catalog.SourceObjectKey(fmt.Sprintf("snapshot-%x", digest[:16]))
}
