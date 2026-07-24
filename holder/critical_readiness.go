package holder

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

type criticalMaterializationScheduler interface {
	ScheduleCriticalMaterialization(context.Context, catalogproto.CriticalReadinessProof) ([]catalogproto.CriticalMaterializationPath, error)
}

type criticalFetchKey struct {
	tenant          catalog.TenantID
	domain          catalogproto.DomainID
	generation      uint64
	object          catalogproto.ObjectID
	objectRevision  uint64
	contentRevision uint64
	size            uint64
	hash            string
}

type criticalReadinessWorkKey struct {
	tenant               catalogproto.TenantID
	domain               catalogproto.DomainID
	generation           uint64
	root                 catalogproto.ObjectID
	presentationInstance catalogproto.PresentationInstanceID
	activationGeneration string
	policyDigest         string
	resolutionDigest     string
	catalogHead          uint64
	sourceRevision       uint64
	sourceAuthority      catalogproto.SourceAuthorityID
	sourcePublication    catalogproto.OperationID
	objectsDigest        [sha256.Size]byte
}

type criticalReadinessWork struct {
	context        catalogproto.CriticalFetchContext
	expiresAt      time.Time
	done           chan struct{}
	inputs         map[string][sha256.Size]byte
	physicalDigest [sha256.Size]byte
	err            error
}

type criticalReadinessReplay struct {
	inputDigest    [sha256.Size]byte
	expiresAt      time.Time
	readChallenge  string
	physicalDigest [sha256.Size]byte
}

type criticalAckWaiter struct {
	context catalogproto.CriticalFetchContext
	ready   chan struct{}
	once    sync.Once
}

type criticalReadinessCoordinator struct {
	lifetime   context.Context
	scheduler  criticalMaterializationScheduler
	runner     workerRunner
	executable string

	mu      sync.Mutex
	works   map[criticalReadinessWorkKey]*criticalReadinessWork
	replays map[string]criticalReadinessReplay
	waiters map[criticalFetchKey]*criticalAckWaiter
}

func newCriticalReadinessCoordinator(
	lifetime context.Context,
	scheduler criticalMaterializationScheduler,
	runner workerRunner,
	executable string,
) (*criticalReadinessCoordinator, error) {
	if lifetime == nil || scheduler == nil || runner == nil || executable == "" {
		return nil, errors.New("FuseKit critical readiness: coordinator is incomplete")
	}
	return &criticalReadinessCoordinator{
		lifetime: lifetime, scheduler: scheduler, runner: runner, executable: executable,
		works:   make(map[criticalReadinessWorkKey]*criticalReadinessWork),
		replays: make(map[string]criticalReadinessReplay),
		waiters: make(map[criticalFetchKey]*criticalAckWaiter),
	}, nil
}

func (c *criticalReadinessCoordinator) PrepareCriticalReadiness(
	ctx context.Context,
	proof catalogproto.TenantPreparationProof,
) (catalogproto.TenantPreparationProof, error) {
	if err := ctx.Err(); err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	if proof.CriticalReadiness == nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness input is missing", catalog.ErrIntegrity)
	}
	readiness := *proof.CriticalReadiness
	if readiness.ReadChallenge != "" || readiness.ReadProofDigest != nil || readiness.Lease.LeaseID == "" || readiness.Lease.ExpiresUnixNano == 0 {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness input is not provisional", catalog.ErrIntegrity)
	}
	if readiness.Lease.ExpiresUnixNano > math.MaxInt64 {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness lease expiry is invalid", catalog.ErrIntegrity)
	}
	expiresAt := time.Unix(0, int64(readiness.Lease.ExpiresUnixNano)).UTC()
	now := time.Now()
	if expiresAt.UnixNano() <= 0 || !expiresAt.After(now) {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness lease is expired", catalog.ErrInvalidTransition)
	}
	key, err := criticalWorkIdentity(readiness)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}
	inputDigest, err := criticalReadinessInputDigest(proof)
	if err != nil {
		return catalogproto.TenantPreparationProof{}, err
	}

	c.mu.Lock()
	c.expireReplaysLocked(now)
	if replay, ok := c.replays[readiness.Lease.LeaseID]; ok {
		c.mu.Unlock()
		if replay.inputDigest != inputDigest {
			return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness lease identity changed", catalog.ErrIntegrity)
		}
		return finalCriticalReadinessProof(proof, replay.readChallenge, replay.physicalDigest)
	}
	work, exists := c.works[key]
	if !exists {
		challenge, challengeErr := criticalReadChallenge()
		if challengeErr != nil {
			c.mu.Unlock()
			return catalogproto.TenantPreparationProof{}, challengeErr
		}
		work = &criticalReadinessWork{
			context: catalogproto.CriticalFetchContext{
				LeaseID: readiness.Lease.LeaseID, ResolutionDigest: readiness.ResolutionDigest, ReadChallenge: challenge,
			},
			expiresAt: expiresAt, done: make(chan struct{}),
			inputs: map[string][sha256.Size]byte{readiness.Lease.LeaseID: inputDigest},
		}
		c.works[key] = work
		scheduled := readiness
		scheduled.ReadChallenge = challenge
		go c.runWork(key, work, scheduled)
	} else if prior, ok := work.inputs[readiness.Lease.LeaseID]; ok && prior != inputDigest {
		c.mu.Unlock()
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness lease identity changed", catalog.ErrIntegrity)
	} else {
		work.inputs[readiness.Lease.LeaseID] = inputDigest
	}
	c.mu.Unlock()

	select {
	case <-work.done:
		if work.err != nil {
			return catalogproto.TenantPreparationProof{}, work.err
		}
	case <-ctx.Done():
		return catalogproto.TenantPreparationProof{}, ctx.Err()
	}
	if !expiresAt.After(time.Now()) {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: critical readiness lease expired during materialization", catalog.ErrInvalidTransition)
	}
	c.mu.Lock()
	c.replays[readiness.Lease.LeaseID] = criticalReadinessReplay{
		inputDigest: inputDigest, expiresAt: expiresAt, readChallenge: work.context.ReadChallenge,
		physicalDigest: work.physicalDigest,
	}
	c.mu.Unlock()
	return finalCriticalReadinessProof(proof, work.context.ReadChallenge, work.physicalDigest)
}

func (c *criticalReadinessCoordinator) runWork(
	key criticalReadinessWorkKey,
	work *criticalReadinessWork,
	readiness catalogproto.CriticalReadinessProof,
) {
	deadline := time.Now().Add(criticalReadTimeout)
	if work.expiresAt.Before(deadline) {
		deadline = work.expiresAt
	}
	ctx, cancel := context.WithDeadline(c.lifetime, deadline)
	defer cancel()
	digest, err := c.prepare(ctx, readiness, work.context)
	c.mu.Lock()
	work.physicalDigest = digest
	work.err = err
	delete(c.works, key)
	close(work.done)
	c.mu.Unlock()
}

func (c *criticalReadinessCoordinator) prepare(
	ctx context.Context,
	readiness catalogproto.CriticalReadinessProof,
	fetchContext catalogproto.CriticalFetchContext,
) ([sha256.Size]byte, error) {
	waiters := make([]*criticalAckWaiter, len(readiness.Objects))
	keys := make([]criticalFetchKey, len(readiness.Objects))
	unique := make(map[criticalFetchKey]struct{}, len(readiness.Objects))
	for index, object := range readiness.Objects {
		key := criticalFetchIdentity(readiness, object)
		if _, exists := unique[key]; exists {
			return [sha256.Size]byte{}, fmt.Errorf("%w: critical readiness fetch identity is duplicated", catalog.ErrIntegrity)
		}
		unique[key] = struct{}{}
		keys[index] = key
		waiters[index] = &criticalAckWaiter{context: fetchContext, ready: make(chan struct{})}
	}
	c.mu.Lock()
	for _, key := range keys {
		if _, exists := c.waiters[key]; exists {
			c.mu.Unlock()
			return [sha256.Size]byte{}, fmt.Errorf("%w: critical readiness fetch identity is already pending", catalog.ErrInvalidTransition)
		}
	}
	for index, key := range keys {
		c.waiters[key] = waiters[index]
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		for _, key := range keys {
			delete(c.waiters, key)
		}
		c.mu.Unlock()
	}()

	paths, err := c.scheduler.ScheduleCriticalMaterialization(ctx, readiness)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	physicalDigest, err := runCriticalPathReads(ctx, c.runner, c.executable, readiness, paths)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	for _, waiter := range waiters {
		select {
		case <-waiter.ready:
		case <-ctx.Done():
			return [sha256.Size]byte{}, ctx.Err()
		}
	}
	return physicalDigest, nil
}

func (c *criticalReadinessCoordinator) ResolveCriticalFetch(
	ctx context.Context,
	_ catalogservice.Identity,
	authorization catalogservice.Authorization,
	tenantID catalog.TenantID,
	request catalogproto.ResolveCriticalFetchRequest,
) (*catalogproto.CriticalFetchContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateCriticalFetchRoute(authorization, tenantID, request.Generation); err != nil {
		return nil, err
	}
	key := criticalFetchKey{
		tenant: tenantID, domain: authorization.Route.Domain, generation: request.Generation,
		object: request.ObjectID, objectRevision: request.ObjectRevision,
		contentRevision: request.ContentRevision, size: request.Size, hash: request.Hash,
	}
	c.mu.Lock()
	waiter := c.waiters[key]
	c.mu.Unlock()
	if waiter == nil {
		return nil, nil
	}
	resolved := waiter.context
	return &resolved, nil
}

func (c *criticalReadinessCoordinator) AckCriticalFetch(
	ctx context.Context,
	_ catalogservice.Identity,
	authorization catalogservice.Authorization,
	tenantID catalog.TenantID,
	request catalogproto.AckCriticalFetchRequest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateCriticalFetchRoute(authorization, tenantID, request.Generation); err != nil {
		return err
	}
	key := criticalFetchKey{
		tenant: tenantID, domain: authorization.Route.Domain, generation: request.Generation,
		object: request.ObjectID, objectRevision: request.ObjectRevision,
		contentRevision: request.ContentRevision, size: request.Size, hash: request.Hash,
	}
	c.mu.Lock()
	waiter := c.waiters[key]
	c.mu.Unlock()
	if waiter == nil {
		return fmt.Errorf("%w: critical fetch acknowledgement is not pending", catalog.ErrNotFound)
	}
	if waiter.context.LeaseID != request.LeaseID || waiter.context.ResolutionDigest != request.ResolutionDigest ||
		waiter.context.ReadChallenge != request.ReadChallenge {
		return fmt.Errorf("%w: critical fetch acknowledgement challenge changed", catalog.ErrIntegrity)
	}
	waiter.once.Do(func() { close(waiter.ready) })
	return nil
}

func (c *criticalReadinessCoordinator) expireReplaysLocked(now time.Time) {
	for leaseID, replay := range c.replays {
		if !replay.expiresAt.After(now) {
			delete(c.replays, leaseID)
		}
	}
}

func criticalReadinessInputDigest(proof catalogproto.TenantPreparationProof) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(proof)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("FuseKit critical readiness: encode input: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func finalCriticalReadinessProof(
	proof catalogproto.TenantPreparationProof,
	readChallenge string,
	physicalDigest [sha256.Size]byte,
) (catalogproto.TenantPreparationProof, error) {
	readiness := *proof.CriticalReadiness
	readiness.ReadChallenge = readChallenge
	digest := criticalReadProofDigest(readiness, physicalDigest)
	value := hex.EncodeToString(digest[:])
	readiness.ReadProofDigest = &value
	proof.CriticalReadiness = &readiness
	if err := catalogproto.Validate(proof); err != nil {
		return catalogproto.TenantPreparationProof{}, fmt.Errorf("%w: final critical readiness proof is invalid: %v", catalog.ErrIntegrity, err)
	}
	return proof, nil
}

func criticalWorkIdentity(readiness catalogproto.CriticalReadinessProof) (criticalReadinessWorkKey, error) {
	objects, err := json.Marshal(readiness.Objects)
	if err != nil {
		return criticalReadinessWorkKey{}, fmt.Errorf("FuseKit critical readiness: encode object identity: %w", err)
	}
	return criticalReadinessWorkKey{
		tenant: readiness.Lease.TenantID, domain: readiness.DomainID, generation: readiness.TenantGeneration,
		root: readiness.RootID, presentationInstance: readiness.PresentationInstanceID,
		activationGeneration: readiness.ActivationGeneration, policyDigest: readiness.PolicyDigest,
		resolutionDigest: readiness.ResolutionDigest, catalogHead: readiness.CatalogHead,
		sourceRevision: readiness.SourceRevision, sourceAuthority: readiness.Lease.SourceAuthority,
		sourcePublication: readiness.Lease.SourcePublication, objectsDigest: sha256.Sum256(objects),
	}, nil
}

func criticalReadChallenge() (string, error) {
	var challenge [sha256.Size]byte
	if _, err := io.ReadFull(rand.Reader, challenge[:]); err != nil {
		return "", fmt.Errorf("FuseKit critical readiness: generate challenge: %w", err)
	}
	return hex.EncodeToString(challenge[:]), nil
}

func criticalReadProofDigest(
	readiness catalogproto.CriticalReadinessProof,
	physicalDigest [sha256.Size]byte,
) [sha256.Size]byte {
	digest := sha256.New()
	writeCriticalDigestString(digest, readiness.Lease.LeaseID)
	writeCriticalDigestString(digest, readiness.ResolutionDigest)
	writeCriticalDigestString(digest, readiness.ReadChallenge)
	_ = binary.Write(digest, binary.BigEndian, readiness.Lease.ExpiresUnixNano)
	_, _ = digest.Write(physicalDigest[:])
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func criticalFetchIdentity(
	readiness catalogproto.CriticalReadinessProof,
	object catalogproto.ResolvedCriticalObjectProof,
) criticalFetchKey {
	return criticalFetchKey{
		tenant: catalog.TenantID(readiness.Lease.TenantID), domain: readiness.DomainID,
		generation: readiness.TenantGeneration, object: object.ObjectID,
		objectRevision: object.ObjectRevision, contentRevision: object.ContentRevision,
		size: object.Size, hash: object.Hash,
	}
}

func validateCriticalFetchRoute(
	authorization catalogservice.Authorization,
	tenantID catalog.TenantID,
	generation uint64,
) error {
	if authorization.Role != catalogservice.RoleFileProvider ||
		authorization.Presentation != catalog.PresentationFileProvider ||
		!authorization.Route.Forwarded || authorization.Route.Tenant != tenantID ||
		authorization.Route.Generation != catalog.Generation(generation) || authorization.Route.Domain == "" {
		return fmt.Errorf("%w: critical fetch route changed", catalog.ErrIntegrity)
	}
	return nil
}

var (
	_ catalogservice.CriticalReadinessPreparer = (*criticalReadinessCoordinator)(nil)
	_ catalogservice.CriticalFetchService      = (*criticalReadinessCoordinator)(nil)
)
