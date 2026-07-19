package catalogworker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

const (
	maxWriteHandles      = 1024
	maxOwnerWriteHandles = 256
	maxWriteStageSize    = int64(1 << 30)
	maxOwnerWriteBytes   = int64(1 << 30)
	maxTotalWriteBytes   = int64(4 << 30)
	maxNativeIOBytes     = 1 << 20
	maxNativeStageFiles  = 2*maxWriteHandles + 17
	nativeWriteRuntime   = "runtime"
)

type nativeWriteManifest struct {
	Token        string                `json:"token"`
	Owner        string                `json:"owner"`
	Tenant       catalog.TenantID      `json:"tenant"`
	Presentation catalog.Presentation  `json:"presentation"`
	Generation   catalog.Generation    `json:"generation"`
	Object       catalog.Object        `json:"object"`
	ExpectedHead catalog.Revision      `json:"expected_head"`
	Dirty        bool                  `json:"dirty"`
	Prepared     *nativePreparedWrite  `json:"prepared,omitempty"`
	Last         *nativeCommittedWrite `json:"last,omitempty"`
}

type nativePreparedWrite struct {
	Operation catalog.MutationID  `json:"operation"`
	Ref       catalog.ContentRef  `json:"ref"`
	Target    catalog.Revision    `json:"target"`
	Pin       catalog.MutationPin `json:"pin"`
}

type nativeWriteProof struct {
	Token        string               `json:"token"`
	Tenant       catalog.TenantID     `json:"tenant"`
	Presentation catalog.Presentation `json:"presentation"`
	Generation   catalog.Generation   `json:"generation"`
	Object       catalog.ObjectID     `json:"object"`
	ExpectedHead catalog.Revision     `json:"expected_head"`
	Ref          catalog.ContentRef   `json:"ref"`
}

type nativeCommittedWrite struct {
	Operation catalog.MutationID  `json:"operation"`
	Proof     nativeWriteProof    `json:"proof"`
	Object    catalog.Object      `json:"object"`
	Pin       catalog.MutationPin `json:"pin"`
}

type nativeWriteSealResult struct {
	Prepared   *catalog.PreparedMutation `json:"prepared,omitempty"`
	Object     *catalog.Object           `json:"object,omitempty"`
	Operation  catalog.MutationID        `json:"operation"`
	Generation catalog.Generation        `json:"generation"`
}

// NativeWriteCommit is the exact catalog identity and object committed by one
// mutable native handle.
type NativeWriteCommit struct {
	OperationID catalog.MutationID
	Object      catalog.Object
}

// PrepareTenantFunc converges one exact tenant generation to a catalog revision.
type PrepareTenantFunc func(context.Context, catalog.TenantID, catalog.Generation, catalog.Revision) error

type nativeOwnerLane struct {
	closing  bool
	next     uint64
	active   map[uint64]context.CancelFunc
	zero     chan struct{}
	closed   chan struct{}
	closeErr error
}

type nativeTokenLane struct {
	gate chan struct{}
	refs int
}

func newNativeOwnerLane() *nativeOwnerLane {
	zero := make(chan struct{})
	close(zero)
	return &nativeOwnerLane{
		active: make(map[uint64]context.CancelFunc), zero: zero, closed: make(chan struct{}),
	}
}

func (m *Manager) beginNativeOwnerOperation(
	ctx context.Context, owner string,
) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("catalog worker: native owner context is nil")
	}
	if err := validateNativeOwner(owner); err != nil {
		return nil, nil, err
	}
	m.nativeOwnerMu.Lock()
	if m.nativeOwners == nil {
		m.nativeOwners = make(map[string]*nativeOwnerLane)
	}
	lane := m.nativeOwners[owner]
	if lane == nil {
		lane = newNativeOwnerLane()
		m.nativeOwners[owner] = lane
	}
	if lane.closing {
		m.nativeOwnerMu.Unlock()
		return nil, nil, catalog.ErrHandleClosed
	}
	if len(lane.active) == 0 {
		lane.zero = make(chan struct{})
	}
	lane.next++
	id := lane.next
	operationCtx, cancel := context.WithCancel(ctx)
	lane.active[id] = cancel
	m.nativeOwnerMu.Unlock()

	var once sync.Once
	done := func() {
		once.Do(func() {
			cancel()
			m.nativeOwnerMu.Lock()
			delete(lane.active, id)
			if len(lane.active) == 0 {
				close(lane.zero)
			}
			m.nativeOwnerMu.Unlock()
		})
	}
	return operationCtx, done, nil
}

func nativeOwnerCall[T any](
	m *Manager,
	ctx context.Context,
	owner string,
	call func(context.Context) (T, error),
) (T, error) {
	operationCtx, done, err := m.beginNativeOwnerOperation(ctx, owner)
	if err != nil {
		var zero T
		return zero, err
	}
	defer done()
	return call(operationCtx)
}

func (m *Manager) lockNativeToken(ctx context.Context, token string) (func(), error) {
	if !validNativeWriteToken(token) {
		return nil, fmt.Errorf("%w: invalid write handle", catalog.ErrInvalidObject)
	}
	m.nativeTokenMu.Lock()
	if m.nativeTokens == nil {
		m.nativeTokens = make(map[string]*nativeTokenLane)
	}
	lane := m.nativeTokens[token]
	if lane == nil {
		lane = &nativeTokenLane{gate: make(chan struct{}, 1)}
		lane.gate <- struct{}{}
		m.nativeTokens[token] = lane
	}
	lane.refs++
	m.nativeTokenMu.Unlock()

	select {
	case <-lane.gate:
	case <-ctx.Done():
		m.nativeTokenMu.Lock()
		lane.refs--
		if lane.refs == 0 {
			delete(m.nativeTokens, token)
		}
		m.nativeTokenMu.Unlock()
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			lane.gate <- struct{}{}
			m.nativeTokenMu.Lock()
			lane.refs--
			if lane.refs == 0 {
				delete(m.nativeTokens, token)
			}
			m.nativeTokenMu.Unlock()
		})
	}, nil
}

func (m *Manager) closeNativeOwner(
	ctx context.Context,
	owner string,
	cleanup func(context.Context) error,
) error {
	if ctx == nil {
		return errors.New("catalog worker: native owner context is nil")
	}
	if err := validateNativeOwner(owner); err != nil {
		return err
	}
	m.nativeOwnerMu.Lock()
	if m.nativeOwners == nil {
		m.nativeOwners = make(map[string]*nativeOwnerLane)
	}
	lane := m.nativeOwners[owner]
	if lane == nil {
		lane = newNativeOwnerLane()
		m.nativeOwners[owner] = lane
	}
	if lane.closing {
		closed := lane.closed
		m.nativeOwnerMu.Unlock()
		<-closed
		m.nativeOwnerMu.Lock()
		err := lane.closeErr
		m.nativeOwnerMu.Unlock()
		return err
	}
	lane.closing = true
	zero := lane.zero
	cancels := make([]context.CancelFunc, 0, len(lane.active))
	for _, cancel := range lane.active {
		cancels = append(cancels, cancel)
	}
	m.nativeOwnerMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	<-zero

	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.config.OperationTimeout)
	cleanupErr := cleanup(cleanupCtx)
	cancel()
	m.nativeOwnerMu.Lock()
	lane.closeErr = cleanupErr
	close(lane.closed)
	m.nativeOwnerMu.Unlock()
	return cleanupErr
}

// BindTenantPreparer binds the one semantic tenant convergence callback.
func (m *Manager) BindTenantPreparer(preparer PrepareTenantFunc) error {
	if preparer == nil {
		return errors.New("catalog worker: tenant preparer is nil")
	}
	m.preparerMu.Lock()
	defer m.preparerMu.Unlock()
	if m.preparer != nil {
		return errors.New("catalog worker: tenant preparer already bound")
	}
	m.preparer = preparer
	return nil
}

func (m *Manager) tenantPreparer() (PrepareTenantFunc, error) {
	m.preparerMu.Lock()
	defer m.preparerMu.Unlock()
	if m.preparer == nil {
		return nil, errors.New("catalog worker: tenant preparer is not bound")
	}
	return m.preparer, nil
}

type ownedWriteFile struct {
	file *os.File
	once sync.Once
	err  error
}

func (s *ownedWriteFile) Read(buffer []byte) (int, error) { return s.file.Read(buffer) }
func (s *ownedWriteFile) Settle(error) error {
	s.once.Do(func() { s.err = s.file.Close() })
	return s.err
}
func (*ownedWriteFile) Wait(context.Context) error { return nil }

func newNativeWriteToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validNativeWriteToken(token string) bool {
	if len(token) != 32 || strings.ToLower(token) != token {
		return false
	}
	decoded, err := hex.DecodeString(token)
	return err == nil && len(decoded) == 16
}

func recoverNativeWrites(root, runtime string) error {
	if !filepath.IsAbs(root) || filepath.Base(root) == "." ||
		!strings.HasSuffix(root, ".native-writes") || !validNativeWriteToken(runtime) {
		return fmt.Errorf("%w: invalid native write recovery identity", catalog.ErrInvalidObject)
	}
	marker := filepath.Join(root, nativeWriteRuntime)
	current, err := os.ReadFile(marker)
	if err == nil && string(current) == runtime {
		return auditNativeWrites(root)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(runtime)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func auditNativeWrites(root string) error {
	pairs := make(map[string]uint8)
	err := visitNativeWriteEntries(root, func(entry os.DirEntry) error {
		if entry.IsDir() {
			return fmt.Errorf("%w: nested native write stage", catalog.ErrIntegrity)
		}
		name := entry.Name()
		if name == nativeWriteRuntime {
			return nil
		}
		if strings.HasPrefix(name, ".manifest-") {
			return os.Remove(filepath.Join(root, name))
		}
		extension := filepath.Ext(name)
		if extension != ".json" && extension != ".data" {
			return fmt.Errorf("%w: unknown native write stage file", catalog.ErrIntegrity)
		}
		token := strings.TrimSuffix(name, extension)
		if !validNativeWriteToken(token) {
			return fmt.Errorf("%w: invalid native write stage file", catalog.ErrIntegrity)
		}
		bit := uint8(1)
		if extension == ".data" {
			bit = 2
		}
		if pairs[token]&bit != 0 {
			return fmt.Errorf("%w: duplicate native write stage file", catalog.ErrIntegrity)
		}
		pairs[token] |= bit
		return nil
	})
	if err != nil {
		return err
	}
	service := &server{writeRoot: root}
	for token, pair := range pairs {
		if pair != 3 {
			return fmt.Errorf("%w: incomplete native write stage", catalog.ErrIntegrity)
		}
		if _, err := service.loadWrite(token); err != nil {
			return err
		}
	}
	return service.writeCapacity("", 0, false)
}

func visitNativeWriteEntries(root string, visit func(os.DirEntry) error) (result error) {
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, directory.Close()) }()
	seen := 0
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			seen++
			if seen > maxNativeStageFiles {
				return fmt.Errorf("%w: native write stage file capacity", catalog.ErrStorageQuota)
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *server) writePaths(token string) (string, string, error) {
	if !validNativeWriteToken(token) {
		return "", "", fmt.Errorf("%w: invalid write handle", catalog.ErrInvalidObject)
	}
	return filepath.Join(s.writeRoot, token+".json"), filepath.Join(s.writeRoot, token+".data"), nil
}

func (s *server) loadWrite(token string) (nativeWriteManifest, error) {
	manifestPath, _, err := s.writePaths(token)
	if err != nil {
		return nativeWriteManifest{}, err
	}
	encoded, err := os.ReadFile(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return nativeWriteManifest{}, catalog.ErrHandleClosed
	}
	if err != nil {
		return nativeWriteManifest{}, err
	}
	var manifest nativeWriteManifest
	if err := decodeStrictJSON(encoded, &manifest); err != nil {
		return nativeWriteManifest{}, fmt.Errorf("%w: decode write handle: %v", catalog.ErrIntegrity, err)
	}
	if manifest.Token != token || manifest.Owner == "" || manifest.Tenant == "" ||
		manifest.Generation == 0 || manifest.Object.ID == (catalog.ObjectID{}) ||
		manifest.ExpectedHead == 0 {
		return nativeWriteManifest{}, fmt.Errorf("%w: incomplete write handle", catalog.ErrIntegrity)
	}
	return manifest, nil
}

func (s *server) saveWrite(manifest nativeWriteManifest) error {
	manifestPath, _, err := s.writePaths(manifest.Token)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.writeRoot, 0o700); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.writeRoot, ".manifest-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	result := errors.Join(temporary.Chmod(0o600))
	if result == nil {
		_, result = temporary.Write(encoded)
	}
	if result == nil {
		result = temporary.Sync()
	}
	result = errors.Join(result, temporary.Close())
	if result != nil {
		return result
	}
	if err := os.Rename(temporaryPath, manifestPath); err != nil {
		return err
	}
	directory, err := os.Open(s.writeRoot)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (s *server) recoverMutationPins(ctx context.Context) error {
	if err := os.MkdirAll(s.writeRoot, 0o700); err != nil {
		return err
	}
	return visitNativeWriteEntries(s.writeRoot, func(entry os.DirEntry) error {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			return nil
		}
		token := strings.TrimSuffix(entry.Name(), ".json")
		manifest, err := s.loadWrite(token)
		if err != nil {
			return err
		}
		var acquired []catalog.MutationPin
		recoverPin := func(operation catalog.MutationID) (catalog.MutationPin, error) {
			owner, ownerErr := nativeRetentionOwner(manifest.Owner)
			if ownerErr != nil {
				return catalog.MutationPin{}, ownerErr
			}
			pin, pinErr := s.store.PinMutation(ctx, owner, manifest.Tenant, operation)
			if pinErr == nil {
				acquired = append(acquired, pin)
			}
			return pin, pinErr
		}
		if manifest.Prepared != nil {
			if manifest.Prepared.Target != manifest.Prepared.Operation.TargetRevision() {
				return catalog.ErrIntegrity
			}
			manifest.Prepared.Pin, err = recoverPin(manifest.Prepared.Operation)
		}
		if err == nil && manifest.Last != nil {
			manifest.Last.Pin, err = recoverPin(manifest.Last.Operation)
		}
		if err == nil {
			err = s.saveWrite(manifest)
		}
		if err != nil {
			cleanupCtx := context.WithoutCancel(ctx)
			for _, pin := range acquired {
				err = errors.Join(err, s.closeAndForgetMutationPin(cleanupCtx, pin))
			}
		}
		return err
	})
}

func (s *server) closeAndForgetMutationPin(
	ctx context.Context,
	pin catalog.MutationPin,
) error {
	if err := s.store.CloseMutationPin(ctx, pin); err != nil {
		return err
	}
	return s.store.ForgetMutationPin(ctx, pin)
}

func (s *server) authorizeWrite(owner, token string) (nativeWriteManifest, string, error) {
	if err := validateNativeOwner(owner); err != nil {
		return nativeWriteManifest{}, "", err
	}
	manifest, err := s.loadWrite(token)
	if err != nil {
		return nativeWriteManifest{}, "", err
	}
	if manifest.Owner != owner {
		return nativeWriteManifest{}, "", catalog.ErrHandleClosed
	}
	_, dataPath, err := s.writePaths(token)
	return manifest, dataPath, err
}

func (s *server) writeCapacity(owner string, additional int64, opening bool) error {
	if additional < 0 {
		return catalog.ErrInvalidObject
	}
	if err := os.MkdirAll(s.writeRoot, 0o700); err != nil {
		return err
	}
	total, owned := 0, 0
	var totalBytes, ownerBytes int64
	err := visitNativeWriteEntries(s.writeRoot, func(entry os.DirEntry) error {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			return nil
		}
		total++
		token := strings.TrimSuffix(entry.Name(), ".json")
		manifest, err := s.loadWrite(token)
		if err != nil {
			return err
		}
		if manifest.Owner == owner {
			owned++
		}
		_, dataPath, pathErr := s.writePaths(token)
		if pathErr != nil {
			return pathErr
		}
		info, statErr := os.Stat(dataPath)
		if statErr != nil {
			return fmt.Errorf("%w: inspect mutable write stage: %v", catalog.ErrIntegrity, statErr)
		}
		if info.Size() < 0 || info.Size() > maxWriteStageSize ||
			totalBytes > maxTotalWriteBytes-info.Size() {
			return fmt.Errorf("%w: mutable write stage accounting", catalog.ErrIntegrity)
		}
		totalBytes += info.Size()
		if manifest.Owner == owner {
			if ownerBytes > maxOwnerWriteBytes-info.Size() {
				return fmt.Errorf("%w: mutable owner stage accounting", catalog.ErrIntegrity)
			}
			ownerBytes += info.Size()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if opening && (total >= maxWriteHandles || owned >= maxOwnerWriteHandles) {
		return fmt.Errorf("%w: mutable write handle capacity", catalog.ErrStorageQuota)
	}
	if additional > maxTotalWriteBytes-totalBytes ||
		additional > maxOwnerWriteBytes-ownerBytes {
		return fmt.Errorf("%w: mutable write byte capacity", catalog.ErrStorageQuota)
	}
	return nil
}

func nativeWriteMetadata(proof nativeWriteProof) (string, error) {
	encoded, err := json.Marshal(proof)
	return string(encoded), err
}

func nativeWriteIntentProof(prepared catalog.PreparedMutation) (nativeWriteProof, error) {
	if prepared.Intent.Revise == nil {
		return nativeWriteProof{}, catalog.ErrMutationConflict
	}
	var proof nativeWriteProof
	if err := decodeStrictJSON([]byte(prepared.Intent.SourceMetadata), &proof); err != nil {
		return nativeWriteProof{}, fmt.Errorf("%w: decode mutable write proof: %v", catalog.ErrIntegrity, err)
	}
	if !validNativeWriteToken(proof.Token) || proof.Tenant == "" ||
		proof.Generation == 0 || proof.Object == (catalog.ObjectID{}) ||
		proof.ExpectedHead == 0 || proof.Ref.Hash == ([32]byte{}) || proof.Ref.Size < 0 {
		return nativeWriteProof{}, fmt.Errorf("%w: incomplete mutable write proof", catalog.ErrIntegrity)
	}
	return proof, nil
}

func validateNativeWriteProof(
	owner string,
	manifest nativeWriteManifest,
	proof nativeWriteProof,
) error {
	if proof.Token != manifest.Token || proof.Tenant != manifest.Tenant ||
		proof.Presentation != manifest.Presentation || proof.Generation != manifest.Generation ||
		proof.Object != manifest.Object.ID || owner != manifest.Owner {
		return catalog.ErrMutationConflict
	}
	return nil
}

func (s *server) lastWriteReceipt(
	ctx context.Context,
	owner string,
	manifest nativeWriteManifest,
) (catalog.Object, bool, error) {
	if manifest.Last == nil {
		return catalog.Object{}, false, nil
	}
	proof := manifest.Last.Proof
	if err := validateNativeWriteProof(owner, manifest, proof); err != nil {
		return catalog.Object{}, false, err
	}
	record, err := s.store.Mutation(ctx, manifest.Tenant, manifest.Last.Operation)
	if err != nil {
		return catalog.Object{}, false, err
	}
	if record.Tenant != proof.Tenant || record.Kind != catalog.MutationRevise ||
		record.Primary != [16]byte(proof.Object) || record.Revision != proof.ExpectedHead+1 {
		return catalog.Object{}, false, fmt.Errorf("%w: mutable write receipt changed", catalog.ErrIntegrity)
	}
	if manifest.Last.Pin.Tenant != manifest.Tenant ||
		manifest.Last.Pin.Mutation != manifest.Last.Operation ||
		manifest.Last.Pin.Target != record.Revision {
		return catalog.Object{}, false, fmt.Errorf("%w: mutable write receipt pin changed", catalog.ErrIntegrity)
	}
	object, err := s.store.LookupAt(
		ctx, proof.Tenant, proof.Presentation, proof.Object, record.Revision,
	)
	if err != nil {
		return catalog.Object{}, false, err
	}
	if object.ID != proof.Object ||
		object.ContentRevision != manifest.Last.Object.ContentRevision ||
		object.Hash != proof.Ref.Hash || object.Size != proof.Ref.Size {
		return catalog.Object{}, false, fmt.Errorf("%w: mutable write receipt content changed", catalog.ErrIntegrity)
	}
	if object != manifest.Last.Object {
		return catalog.Object{}, false, fmt.Errorf("%w: mutable write receipt object changed", catalog.ErrIntegrity)
	}
	return object, true, nil
}

func (s *server) handleOpenWriteAt(ctx context.Context, request wire.Request) (any, error) {
	var input openWriteAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(openWriteAtResponse{Header: decodeError(err)})
	}
	response := openWriteAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := validateNativeOwner(input.Owner); err != nil || !validNativeWriteToken(input.Token) ||
		input.Generation == 0 || input.Revision == 0 {
		if err == nil {
			err = fmt.Errorf("%w: incomplete write handle identity", catalog.ErrInvalidObject)
		}
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	if existing, err := s.loadWrite(input.Token); err == nil {
		if existing.Owner != input.Owner || existing.Tenant != input.Tenant ||
			existing.Presentation != input.Presentation || existing.Generation != input.Generation ||
			existing.Object.ID != input.ID || existing.ExpectedHead != input.Revision {
			response.Header.Error = encodeRemoteError(catalog.ErrConflict)
		} else {
			response.Object = existing.Object
		}
		return encodeResponse(response)
	} else if !errors.Is(err, catalog.ErrHandleClosed) {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	state, err := s.store.LoadTenantState(ctx, input.Tenant)
	if err != nil || state.Generation != input.Generation {
		if err == nil {
			err = catalog.ErrGenerationMismatch
		}
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	object, err := s.store.LookupAt(ctx, input.Tenant, input.Presentation, input.ID, input.Revision)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	if object.Kind != catalog.KindFile || object.Size < 0 || object.Size > maxWriteStageSize {
		response.Header.Error = encodeRemoteError(fmt.Errorf("%w: mutable object size", catalog.ErrStorageQuota))
		return encodeResponse(response)
	}
	if err := s.writeCapacity(input.Owner, object.Size, true); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	retentionOwner, err := nativeRetentionOwner(input.Owner)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	handle, err := s.store.OpenAt(
		ctx, retentionOwner, input.Tenant, input.Presentation,
		input.Generation, input.ID, object.Revision,
	)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	if err := os.MkdirAll(s.writeRoot, 0o700); err != nil {
		_ = closeAndForgetSnapshotHandle(context.WithoutCancel(ctx), handle)
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	_, dataPath, _ := s.writePaths(input.Token)
	_ = os.Remove(dataPath)
	err = seedNativeWrite(dataPath, object.Size, handle, os.OpenFile)
	err = errors.Join(
		err,
		closeAndForgetSnapshotHandle(context.WithoutCancel(ctx), handle),
	)
	if err != nil {
		_ = os.Remove(dataPath)
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	manifest := nativeWriteManifest{
		Token: input.Token, Owner: input.Owner, Tenant: input.Tenant,
		Presentation: input.Presentation, Generation: input.Generation,
		Object: object, ExpectedHead: input.Revision,
	}
	if err := s.saveWrite(manifest); err != nil {
		_ = os.Remove(dataPath)
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	response.Object = object
	return encodeResponse(response)
}

func seedNativeWrite(
	path string,
	expected int64,
	source io.ReadCloser,
	open func(string, int, os.FileMode) (*os.File, error),
) error {
	file, err := open(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err == nil {
		_, err = io.Copy(file, io.LimitReader(source, maxWriteStageSize+1))
	}
	if err == nil {
		var position int64
		position, err = file.Seek(0, io.SeekEnd)
		if err == nil && position != expected {
			err = fmt.Errorf("%w: mutable seed size changed", catalog.ErrIntegrity)
		}
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, source.Close())
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	return err
}

func (s *server) handleReadWriteAt(ctx context.Context, request wire.Request) (any, error) {
	var input readWriteAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(readWriteAtResponse{Header: decodeError(err)})
	}
	response := readWriteAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, dataPath, err := s.authorizeWrite(input.Owner, input.Token)
	if err == nil && (input.Offset < 0 || input.Limit <= 0 || input.Limit > maxNativeIOBytes) {
		err = catalog.ErrInvalidObject
	}
	if err == nil {
		file, openErr := os.Open(dataPath)
		if openErr == nil {
			buffer := make([]byte, input.Limit)
			var count int
			count, err = file.ReadAt(buffer, input.Offset)
			if errors.Is(err, io.EOF) {
				response.EOF = true
				err = nil
			}
			response.Data = buffer[:count]
			err = errors.Join(err, file.Close())
		}
		err = errors.Join(err, openErr)
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleWriteAt(ctx context.Context, request wire.Request) (any, error) {
	var input writeAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(writeAtResponse{Header: decodeError(err)})
	}
	response := writeAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	manifest, dataPath, err := s.authorizeWrite(input.Owner, input.Token)
	end := input.Offset + int64(len(input.Data))
	if err == nil && (input.Offset < 0 || len(input.Data) == 0 ||
		len(input.Data) > maxNativeIOBytes || end < input.Offset || end > maxWriteStageSize) {
		err = catalog.ErrStorageQuota
	}
	if err == nil && manifest.Prepared != nil {
		err = catalog.ErrMutationActive
	}
	if err == nil {
		info, statErr := os.Stat(dataPath)
		if statErr != nil {
			err = statErr
		} else if growth := end - info.Size(); growth > 0 {
			err = s.writeCapacity(input.Owner, growth, false)
		}
	}
	if err == nil && !manifest.Dirty {
		manifest.Dirty = true
		err = s.saveWrite(manifest)
	}
	if err == nil {
		file, openErr := os.OpenFile(dataPath, os.O_RDWR, 0)
		if openErr == nil {
			response.Written, err = file.WriteAt(input.Data, input.Offset)
			err = errors.Join(err, file.Close())
		}
		err = errors.Join(err, openErr)
	}
	if err == nil && response.Written != len(input.Data) {
		err = catalog.ErrIntegrity
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleTruncateWrite(ctx context.Context, request wire.Request) (any, error) {
	var input truncateWriteRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(truncateWriteResponse{Header: decodeError(err)})
	}
	response := truncateWriteResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	manifest, dataPath, err := s.authorizeWrite(input.Owner, input.Token)
	if err == nil && (input.Size < 0 || input.Size > maxWriteStageSize) {
		err = catalog.ErrStorageQuota
	}
	if err == nil && manifest.Prepared != nil {
		err = catalog.ErrMutationActive
	}
	if err == nil {
		info, statErr := os.Stat(dataPath)
		if statErr != nil {
			err = statErr
		} else if growth := input.Size - info.Size(); growth > 0 {
			err = s.writeCapacity(input.Owner, growth, false)
		}
	}
	if err == nil && !manifest.Dirty {
		manifest.Dirty = true
		err = s.saveWrite(manifest)
	}
	if err == nil {
		err = os.Truncate(dataPath, input.Size)
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleSyncWrite(ctx context.Context, request wire.Request) (any, error) {
	var input syncWriteRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(syncWriteResponse{Header: decodeError(err)})
	}
	response := syncWriteResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, dataPath, err := s.authorizeWrite(input.Owner, input.Token)
	if err == nil {
		file, openErr := os.OpenFile(dataPath, os.O_RDWR, 0)
		if openErr == nil {
			err = file.Sync()
			err = errors.Join(err, file.Close())
		}
		err = errors.Join(err, openErr)
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleSealAndBeginWrite(ctx context.Context, request wire.Request) (any, error) {
	var input sealAndBeginWriteRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(sealAndBeginWriteResponse{Header: decodeError(err)})
	}
	response := sealAndBeginWriteResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	manifest, dataPath, err := s.authorizeWrite(input.Owner, input.Token)
	if err == nil {
		var object catalog.Object
		var committed bool
		object, committed, err = s.lastWriteReceipt(ctx, input.Owner, manifest)
		if err == nil && committed {
			response.Result.Object = &object
			response.Result.Operation = manifest.Last.Operation
			response.Result.Generation = manifest.Generation
			return encodeResponse(response)
		}
	}
	if err == nil && manifest.Prepared != nil {
		prepared, preparedErr := s.store.PreparedMutation(
			ctx, manifest.Tenant, manifest.Prepared.Operation,
		)
		if preparedErr != nil {
			err = preparedErr
		} else {
			proof, proofErr := nativeWriteIntentProof(prepared)
			if proofErr != nil {
				err = proofErr
			} else if proofErr = validateNativeWriteProof(input.Owner, manifest, proof); proofErr != nil {
				err = proofErr
			} else {
				response.Result.Prepared = &prepared
				response.Result.Operation = prepared.OperationID
				response.Result.Generation = manifest.Generation
				return encodeResponse(response)
			}
		}
	}
	if err == nil {
		state, stateErr := s.store.LoadTenantState(ctx, manifest.Tenant)
		if stateErr != nil {
			err = stateErr
		} else if state.Generation != manifest.Generation {
			err = catalog.ErrGenerationMismatch
		}
	}
	if err == nil && !manifest.Dirty {
		err = catalog.ErrInvalidTransition
	}
	var ref catalog.ContentRef
	if err == nil {
		file, openErr := os.Open(dataPath)
		if openErr == nil {
			ref, err = s.store.StageOwnedContent(ctx, &ownedWriteFile{file: file})
		}
		if openErr != nil {
			err = openErr
		}
	}
	var prepared catalog.PreparedMutation
	var proof nativeWriteProof
	if err == nil {
		proof = nativeWriteProof{
			Token: manifest.Token, Tenant: manifest.Tenant,
			Presentation: manifest.Presentation, Generation: manifest.Generation,
			Object: manifest.Object.ID, ExpectedHead: manifest.ExpectedHead, Ref: ref,
		}
		metadata, metadataErr := nativeWriteMetadata(proof)
		if metadataErr != nil {
			err = metadataErr
		}
		intent := catalog.MutationIntent{
			SourceID:       "mount:" + input.Owner,
			SourceMetadata: metadata,
			Origin:         catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
			Revise: &catalog.ReviseMutation{
				Object: manifest.Object.ID,
				Spec: catalog.RevisionSpec{
					Parent: manifest.Object.Parent, Name: manifest.Object.Name,
					Mode: manifest.Object.Mode, Visibility: manifest.Object.Visibility,
					Content: &catalog.ContentUpdate{
						Revision: manifest.Object.ContentRevision + 1, Ref: ref,
					},
				},
			},
		}
		if err == nil {
			prepared, err = s.store.BeginMutation(
				ctx, manifest.Tenant, manifest.ExpectedHead, intent,
			)
		}
	}
	if err == nil {
		persistedProof, proofErr := nativeWriteIntentProof(prepared)
		if proofErr != nil {
			err = proofErr
		} else if prepared.Tenant != manifest.Tenant || prepared.ExpectedHead != manifest.ExpectedHead ||
			prepared.Intent.Revise == nil || prepared.Intent.Revise.Object != manifest.Object.ID ||
			persistedProof != proof {
			err = catalog.ErrMutationConflict
		} else {
			if persisted := prepared.Intent.Revise.Spec.Content.Ref; persisted != ref {
				err = s.store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), []catalog.ContentRef{ref})
			}
		}
		if err == nil {
			ref = prepared.Intent.Revise.Spec.Content.Ref
			owner, ownerErr := nativeRetentionOwner(input.Owner)
			if ownerErr != nil {
				err = ownerErr
			}
			var pin catalog.MutationPin
			if err == nil {
				pin, err = s.store.PinMutation(
					ctx, owner, manifest.Tenant, prepared.OperationID,
				)
			}
			manifest.Prepared = &nativePreparedWrite{
				Operation: prepared.OperationID, Ref: ref,
				Target: prepared.ExpectedHead + 1, Pin: pin,
			}
			if err == nil {
				err = s.saveWrite(manifest)
			}
			if err != nil && pin.ID != (catalog.MutationPinID{}) {
				err = errors.Join(
					err,
					s.closeAndForgetMutationPin(context.WithoutCancel(ctx), pin),
				)
			}
		}
	}
	if err == nil {
		response.Result.Prepared = &prepared
		response.Result.Operation = prepared.OperationID
		response.Result.Generation = manifest.Generation
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleResolveCommittedWrite(ctx context.Context, request wire.Request) (any, error) {
	var input resolveCommittedWriteRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(resolveCommittedWriteResponse{Header: decodeError(err)})
	}
	response := resolveCommittedWriteResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	manifest, _, err := s.authorizeWrite(input.Owner, input.Token)
	if err == nil && manifest.Prepared == nil {
		var committed bool
		response.Object, committed, err = s.lastWriteReceipt(ctx, input.Owner, manifest)
		if err == nil && committed {
			response.Operation = manifest.Last.Operation
			return encodeResponse(response)
		}
		if err == nil {
			err = catalog.ErrMutationConflict
		}
	}
	if err == nil && manifest.Prepared == nil {
		err = catalog.ErrMutationConflict
	}
	var operation catalog.MutationID
	if err == nil {
		operation = manifest.Prepared.Operation
	}
	var proof nativeWriteProof
	if err == nil {
		var prepared catalog.PreparedMutation
		prepared, err = s.store.PreparedMutation(ctx, manifest.Tenant, operation)
		if err == nil {
			proof, err = nativeWriteIntentProof(prepared)
		}
		if err == nil && (prepared.State != catalog.MutationCommitted ||
			prepared.Tenant != manifest.Tenant || prepared.ExpectedHead != manifest.ExpectedHead ||
			prepared.Intent.SourceID != "mount:"+input.Owner ||
			prepared.Intent.Revise == nil || prepared.Intent.Revise.Object != manifest.Object.ID ||
			prepared.Intent.Revise.Spec.Content == nil ||
			prepared.Intent.Revise.Spec.Content.Ref != manifest.Prepared.Ref) {
			err = catalog.ErrMutationConflict
		}
		if err == nil {
			err = validateNativeWriteProof(input.Owner, manifest, proof)
		}
	}
	var record catalog.MutationRecord
	if err == nil {
		record, err = s.store.Mutation(ctx, manifest.Tenant, operation)
	}
	if err == nil && (record.Tenant != manifest.Tenant || record.Kind != catalog.MutationRevise ||
		record.Primary != [16]byte(manifest.Object.ID) ||
		record.Revision != manifest.ExpectedHead+1) {
		err = catalog.ErrIntegrity
	}
	if err == nil {
		response.Object, err = s.store.LookupAt(
			ctx, manifest.Tenant, manifest.Presentation, catalog.ObjectID(record.Primary), record.Revision,
		)
	}
	if err == nil && (response.Object.ID != manifest.Object.ID ||
		response.Object.ContentRevision != manifest.Object.ContentRevision+1 ||
		response.Object.Hash != manifest.Prepared.Ref.Hash ||
		response.Object.Size != manifest.Prepared.Ref.Size) {
		err = catalog.ErrIntegrity
	}
	if err == nil {
		prior := manifest.Last
		manifest.Last = &nativeCommittedWrite{
			Operation: operation, Proof: proof, Object: response.Object,
			Pin: manifest.Prepared.Pin,
		}
		manifest.Object = response.Object
		manifest.ExpectedHead = record.Revision
		manifest.Dirty = false
		manifest.Prepared = nil
		err = s.saveWrite(manifest)
		if err == nil && prior != nil {
			err = s.closeAndForgetMutationPin(context.WithoutCancel(ctx), prior.Pin)
		}
	}
	if err == nil {
		response.Operation = operation
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) handleAbortWrite(ctx context.Context, request wire.Request) (any, error) {
	var input abortWriteRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(abortWriteResponse{Header: decodeError(err)})
	}
	response := abortWriteResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _, err := s.authorizeWrite(input.Owner, input.Token)
	if errors.Is(err, catalog.ErrHandleClosed) {
		err = nil
	}
	if err == nil {
		err = s.removeWrite(ctx, input.Token)
	}
	response.Header.Error = encodeRemoteError(err)
	return encodeResponse(response)
}

func (s *server) removeWrite(ctx context.Context, token string) error {
	manifestPath, dataPath, err := s.writePaths(token)
	if err != nil {
		return err
	}
	manifest, loadErr := s.loadWrite(token)
	if loadErr != nil && !errors.Is(loadErr, catalog.ErrHandleClosed) {
		return loadErr
	}
	if loadErr == nil {
		if manifest.Prepared != nil {
			err = errors.Join(err, s.closeAndForgetMutationPin(ctx, manifest.Prepared.Pin))
		}
		if manifest.Last != nil {
			err = errors.Join(err, s.closeAndForgetMutationPin(ctx, manifest.Last.Pin))
		}
		if err != nil {
			return err
		}
	}
	var result error
	for _, path := range []string{manifestPath, dataPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (s *server) closeOwnerWrites(ctx context.Context, owner string) error {
	if err := os.MkdirAll(s.writeRoot, 0o700); err != nil {
		return err
	}
	var result error
	err := visitNativeWriteEntries(s.writeRoot, func(entry os.DirEntry) error {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			return nil
		}
		token := strings.TrimSuffix(entry.Name(), ".json")
		manifest, loadErr := s.loadWrite(token)
		if loadErr != nil {
			result = errors.Join(result, loadErr)
			return nil
		}
		if manifest.Owner == owner {
			result = errors.Join(result, s.removeWrite(ctx, token))
		}
		return nil
	})
	return errors.Join(result, err)
}

func (c *Client) OpenWriteAt(
	ctx context.Context,
	token, owner string,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (catalog.Object, error) {
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, err
	}
	response, err := call[openWriteAtResponse](ctx, c.wire, OperationOpenWriteAt, openWriteAtRequest{
		Header: header, Token: token, Owner: owner, Tenant: tenant, Presentation: presentation,
		Generation: generation, ID: id, Revision: revision,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, err
	}
	return response.Object, nil
}

func (c *Client) ReadWriteAt(
	ctx context.Context,
	owner, token string,
	offset int64,
	limit int,
) ([]byte, bool, error) {
	if offset < 0 || limit <= 0 || limit > maxNativeIOBytes {
		return nil, false, catalog.ErrInvalidObject
	}
	header, err := c.header()
	if err != nil {
		return nil, false, err
	}
	response, err := call[readWriteAtResponse](ctx, c.wire, OperationReadWriteAt, readWriteAtRequest{
		Header: header, Owner: owner, Token: token, Offset: offset, Limit: limit,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return nil, false, err
	}
	return response.Data, response.EOF, nil
}

func (c *Client) WriteAt(
	ctx context.Context,
	owner, token string,
	offset int64,
	data []byte,
) (int, error) {
	if offset < 0 || len(data) == 0 || len(data) > maxNativeIOBytes {
		return 0, catalog.ErrStorageQuota
	}
	header, err := c.header()
	if err != nil {
		return 0, err
	}
	response, err := call[writeAtResponse](ctx, c.wire, OperationWriteAt, writeAtRequest{
		Header: header, Owner: owner, Token: token, Offset: offset, Data: data,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return 0, err
	}
	return response.Written, nil
}

func (c *Client) TruncateWrite(ctx context.Context, owner, token string, size int64) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[truncateWriteResponse](ctx, c.wire, OperationTruncateWrite, truncateWriteRequest{
		Header: header, Owner: owner, Token: token, Size: size,
	})
	return validateResponse(header, response.Header, err)
}

func (c *Client) SyncWrite(ctx context.Context, owner, token string) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[syncWriteResponse](ctx, c.wire, OperationSyncWrite, syncWriteRequest{
		Header: header, Owner: owner, Token: token,
	})
	return validateResponse(header, response.Header, err)
}

func (c *Client) SealAndBeginWrite(
	ctx context.Context,
	owner, token string,
) (nativeWriteSealResult, error) {
	header, err := c.header()
	if err != nil {
		return nativeWriteSealResult{}, err
	}
	response, err := call[sealAndBeginWriteResponse](
		ctx, c.wire, OperationSealAndBeginWrite,
		sealAndBeginWriteRequest{Header: header, Owner: owner, Token: token},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return nativeWriteSealResult{}, err
	}
	return response.Result, nil
}

func (c *Client) ResolveCommittedWrite(
	ctx context.Context,
	owner, token string,
) (NativeWriteCommit, error) {
	header, err := c.header()
	if err != nil {
		return NativeWriteCommit{}, err
	}
	response, err := call[resolveCommittedWriteResponse](
		ctx, c.wire, OperationResolveCommittedWrite,
		resolveCommittedWriteRequest{Header: header, Owner: owner, Token: token},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return NativeWriteCommit{}, err
	}
	if response.Operation == (catalog.MutationID{}) {
		return NativeWriteCommit{}, catalog.ErrIntegrity
	}
	return NativeWriteCommit{OperationID: response.Operation, Object: response.Object}, nil
}

func (c *Client) AbortWrite(ctx context.Context, owner, token string) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[abortWriteResponse](ctx, c.wire, OperationAbortWrite, abortWriteRequest{
		Header: header, Owner: owner, Token: token,
	})
	return validateResponse(header, response.Header, err)
}

func (m *Manager) OpenWriteAt(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (string, catalog.Object, error) {
	type result struct {
		token  string
		object catalog.Object
	}
	value, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (result, error) {
		token, tokenErr := newNativeWriteToken()
		if tokenErr != nil {
			return result{}, tokenErr
		}
		object, callErr := managerCall(m, operationCtx, func(client *Client) (catalog.Object, error) {
			return client.OpenWriteAt(
				operationCtx, token, owner, tenant, presentation, generation, id, revision,
			)
		})
		if callErr != nil {
			cleanupCtx, cancel := context.WithTimeout(
				context.WithoutCancel(operationCtx), m.config.OperationTimeout,
			)
			cleanupErr := m.handleCall(cleanupCtx, func(client *Client) error {
				return client.AbortWrite(cleanupCtx, owner, token)
			})
			cancel()
			return result{}, errors.Join(callErr, cleanupErr)
		}
		return result{token: token, object: object}, nil
	})
	return value.token, value.object, err
}

func (m *Manager) ReadWriteAt(
	ctx context.Context,
	owner, token string,
	offset int64,
	limit int,
) ([]byte, bool, error) {
	type result struct {
		data []byte
		eof  bool
	}
	value, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (result, error) {
		return managerCall(m, operationCtx, func(client *Client) (result, error) {
			data, eof, callErr := client.ReadWriteAt(operationCtx, owner, token, offset, limit)
			return result{data: data, eof: eof}, callErr
		})
	})
	return value.data, value.eof, err
}

func (m *Manager) WriteAt(
	ctx context.Context,
	owner, token string,
	offset int64,
	data []byte,
) (int, error) {
	return nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (int, error) {
		return managerCall(m, operationCtx, func(client *Client) (int, error) {
			return client.WriteAt(operationCtx, owner, token, offset, data)
		})
	})
}

func (m *Manager) TruncateWrite(ctx context.Context, owner, token string, size int64) error {
	_, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, m.handleCall(operationCtx, func(client *Client) error {
			return client.TruncateWrite(operationCtx, owner, token, size)
		})
	})
	return err
}

func (m *Manager) SyncWrite(ctx context.Context, owner, token string) error {
	_, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, m.handleCall(operationCtx, func(client *Client) error {
			return client.SyncWrite(operationCtx, owner, token)
		})
	})
	return err
}

func (m *Manager) CommitWriteAt(
	ctx context.Context,
	owner, token string,
) (NativeWriteCommit, error) {
	preparer, err := m.tenantPreparer()
	if err != nil {
		return NativeWriteCommit{}, err
	}
	return nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (NativeWriteCommit, error) {
		unlock, err := m.lockNativeToken(operationCtx, token)
		if err != nil {
			return NativeWriteCommit{}, err
		}
		defer unlock()
		commitCtx, cancel := context.WithTimeout(operationCtx, m.config.OperationTimeout)
		defer cancel()
		sealed, err := managerCall(m, commitCtx, func(client *Client) (nativeWriteSealResult, error) {
			return client.SealAndBeginWrite(commitCtx, owner, token)
		})
		if err != nil {
			return NativeWriteCommit{}, err
		}
		if sealed.Object != nil {
			if sealed.Operation == (catalog.MutationID{}) {
				return NativeWriteCommit{}, catalog.ErrIntegrity
			}
			return NativeWriteCommit{OperationID: sealed.Operation, Object: *sealed.Object}, nil
		}
		if sealed.Prepared == nil || sealed.Prepared.OperationID == (catalog.MutationID{}) ||
			sealed.Prepared.OperationID != sealed.Operation ||
			sealed.Prepared.ExpectedHead == 0 || sealed.Generation == 0 {
			return NativeWriteCommit{}, fmt.Errorf("%w: invalid mutable write seal", catalog.ErrIntegrity)
		}
		if err := preparer(
			commitCtx, sealed.Prepared.Tenant, sealed.Generation,
			sealed.Prepared.ExpectedHead+1,
		); err != nil {
			return NativeWriteCommit{}, err
		}
		commit, err := managerCall(m, commitCtx, func(client *Client) (NativeWriteCommit, error) {
			return client.ResolveCommittedWrite(commitCtx, owner, token)
		})
		if err == nil && commit.OperationID != sealed.Prepared.OperationID {
			err = catalog.ErrIntegrity
		}
		return commit, err
	})
}

func (m *Manager) AbortWrite(ctx context.Context, owner, token string) error {
	_, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, m.handleCall(operationCtx, func(client *Client) error {
			return client.AbortWrite(operationCtx, owner, token)
		})
	})
	return err
}
