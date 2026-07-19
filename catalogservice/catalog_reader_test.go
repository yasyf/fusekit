package catalogservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogReaderScopesWorkingSetToAuthorizedRoute(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
	}
	reader := CatalogReader{Store: store}
	authorization := Authorization{
		Presentation: catalog.PresentationFileProvider,
		Route: Route{
			Tenant: tenantID, Generation: 7, Domain: catalogproto.DomainID("domain"),
		},
	}
	_, err := reader.Snapshot(context.Background(), authorization, tenantID, catalog.EnumerationScope{
		Kind:         catalog.EnumerationWorkingSet,
		Presentation: catalog.PresentationMount,
		Domain:       causal.DomainID("forged"),
		Generation:   99,
	}, 3, catalog.SnapshotCursor{}, 17)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if store.snapshotScope.Presentation != catalog.PresentationFileProvider ||
		store.snapshotScope.Domain != causal.DomainID("domain") ||
		store.snapshotScope.Generation != 7 {
		t.Fatalf("authorized snapshot scope = %#v", store.snapshotScope)
	}
}

func TestCatalogReaderOpenUsesRemoteContentStream(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	objectID := catalog.ObjectID{1}
	content := &trackingReadCloser{Reader: bytes.NewBufferString("content")}
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
		openObject: catalog.Object{
			Tenant: tenantID, ID: objectID, Revision: 4, MetadataRevision: 4, Kind: catalog.KindFile,
			Visibility: catalog.Visibility{FileProvider: true},
		},
		openContent: content,
	}
	reader := CatalogReader{Store: store}
	opened, err := reader.OpenAt(context.Background(), Authorization{
		Presentation: catalog.PresentationFileProvider,
	}, tenantID, 7, objectID, 4)
	if err != nil {
		t.Fatalf("OpenAt(): %v", err)
	}
	got, err := io.ReadAll(opened.Content)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}
	if string(got) != "content" || opened.Object != store.openObject {
		t.Fatalf("OpenAt() = %#v, %q", opened.Object, got)
	}
	if err := opened.Content.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if !content.closed {
		t.Fatal("remote content stream was not closed")
	}
}

func TestCatalogReaderRejectsPresentationBeforeRemoteRead(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentMount),
	}
	_, err := (CatalogReader{Store: store}).OpenAt(context.Background(), Authorization{
		Presentation: catalog.PresentationFileProvider,
	}, tenantID, 7, catalog.ObjectID{1}, 4)
	if err == nil {
		t.Fatal("OpenAt() unexpectedly admitted an absent presentation")
	}
}

func TestCatalogReaderRejectsAndClosesWrongRemoteContentIdentity(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	closeErr := errors.New("worker reap failed")
	content := &trackingReadCloser{Reader: bytes.NewReader(nil), closeErr: closeErr}
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
		openObject: catalog.Object{
			Tenant: tenantID, ID: catalog.ObjectID{2}, Revision: 4, MetadataRevision: 4, Kind: catalog.KindFile,
			Visibility: catalog.Visibility{FileProvider: true},
		},
		openContent: content,
	}
	_, err := (CatalogReader{Store: store}).OpenAt(context.Background(), Authorization{
		Presentation: catalog.PresentationFileProvider,
	}, tenantID, 7, catalog.ObjectID{1}, 4)
	if err == nil {
		t.Fatal("OpenAt() unexpectedly accepted the wrong remote object")
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("OpenAt() = %v, want remote close failure", err)
	}
	if !content.closed {
		t.Fatal("wrong remote content stream was not closed")
	}
}

func TestCatalogReaderRejectsAndClosesOverBudgetRemoteContentMetadata(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	objectID := catalog.ObjectID{1}
	content := &trackingReadCloser{Reader: bytes.NewReader(nil)}
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
		openObject: catalog.Object{
			Tenant: tenantID, ID: objectID, Revision: 4, MetadataRevision: 4,
			Name:       strings.Repeat("x", remoteObjectWireBudget),
			Kind:       catalog.KindFile,
			Visibility: catalog.Visibility{FileProvider: true},
		},
		openContent: content,
	}
	_, err := (CatalogReader{Store: store}).OpenAt(context.Background(), Authorization{
		Presentation: catalog.PresentationFileProvider,
	}, tenantID, 7, objectID, 4)
	if !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("OpenAt() = %v, want ErrIntegrity", err)
	}
	if !content.closed {
		t.Fatal("over-budget remote content stream was not closed")
	}
}

func TestCatalogReaderRejectsZeroRemoteHead(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
	}
	_, err := (CatalogReader{Store: store}).Head(
		context.Background(),
		Authorization{Presentation: catalog.PresentationFileProvider},
		tenantID,
	)
	if !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Head() = %v, want ErrIntegrity", err)
	}
}

func TestCatalogReaderValidatesSelfParentedRemoteRoot(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	rootID := catalog.ObjectID{1}
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
		rootObject: catalog.Object{
			Tenant: tenantID, ID: rootID, Parent: rootID, Revision: 1, MetadataRevision: 1,
			Kind: catalog.KindDirectory, Visibility: catalog.Visibility{FileProvider: true},
		},
	}
	reader := CatalogReader{Store: store}
	authorization := Authorization{Presentation: catalog.PresentationFileProvider}
	if _, err := reader.Root(context.Background(), authorization, tenantID); err != nil {
		t.Fatalf("Root(): %v", err)
	}
	store.rootObject.Parent = catalog.ObjectID{}
	if _, err := reader.Root(context.Background(), authorization, tenantID); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Root(wrong parent) = %v, want ErrIntegrity", err)
	}
}

func TestCatalogReaderRejectsCorruptRemoteSnapshotPage(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{1}
	object := catalog.Object{
		Tenant: tenantID, ID: catalog.ObjectID{2}, Parent: parent,
		Revision: 3, MetadataRevision: 3, Kind: catalog.KindFile,
		Visibility: catalog.Visibility{FileProvider: true},
	}
	nextID := catalog.ObjectID{3}
	tests := map[string]catalog.SnapshotPage{
		"wrong tenant": {
			Revision: 3,
			Objects:  []catalog.Object{func() catalog.Object { copy := object; copy.Tenant = "other"; return copy }()},
		},
		"wrong parent": {
			Revision: 3,
			Objects:  []catalog.Object{func() catalog.Object { copy := object; copy.Parent = catalog.ObjectID{9}; return copy }()},
		},
		"forged continuation": {
			Revision: 3,
			Objects:  []catalog.Object{object},
			Next:     &catalog.SnapshotCursor{After: &nextID},
		},
	}
	for name, page := range tests {
		t.Run(name, func(t *testing.T) {
			store := &recordingCatalogReadStore{
				metadata:     readerTenantMetadata(tenantID, catalog.PresentFileProvider),
				snapshotPage: page,
			}
			_, err := (CatalogReader{Store: store}).Snapshot(
				context.Background(),
				Authorization{Presentation: catalog.PresentationFileProvider},
				tenantID,
				catalog.EnumerationScope{
					Kind: catalog.EnumerationContainer, Parent: parent,
				},
				3,
				catalog.SnapshotCursor{},
				1,
			)
			if !errors.Is(err, catalog.ErrIntegrity) {
				t.Fatalf("Snapshot() = %v, want ErrIntegrity", err)
			}
		})
	}
}

func TestCatalogReaderSnapshotEnforcesExactCountAndSemanticWireBounds(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{42}
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
	}
	reader := CatalogReader{Store: store}
	authorization := Authorization{Presentation: catalog.PresentationFileProvider}
	scope := catalog.EnumerationScope{Kind: catalog.EnumerationContainer, Parent: parent}
	if _, err := reader.Snapshot(
		context.Background(), authorization, tenantID, scope, 3,
		catalog.SnapshotCursor{}, int(catalogproto.MaxPageSize)+1,
	); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("Snapshot(over count) = %v, want ErrInvalidObject", err)
	}
	if store.snapshotCalls != 0 {
		t.Fatalf("over-count request reached remote store %d times", store.snapshotCalls)
	}

	store.snapshotPage = catalog.SnapshotPage{
		Revision: 3,
		Objects:  exactRemoteBudgetObjects(tenantID, parent, 3),
	}
	if _, err := reader.Snapshot(
		context.Background(), authorization, tenantID, scope, 3,
		catalog.SnapshotCursor{}, len(store.snapshotPage.Objects),
	); err != nil {
		t.Fatalf("Snapshot(exact wire budget): %v", err)
	}
	store.snapshotPage.Objects = append(
		store.snapshotPage.Objects,
		remoteBudgetObjects(tenantID, parent, 3, "x", 1)...,
	)
	if _, err := reader.Snapshot(
		context.Background(), authorization, tenantID, scope, 3,
		catalog.SnapshotCursor{}, len(store.snapshotPage.Objects),
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Snapshot(over wire budget) = %v, want ErrIntegrity", err)
	}
}

func TestCatalogReaderRejectsCorruptRemoteChangePage(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{1}
	cursor := catalog.CompleteChangeCursor(1)
	object := catalog.Object{
		Tenant: tenantID, ID: catalog.ObjectID{2}, Parent: parent,
		Revision: 2, MetadataRevision: 2, Kind: catalog.KindFile,
		Visibility: catalog.Visibility{FileProvider: true},
	}
	valid := catalog.ChangePage{
		Floor: 1, Head: 2, Next: catalog.CompleteChangeCursor(2), Complete: true,
		Changes: []catalog.Change{{
			Revision: 2, Sequence: 0, Kind: catalog.ChangeUpsert, Object: object,
		}},
	}
	tests := map[string]func(*catalog.ChangePage){
		"unknown kind": func(page *catalog.ChangePage) {
			page.Changes[0].Kind = 99
		},
		"future object": func(page *catalog.ChangePage) {
			page.Changes[0].Object.Revision = 3
		},
		"wrong continuation": func(page *catalog.ChangePage) {
			page.Next = catalog.ChangeCursor{Revision: 2, Sequence: 0}
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			page := valid
			page.Changes = append([]catalog.Change(nil), valid.Changes...)
			corrupt(&page)
			store := &recordingCatalogReadStore{
				metadata:   readerTenantMetadata(tenantID, catalog.PresentFileProvider),
				changePage: page,
			}
			_, err := (CatalogReader{Store: store}).ChangesSince(
				context.Background(),
				Authorization{Presentation: catalog.PresentationFileProvider},
				tenantID,
				catalog.EnumerationScope{
					Kind: catalog.EnumerationContainer, Parent: parent,
				},
				cursor,
				1,
			)
			if !errors.Is(err, catalog.ErrIntegrity) {
				t.Fatalf("ChangesSince() = %v, want ErrIntegrity", err)
			}
		})
	}
}

func TestCatalogReaderChangesEnforcesExactSemanticWireBound(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{42}
	cursor := catalog.CompleteChangeCursor(1)
	store := &recordingCatalogReadStore{
		metadata: readerTenantMetadata(tenantID, catalog.PresentFileProvider),
	}
	reader := CatalogReader{Store: store}
	authorization := Authorization{Presentation: catalog.PresentationFileProvider}
	scope := catalog.EnumerationScope{Kind: catalog.EnumerationContainer, Parent: parent}
	store.changePage = exactRemoteBudgetChanges(tenantID, parent)
	if _, err := reader.ChangesSince(
		context.Background(), authorization, tenantID, scope, cursor, len(store.changePage.Changes),
	); err != nil {
		t.Fatalf("ChangesSince(exact wire budget): %v", err)
	}
	extra := remoteBudgetObjects(tenantID, parent, 2, "x", 1)[0]
	store.changePage.Changes = append(store.changePage.Changes, catalog.Change{
		Revision: 2, Sequence: uint32(len(store.changePage.Changes)), Kind: catalog.ChangeUpsert, Object: extra,
	})
	if _, err := reader.ChangesSince(
		context.Background(), authorization, tenantID, scope, cursor, len(store.changePage.Changes),
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("ChangesSince(over wire budget) = %v, want ErrIntegrity", err)
	}
}

func TestProtocolConversionEnforcesSemanticBoundsBeforeAllocation(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{42}
	objects := exactRemoteBudgetObjects(tenantID, parent, 3)
	if converted, err := protocolObjects(objects); err != nil || len(converted) != len(objects) {
		t.Fatalf("protocolObjects(exact page) = %d, %v", len(converted), err)
	}
	if _, err := protocolObjects(append(
		objects, remoteBudgetObjects(tenantID, parent, 3, "x", 1)...,
	)); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("protocolObjects(over page) = %v, want ErrIntegrity", err)
	}
	if _, err := protocolObjects(remoteBudgetObjects(
		tenantID, parent, 3, "x", int(catalogproto.MaxPageSize)+1,
	)); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("protocolObjects(over count) = %v, want ErrIntegrity", err)
	}
	overObject := objects[0]
	overObject.Name = strings.Repeat("x", remoteObjectWireBudget)
	if _, err := protocolObject(overObject); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("protocolObject(over object) = %v, want ErrIntegrity", err)
	}

	exactChanges := exactRemoteBudgetChanges(tenantID, parent).Changes
	if converted, err := protocolChanges(exactChanges); err != nil || len(converted) != len(exactChanges) {
		t.Fatalf("protocolChanges(exact page) = %d, %v", len(converted), err)
	}
	overChanges := append(append([]catalog.Change(nil), exactChanges...), catalog.Change{
		Revision: 2, Sequence: uint32(len(exactChanges)), Kind: catalog.ChangeUpsert,
		Object: remoteBudgetObjects(tenantID, parent, 2, "x", 1)[0],
	})
	if _, err := protocolChanges(overChanges); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("protocolChanges(over page) = %v, want ErrIntegrity", err)
	}
	overCountChanges := remoteBudgetChanges(
		tenantID, parent, "x", int(catalogproto.MaxPageSize)+1,
	).Changes
	if _, err := protocolChanges(overCountChanges); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("protocolChanges(over count) = %v, want ErrIntegrity", err)
	}
}

func TestCatalogReaderLookupNameValidatesRemoteBindingWithTenantCasePolicy(t *testing.T) {
	tenantID := catalog.TenantID("tenant")
	parent := catalog.ObjectID{1}
	store := &recordingCatalogReadStore{
		metadata: func() catalog.TenantMetadata {
			metadata := readerTenantMetadata(tenantID, catalog.PresentFileProvider)
			metadata.CasePolicy = catalog.CaseInsensitive
			return metadata
		}(),
		lookupNameObject: catalog.Object{
			Tenant: tenantID, ID: catalog.ObjectID{2}, Parent: parent,
			Revision: 2, MetadataRevision: 2, Name: "Straße", Kind: catalog.KindFile,
			Visibility: catalog.Visibility{FileProvider: true},
		},
	}
	reader := CatalogReader{Store: store}
	if _, err := reader.LookupName(
		context.Background(),
		Authorization{Presentation: catalog.PresentationFileProvider},
		tenantID,
		parent,
		"STRASSE",
	); err != nil {
		t.Fatalf("LookupName(equivalent): %v", err)
	}
	store.lookupNameObject.Parent = catalog.ObjectID{9}
	if _, err := reader.LookupName(
		context.Background(),
		Authorization{Presentation: catalog.PresentationFileProvider},
		tenantID,
		parent,
		"STRASSE",
	); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("LookupName(wrong parent) = %v, want ErrIntegrity", err)
	}
}

type recordingCatalogReadStore struct {
	CatalogReadStore
	metadata         catalog.TenantMetadata
	rootObject       catalog.Object
	snapshotScope    catalog.EnumerationScope
	snapshotCalls    int
	snapshotPage     catalog.SnapshotPage
	changePage       catalog.ChangePage
	lookupNameObject catalog.Object
	openObject       catalog.Object
	openContent      io.ReadCloser
}

func readerTenantMetadata(tenant catalog.TenantID, presentations catalog.PresentationSet) catalog.TenantMetadata {
	return catalog.TenantMetadata{
		Tenant: tenant, Root: catalog.ObjectID{1}, CasePolicy: catalog.CaseSensitive,
		Presentations: presentations,
	}
}

func remoteBudgetObjects(
	tenant catalog.TenantID,
	parent catalog.ObjectID,
	revision catalog.Revision,
	name string,
	count int,
) []catalog.Object {
	objects := make([]catalog.Object, count)
	for index := range objects {
		var id catalog.ObjectID
		id[len(id)-2] = byte((index + 1) >> 8)
		id[len(id)-1] = byte(index + 1)
		objects[index] = catalog.Object{
			Tenant: tenant, ID: id, Parent: parent, Revision: revision, MetadataRevision: revision,
			Name: name, Kind: catalog.KindDirectory, Visibility: catalog.Visibility{FileProvider: true},
		}
	}
	return objects
}

func exactRemoteBudgetObjects(
	tenant catalog.TenantID,
	parent catalog.ObjectID,
	revision catalog.Revision,
) []catalog.Object {
	objects := make([]catalog.Object, 0, 60)
	appendSymlink := func(name, target string) {
		index := len(objects)
		var id catalog.ObjectID
		id[len(id)-2] = byte((index + 1) >> 8)
		id[len(id)-1] = byte(index + 1)
		objects = append(objects, catalog.Object{
			Tenant: tenant, ID: id, Parent: parent, Revision: revision, MetadataRevision: revision,
			ContentRevision: revision, Name: name, Kind: catalog.KindSymlink,
			Size: int64(len(target)), Hash: sha256.Sum256([]byte(target)), LinkTarget: target,
			Visibility: catalog.Visibility{FileProvider: true},
		})
	}
	for range 57 {
		appendSymlink(strings.Repeat("n", int(catalogproto.MaxNameBytes)), strings.Repeat("t", 4096))
	}
	for range 3 {
		appendSymlink("n", strings.Repeat("t", 1178))
	}
	return objects
}

func remoteBudgetChanges(
	tenant catalog.TenantID,
	parent catalog.ObjectID,
	name string,
	count int,
) catalog.ChangePage {
	objects := remoteBudgetObjects(tenant, parent, 2, name, count)
	changes := make([]catalog.Change, count)
	for index, object := range objects {
		changes[index] = catalog.Change{
			Revision: 2, Sequence: uint32(index), Kind: catalog.ChangeUpsert, Object: object,
		}
	}
	return catalog.ChangePage{
		Floor: 1, Head: 2, Next: catalog.CompleteChangeCursor(2), Complete: true, Changes: changes,
	}
}

func exactRemoteBudgetChanges(tenant catalog.TenantID, parent catalog.ObjectID) catalog.ChangePage {
	objects := exactRemoteBudgetObjects(tenant, parent, 2)
	objects = objects[:58]
	objects[len(objects)-1].Name = "n"
	objects[len(objects)-1].LinkTarget = strings.Repeat("t", 2652)
	objects[len(objects)-1].Size = int64(len(objects[len(objects)-1].LinkTarget))
	objects[len(objects)-1].Hash = sha256.Sum256([]byte(objects[len(objects)-1].LinkTarget))
	changes := make([]catalog.Change, len(objects))
	for index, object := range objects {
		changes[index] = catalog.Change{
			Revision: 2, Sequence: uint32(index), Kind: catalog.ChangeUpsert, Object: object,
		}
	}
	return catalog.ChangePage{
		Floor: 1, Head: 2, Next: catalog.CompleteChangeCursor(2), Complete: true, Changes: changes,
	}
}

func (s *recordingCatalogReadStore) Tenant(context.Context, catalog.TenantID) (catalog.TenantMetadata, error) {
	return s.metadata, nil
}

func (s *recordingCatalogReadStore) Root(context.Context, catalog.TenantID) (catalog.Object, error) {
	return s.rootObject, nil
}

func (s *recordingCatalogReadStore) Head(context.Context, catalog.TenantID) (catalog.Revision, error) {
	return 0, nil
}

func (s *recordingCatalogReadStore) Snapshot(
	_ context.Context,
	_ catalog.TenantID,
	scope catalog.EnumerationScope,
	revision catalog.Revision,
	_ catalog.SnapshotCursor,
	_ int,
) (catalog.SnapshotPage, error) {
	s.snapshotScope = scope
	s.snapshotCalls++
	if s.snapshotPage.Revision == 0 {
		return catalog.SnapshotPage{Revision: revision}, nil
	}
	return s.snapshotPage, nil
}

func (s *recordingCatalogReadStore) ChangesSince(
	context.Context,
	catalog.TenantID,
	catalog.EnumerationScope,
	catalog.ChangeCursor,
	int,
) (catalog.ChangePage, error) {
	return s.changePage, nil
}

func (s *recordingCatalogReadStore) LookupName(
	context.Context,
	catalog.TenantID,
	catalog.Presentation,
	catalog.ObjectID,
	string,
) (catalog.Object, error) {
	return s.lookupNameObject, nil
}

func (s *recordingCatalogReadStore) OpenContentAt(
	context.Context,
	catalog.TenantID,
	catalog.Presentation,
	catalog.Generation,
	catalog.ObjectID,
	catalog.Revision,
) (catalog.Object, io.ReadCloser, error) {
	return s.openObject, s.openContent, nil
}

type trackingReadCloser struct {
	io.Reader
	closed   bool
	closeErr error
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return r.closeErr
}
