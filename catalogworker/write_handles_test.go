package catalogworker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestNativeWriteRecoveryPreservesWorkerGenerationAndClearsManagerEpoch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	firstRuntime := writeTokenForTest(1)
	if err := recoverNativeWrites(root, firstRuntime); err != nil {
		t.Fatal(err)
	}
	service := &server{writeRoot: root}
	manifest := writeManifestForTest(2, "owner-a")
	manifest.Dirty = true
	persistWriteForTest(t, service, manifest, 7)

	if err := recoverNativeWrites(root, firstRuntime); err != nil {
		t.Fatalf("same manager epoch recovery: %v", err)
	}
	if _, err := service.loadWrite(manifest.Token); err != nil {
		t.Fatalf("worker generation lost durable stage: %v", err)
	}

	if err := recoverNativeWrites(root, writeTokenForTest(4)); err != nil {
		t.Fatalf("new manager epoch recovery: %v", err)
	}
	if _, err := service.loadWrite(manifest.Token); !errors.Is(err, catalog.ErrHandleClosed) {
		t.Fatalf("manager restart retained orphan handle: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nativeWriteRuntime {
		t.Fatalf("recovered files = %v, want runtime marker only", entries)
	}
}

func TestNativeWriteAuditRejectsOversizedDirectoryBeforeUnboundedCollection(t *testing.T) {
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	if err := recoverNativeWrites(root, writeTokenForTest(1)); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= maxNativeStageFiles; index++ {
		path := filepath.Join(root, writeTokenForTest(index+1)+".data")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := auditNativeWrites(root); !errors.Is(err, catalog.ErrStorageQuota) {
		t.Fatalf("oversized native stage audit = %v, want storage quota", err)
	}
}

func TestNativeWriteAggregateByteBudgetsIncludeSparseGrowth(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		service := newWriteServerForTest(t)
		persistWriteForTest(t, service, writeManifestForTest(1, "owner-a"), 600<<20)
		if err := service.writeCapacity("owner-a", 500<<20, false); !errors.Is(err, catalog.ErrStorageQuota) {
			t.Fatalf("owner sparse growth = %v, want storage quota", err)
		}
	})

	t.Run("runtime", func(t *testing.T) {
		service := newWriteServerForTest(t)
		for index := 1; index <= 4; index++ {
			persistWriteForTest(
				t, service, writeManifestForTest(index, fmt.Sprintf("owner-%d", index)),
				maxWriteStageSize,
			)
		}
		if err := service.writeCapacity("new-owner", 1, false); !errors.Is(err, catalog.ErrStorageQuota) {
			t.Fatalf("runtime sparse growth = %v, want storage quota", err)
		}
	})
}

func TestSeedNativeWriteJoinsSourceCloseWhenOpenFails(t *testing.T) {
	source := &trackingReadCloser{Reader: strings.NewReader("content")}
	err := seedNativeWrite(
		filepath.Join(t.TempDir(), "stage"), int64(len("content")), source,
		func(string, int, os.FileMode) (*os.File, error) {
			return nil, syscall.ENOSPC
		},
	)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("seed error = %v, want ENOSPC", err)
	}
	if !source.closed {
		t.Fatal("seed open failure did not close source")
	}
}

type trackingReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}

func newWriteServerForTest(t *testing.T) *server {
	t.Helper()
	root := filepath.Join(t.TempDir(), "catalog.sqlite.native-writes")
	if err := recoverNativeWrites(root, writeTokenForTest(1)); err != nil {
		t.Fatal(err)
	}
	return &server{writeRoot: root}
}

func persistWriteForTest(
	t *testing.T, service *server, manifest nativeWriteManifest, size int64,
) {
	t.Helper()
	_, dataPath, err := service.writePaths(manifest.Token)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := service.saveWrite(manifest); err != nil {
		t.Fatal(err)
	}
}

func writeManifestForTest(index int, owner string) nativeWriteManifest {
	tenant, err := catalog.NewTenantID(fmt.Sprintf("write-test-%d", index))
	if err != nil {
		panic(err)
	}
	var object catalog.ObjectID
	object[15] = byte(index)
	return nativeWriteManifest{
		Token: writeTokenForTest(index), Owner: owner, Tenant: tenant,
		Presentation: catalog.PresentationMount, Generation: 1,
		Object: catalog.Object{
			ID: object, Tenant: tenant, Kind: catalog.KindFile, Revision: 1,
			ContentRevision: 1, Size: 0, Mode: 0o600,
			Visibility: catalog.Visibility{Mount: true},
		},
		ExpectedHead: 1,
	}
}

func writeTokenForTest(index int) string {
	return fmt.Sprintf("%032x", index)
}

func newMutableWriteManagerForTest(
	t *testing.T,
) (*Manager, catalog.TenantProvision, catalog.Object, catalog.Revision) {
	t.Helper()
	manager, _ := newTestManager(t)
	fixture := installCurrentWorkerTenantForTest(
		t, manager, testTenantProvision(t, "snapshot-handle"),
	)
	fixture.Provision = provisionWorkerTenantStateForTest(t, manager, fixture.Provision)
	return manager, fixture.Provision, fixture.Object, fixture.Revision
}
