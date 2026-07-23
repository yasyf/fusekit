package holder

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/mountservice"
)

type nativeCatalog struct {
	worker *catalogworker.Manager
}

func newNativeCatalog(worker *catalogworker.Manager) (*nativeCatalog, error) {
	if worker == nil {
		return nil, errors.New("FuseKit runtime: catalog worker is nil")
	}
	return &nativeCatalog{worker: worker}, nil
}

func (c *nativeCatalog) OpenSnapshot(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (mountservice.NativeHandle, error) {
	token, object, err := c.worker.OpenSnapshotAt(
		ctx, owner, tenant, catalog.PresentationMount, generation, id, revision,
	)
	return mountservice.NativeHandle{Token: token, Object: object}, err
}

func (c *nativeCatalog) ReadSnapshot(
	ctx context.Context,
	owner, token string,
	offset int64,
	limit int,
) ([]byte, bool, error) {
	return c.worker.ReadSnapshotAt(ctx, owner, token, offset, limit)
}

func (c *nativeCatalog) CloseSnapshot(ctx context.Context, owner, token string) error {
	return c.worker.CloseSnapshot(ctx, owner, token)
}

func (c *nativeCatalog) OpenWrite(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (mountservice.NativeHandle, error) {
	token, object, err := c.worker.OpenWriteAt(
		ctx, owner, tenant, catalog.PresentationMount, generation, id, revision,
	)
	return mountservice.NativeHandle{Token: token, Object: object}, err
}

func (c *nativeCatalog) ReadWrite(
	ctx context.Context,
	owner, token string,
	offset int64,
	limit int,
) ([]byte, bool, error) {
	return c.worker.ReadWriteAt(ctx, owner, token, offset, limit)
}

func (c *nativeCatalog) Write(
	ctx context.Context,
	owner, token string,
	offset int64,
	data []byte,
) (int, error) {
	return c.worker.WriteAt(ctx, owner, token, offset, data)
}

func (c *nativeCatalog) Truncate(ctx context.Context, owner, token string, size int64) error {
	return c.worker.TruncateWrite(ctx, owner, token, size)
}

func (c *nativeCatalog) Sync(ctx context.Context, owner, token string) error {
	return c.worker.SyncWrite(ctx, owner, token)
}

func (c *nativeCatalog) CommitWrite(
	ctx context.Context,
	owner, token string,
) (catalog.Object, catalog.MutationID, error) {
	committed, err := c.worker.CommitWriteAt(ctx, owner, token)
	return committed.Object, committed.OperationID, err
}

func (c *nativeCatalog) AbortWrite(ctx context.Context, owner, token string) error {
	return c.worker.AbortWrite(ctx, owner, token)
}

func (c *nativeCatalog) CloseSession(ctx context.Context, owner string) error {
	return c.worker.CloseNativeSession(ctx, owner)
}

var _ mountservice.NativeCatalog = (*nativeCatalog)(nil)
