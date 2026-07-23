package mountservice

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
)

const nativeIOChunkLimit = 1 << 20

func (s *Server) handleNativeSnapshotOpen(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeSnapshotOpenRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeSnapshotOpenError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeSnapshotOpen)
	if err == nil {
		defer finish()
	}
	tenantID, objectID, generation := nativeObjectRoute(input.TenantID, input.Generation, input.ObjectID)
	if err == nil {
		err = errors.Join(tenantID.err, objectID.err)
	}
	if err == nil {
		err = state.authorizeRoute(tenantID.value, generation)
	}
	var opened NativeHandle
	if err == nil {
		opened, err = s.config.Native.Catalog.OpenSnapshot(
			ctx, state.owner, tenantID.value, generation, objectID.value, catalog.Revision(input.Revision),
		)
	}
	if err == nil {
		err = validateOpenedHandle(opened, tenantID.value, objectID.value, catalog.Revision(input.Revision))
	}
	if err == nil {
		err = state.addHandle(opened.Token, nativeHandle{
			kind: nativeSnapshotHandle, tenant: tenantID.value, generation: generation,
			object: objectID.value, revision: catalog.Revision(input.Revision),
		})
		if err != nil {
			err = errors.Join(err, s.config.Native.Catalog.CloseSnapshot(ctx, state.owner, opened.Token))
		}
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeSnapshotOpenError(code, errors.New(message))
	}
	object := protocolNativeObject(opened.Object)
	return encoded(mountproto.NativeSnapshotOpenResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Handle: opened.Token, Object: &object,
	})
}

func (s *Server) handleNativeSnapshotRead(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeSnapshotReadRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeSnapshotReadError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeSnapshotRead)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeSnapshotHandle)
	}
	if err == nil && (input.Offset < 0 || input.Length == 0 || input.Length > nativeIOChunkLimit) {
		err = catalog.ErrInvalidObject
	}
	var data []byte
	var eof bool
	if err == nil {
		data, eof, err = s.config.Native.Catalog.ReadSnapshot(ctx, state.owner, input.Handle, input.Offset, int(input.Length))
	}
	if err == nil && len(data) > int(input.Length) {
		err = catalog.ErrIntegrity
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeSnapshotReadError(code, errors.New(message))
	}
	return encoded(mountproto.NativeSnapshotReadResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Data: data, EOF: eof,
	})
}

func (s *Server) handleNativeSnapshotClose(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeSnapshotCloseRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeSnapshotCloseError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeSnapshotClose)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeSnapshotHandle)
	}
	if err == nil {
		err = s.config.Native.Catalog.CloseSnapshot(ctx, state.owner, input.Handle)
	}
	if err == nil {
		_, err = state.removeHandle(input.Handle, nativeSnapshotHandle)
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeSnapshotCloseError(code, errors.New(message))
	}
	return encoded(mountproto.NativeSnapshotCloseResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Handle: input.Handle,
	})
}

func (s *Server) handleNativeWriteOpen(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteOpenRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteOpenError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteOpen)
	if err == nil {
		defer finish()
	}
	tenantID, objectID, generation := nativeObjectRoute(input.TenantID, input.Generation, input.ObjectID)
	if err == nil {
		err = errors.Join(tenantID.err, objectID.err)
	}
	if err == nil {
		err = state.authorizeRoute(tenantID.value, generation)
	}
	var opened NativeHandle
	if err == nil {
		opened, err = s.config.Native.Catalog.OpenWrite(
			ctx, state.owner, tenantID.value, generation, objectID.value, catalog.Revision(input.Revision),
		)
	}
	if err == nil {
		err = validateOpenedHandle(opened, tenantID.value, objectID.value, catalog.Revision(input.Revision))
	}
	if err == nil {
		err = state.addHandle(opened.Token, nativeHandle{
			kind: nativeWriteHandle, tenant: tenantID.value, generation: generation,
			object: objectID.value, revision: catalog.Revision(input.Revision),
		})
		if err != nil {
			err = errors.Join(err, s.config.Native.Catalog.AbortWrite(ctx, state.owner, opened.Token))
		}
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteOpenError(code, errors.New(message))
	}
	object := protocolNativeObject(opened.Object)
	return encoded(mountproto.NativeWriteOpenResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Handle: opened.Token, Object: &object,
	})
}

func (s *Server) handleNativeWriteRead(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteReadRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteReadError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteRead)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	if err == nil && (input.Offset < 0 || input.Length == 0 || input.Length > nativeIOChunkLimit) {
		err = catalog.ErrInvalidObject
	}
	var data []byte
	var eof bool
	if err == nil {
		data, eof, err = s.config.Native.Catalog.ReadWrite(ctx, state.owner, input.Handle, input.Offset, int(input.Length))
	}
	if err == nil && len(data) > int(input.Length) {
		err = catalog.ErrIntegrity
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteReadError(code, errors.New(message))
	}
	return encoded(mountproto.NativeWriteReadResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Data: data, EOF: eof,
	})
}

func (s *Server) handleNativeWrite(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteWriteRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteWriteError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteWrite)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	if err == nil && (input.Offset < 0 || len(input.Data) == 0 || len(input.Data) > nativeIOChunkLimit) {
		err = catalog.ErrInvalidObject
	}
	var written int
	if err == nil {
		written, err = s.config.Native.Catalog.Write(ctx, state.owner, input.Handle, input.Offset, input.Data)
	}
	if err == nil && written != len(input.Data) {
		err = catalog.ErrIntegrity
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteWriteError(code, errors.New(message))
	}
	return encoded(mountproto.NativeWriteWriteResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Written: uint32(written),
	})
}

func (s *Server) handleNativeWriteTruncate(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteTruncateRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteTruncateError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteTruncate)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	if err == nil && input.Size < 0 {
		err = catalog.ErrInvalidObject
	}
	if err == nil {
		err = s.config.Native.Catalog.Truncate(ctx, state.owner, input.Handle, input.Size)
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteTruncateError(code, errors.New(message))
	}
	return encoded(mountproto.NativeWriteTruncateResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Size: input.Size,
	})
}

func (s *Server) handleNativeWriteSync(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteSyncRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteSyncError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteSync)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	if err == nil {
		err = s.config.Native.Catalog.Sync(ctx, state.owner, input.Handle)
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteSyncError(code, errors.New(message))
	}
	return encoded(mountproto.NativeWriteSyncResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Handle: input.Handle,
	})
}

func (s *Server) handleNativeWriteCommit(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteCommitRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteCommitError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteCommit)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	var operation catalog.MutationID
	var object catalog.Object
	if err == nil {
		object, operation, err = s.config.Native.Catalog.CommitWrite(ctx, state.owner, input.Handle)
	}
	if err == nil {
		err = state.acceptWriteCommit(input.Handle, operation, object)
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteCommitError(code, errors.New(message))
	}
	encodedObject := protocolNativeObject(object)
	return encoded(mountproto.NativeWriteCommitResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		Handle: input.Handle, MutationID: mountproto.MutationID(operation.String()), Object: &encodedObject,
	})
}

func (s *Server) handleNativeWriteAbort(ctx context.Context, request wire.Request) (any, error) {
	var input mountproto.NativeWriteAbortRequest
	if err := mountproto.Decode(request.Payload, &input); err != nil {
		return nativeWriteAbortError(mountproto.ErrorCodeInvalidRequest, err)
	}
	state, finish, err := s.boundNative(ctx, request, mountproto.OperationNativeWriteAbort)
	if err == nil {
		defer finish()
	}
	if err == nil {
		_, err = state.handle(input.Handle, nativeWriteHandle)
	}
	if err == nil {
		err = s.config.Native.Catalog.AbortWrite(ctx, state.owner, input.Handle)
	}
	if err == nil {
		_, err = state.removeHandle(input.Handle, nativeWriteHandle)
	}
	if err != nil {
		code, message := applicationError(err)
		return nativeWriteAbortError(code, errors.New(message))
	}
	return encoded(mountproto.NativeWriteAbortResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk, Handle: input.Handle,
	})
}

type parsed[T any] struct {
	value T
	err   error
}

func nativeObjectRoute(tenantValue mountproto.TenantID, generationValue uint64, objectValue string) (parsed[catalog.TenantID], parsed[catalog.ObjectID], catalog.Generation) {
	tenantID, tenantErr := catalog.NewTenantID(string(tenantValue))
	objectID, objectErr := catalog.ParseObjectID(objectValue)
	generation := catalog.Generation(generationValue)
	if generation == 0 {
		tenantErr = errors.Join(tenantErr, catalog.ErrGenerationMismatch)
	}
	return parsed[catalog.TenantID]{value: tenantID, err: tenantErr},
		parsed[catalog.ObjectID]{value: objectID, err: objectErr}, generation
}

func validateOpenedHandle(handle NativeHandle, tenantID catalog.TenantID, objectID catalog.ObjectID, revision catalog.Revision) error {
	if handle.Token == "" || handle.Object.Tenant != tenantID || handle.Object.ID != objectID ||
		handle.Object.Revision != revision || handle.Object.Kind != catalog.KindFile {
		return catalog.ErrIntegrity
	}
	return nil
}

func protocolNativeObject(object catalog.Object) mountproto.NativeObject {
	kind := mountproto.ObjectKindDirectory
	switch object.Kind {
	case catalog.KindFile:
		kind = mountproto.ObjectKindFile
	case catalog.KindSymlink:
		kind = mountproto.ObjectKindSymlink
	}
	return mountproto.NativeObject{
		ID: object.ID.String(), ParentID: object.Parent.String(), Name: object.Name, Kind: kind,
		Mode: object.Mode, Size: object.Size, Hash: hex.EncodeToString(object.Hash[:]), LinkTarget: object.LinkTarget,
		Revision: uint64(object.Revision), MetadataRevision: uint64(object.MetadataRevision),
		ContentRevision: uint64(object.ContentRevision), Desired: uint64(object.Convergence.Desired),
		Observed: uint64(object.Convergence.Observed), Verified: uint64(object.Convergence.Verified),
		Applied: uint64(object.Convergence.Applied),
	}
}

func nativeSnapshotOpenError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeSnapshotOpenResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeSnapshotReadError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeSnapshotReadResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeSnapshotCloseError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeSnapshotCloseResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteOpenError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteOpenResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteReadError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteReadResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteWriteError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteWriteResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteTruncateError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteTruncateResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteSyncError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteSyncResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteCommitError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteCommitResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}

func nativeWriteAbortError(code mountproto.ErrorCode, err error) (any, error) {
	return encoded(mountproto.NativeWriteAbortResponse{Protocol: mountproto.Version, Code: code, Message: err.Error()})
}
