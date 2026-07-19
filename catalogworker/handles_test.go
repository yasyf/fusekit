package catalogworker

import (
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestSnapshotCloseReplaysUntilExactForget(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		token, _, err := client.OpenSnapshotAt(
			t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
			provision.Generation, object.ID, revision,
		)
		if err != nil {
			return struct{}{}, err
		}
		if err := rawCloseSnapshot(t, client, "owner-a", token); err != nil {
			return struct{}{}, err
		}
		if err := rawCloseSnapshot(t, client, "owner-a", token); err != nil {
			t.Fatalf("close replay: %v", err)
		}
		if err := rawCloseSnapshot(t, client, "owner-b", token); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("wrong-owner close = %v, want handle closed", err)
		}
		if err := rawForgetSnapshot(t, client, "owner-b", token); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("wrong-owner forget = %v, want handle closed", err)
		}
		if err := rawForgetSnapshot(t, client, "owner-a", token); err != nil {
			return struct{}{}, err
		}
		if err := rawForgetSnapshot(t, client, "owner-a", token); err != nil {
			t.Fatalf("forget replay: %v", err)
		}
		if err := rawCloseSnapshot(t, client, "owner-a", token); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("close after acknowledged forget = %v, want handle closed", err)
		}
		wrongGeneration := "wrong." + strings.Repeat("0", 32)
		if err := rawForgetSnapshot(t, client, "owner-a", wrongGeneration); !errors.Is(err, catalog.ErrInvalidObject) {
			t.Fatalf("wrong-generation forget = %v, want invalid object", err)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotForgetKeepsSequentialCapacityBounded(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	for index := 0; index < maxOwnerHandles+1; index++ {
		token, _, err := manager.OpenSnapshotAt(
			t.Context(), "owner-a", provision.Tenant, catalog.PresentationMount,
			provision.Generation, object.ID, revision,
		)
		if err != nil {
			t.Fatalf("OpenSnapshotAt(%d): %v", index, err)
		}
		if err := manager.CloseSnapshot(t.Context(), "owner-a", token); err != nil {
			t.Fatalf("CloseSnapshot(%d): %v", index, err)
		}
	}
}

func rawCloseSnapshot(t *testing.T, client *Client, owner, token string) error {
	t.Helper()
	header, err := client.header()
	if err != nil {
		return err
	}
	response, err := call[closeSnapshotResponse](
		t.Context(), client.wire, OperationCloseSnapshot,
		closeSnapshotRequest{Header: header, Owner: owner, Token: token},
	)
	return validateResponse(header, response.Header, err)
}

func rawForgetSnapshot(t *testing.T, client *Client, owner, token string) error {
	t.Helper()
	header, err := client.header()
	if err != nil {
		return err
	}
	response, err := call[forgetSnapshotResponse](
		t.Context(), client.wire, OperationForgetSnapshot,
		forgetSnapshotRequest{Header: header, Owner: owner, Token: token},
	)
	return validateResponse(header, response.Header, err)
}
