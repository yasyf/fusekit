package sourcedriverservice

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

const testAuthority causal.SourceAuthorityID = "source-one"

func TestExactServiceStreamsSnapshotsChangesContentAndMutations(t *testing.T) {
	driver := newTestDriver()
	client := startSourceDriverClient(t, driver)
	ctx := context.Background()
	targetSet := declareServiceTestTargetSet(t, client)

	head, err := client.Refresh(ctx, testAuthority)
	if err != nil || head.Revision != "head-2" {
		t.Fatalf("Refresh = %+v, %v", head, err)
	}
	snapshotRequest := sourcedriver.SnapshotRequest{TargetSet: targetSet, Revision: head.Revision, Limit: 16}
	page, err := client.Snapshot(ctx, testAuthority, snapshotRequest)
	if err != nil || len(page.Objects) != 1 || page.Objects[0].ID != "settings" {
		t.Fatalf("Snapshot = %+v, %v", page, err)
	}
	changesRequest := sourcedriver.ChangesRequest{TargetSet: targetSet, From: "head-1", To: "head-2", Limit: 16}
	changes, err := client.ChangesSince(ctx, testAuthority, changesRequest)
	if err != nil || len(changes.Changes) != 1 || changes.Changes[0].ID != "settings" {
		t.Fatalf("ChangesSince = %+v, %v", changes, err)
	}

	opened, err := client.OpenContent(ctx, testAuthority, *page.Objects[0].Content)
	if err != nil {
		t.Fatalf("OpenContent: %v", err)
	}
	body, err := io.ReadAll(opened)
	if err != nil || string(body) != "settings-body" {
		t.Fatalf("ReadAll = %q, %v", body, err)
	}
	if err := opened.Settle(nil); err != nil {
		t.Fatalf("Settle open: %v", err)
	}
	if err := opened.Wait(ctx); err != nil {
		t.Fatalf("Wait open: %v", err)
	}

	mutation, source := testMutation(targetSet, "head-2", []byte("next-body"))
	receipt, err := client.ApplyMutation(ctx, testAuthority, mutation, source)
	if err != nil {
		t.Fatalf("ApplyMutation: %v", err)
	}
	if receipt.State != sourcedriver.MutationApplied || receipt.Committed != "head-3" || receipt.OperationID != mutation.OperationID {
		t.Fatalf("ApplyMutation receipt = %+v", receipt)
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(mutation)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := client.InspectMutation(ctx, testAuthority, mutation.OperationID, requestDigest)
	if err != nil || replayed != receipt {
		t.Fatalf("InspectMutation = %+v, %v; want %+v", replayed, err, receipt)
	}
	wrongDigest := requestDigest
	wrongDigest[0] ^= 0xff
	_, err = client.InspectMutation(ctx, testAuthority, mutation.OperationID, wrongDigest)
	var inspectConflict *RemoteError
	if !errors.As(err, &inspectConflict) || inspectConflict.Code != sourcedriverproto.ErrorCodeConflict {
		t.Fatalf("cross-request inspection error = %#v", err)
	}
	_, replaySource := testMutation(targetSet, "head-2", []byte("next-body"))
	reapplied, err := client.ApplyMutation(ctx, testAuthority, mutation, replaySource)
	if err != nil || reapplied != receipt {
		t.Fatalf("idempotent ApplyMutation = %+v, %v; want %+v", reapplied, err, receipt)
	}
	collision, collisionSource := testMutation(targetSet, "head-2", []byte("other-body"))
	_, err = client.ApplyMutation(ctx, testAuthority, collision, collisionSource)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != sourcedriverproto.ErrorCodeConflict {
		t.Fatalf("operation-id collision error = %#v", err)
	}
	deleteRequest := testContentlessMutation(targetSet)
	deleted, err := client.ApplyMutation(ctx, testAuthority, deleteRequest, nil)
	if err != nil || deleted.State != sourcedriver.MutationApplied || deleted.OperationID != deleteRequest.OperationID {
		t.Fatalf("contentless ApplyMutation = %+v, %v", deleted, err)
	}
}

func TestLostApplyResponseRecoversByExactOperationInspection(t *testing.T) {
	driver := newTestDriver()
	driver.blockApplied = make(chan struct{})
	driver.applied = make(chan struct{})
	client := startSourceDriverClient(t, driver)
	targetSet := declareServiceTestTargetSet(t, client)
	request, content := testMutation(targetSet, "head-2", []byte("next-body"))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.ApplyMutation(ctx, testAuthority, request, content)
		result <- err
	}()
	select {
	case <-driver.applied:
	case <-time.After(5 * time.Second):
		t.Fatal("driver did not durably apply mutation")
	}
	cancel()
	close(driver.blockApplied)
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("lost response unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled apply did not return")
	}

	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.InspectMutation(context.Background(), testAuthority, request.OperationID, requestDigest)
	if err != nil {
		t.Fatalf("InspectMutation: %v", err)
	}
	if receipt.State != sourcedriver.MutationApplied || receipt.Committed != "head-3" || receipt.OperationID != request.OperationID {
		t.Fatalf("recovered receipt = %+v", receipt)
	}
	_ = readRetainedMutationView(t, client, targetSet, receipt)
}

func TestAppliedReceiptPinsExactViewUntilDurableAcknowledgement(t *testing.T) {
	driver := newTestDriver()
	client := startSourceDriverClient(t, driver)
	originalTarget := declareServiceTestTargetSet(t, client)
	request, content := testMutation(originalTarget, "head-2", []byte("next-body"))
	receipt, err := client.ApplyMutation(t.Context(), testAuthority, request, content)
	if err != nil {
		t.Fatal(err)
	}
	baseline := readRetainedMutationView(t, client, originalTarget, receipt)
	head, err := client.Refresh(t.Context(), testAuthority)
	if err != nil || head.Revision != receipt.Committed {
		t.Fatalf("head after apply = %+v, %v; want %q", head, err, receipt.Committed)
	}
	newerRequest := testContentlessMutation(originalTarget)
	newerReceipt, err := client.ApplyMutation(t.Context(), testAuthority, newerRequest, nil)
	if err != nil || newerReceipt.Committed != "head-4" {
		t.Fatalf("advance beyond retained receipt = %+v, %v", newerReceipt, err)
	}
	head, err = client.Refresh(t.Context(), testAuthority)
	if err != nil || head.Revision != newerReceipt.Committed {
		t.Fatalf("newer head = %+v, %v; want %q", head, err, newerReceipt.Committed)
	}
	newerPage, err := client.Snapshot(t.Context(), testAuthority, sourcedriver.SnapshotRequest{
		TargetSet: originalTarget, Revision: newerReceipt.Committed, Limit: 16,
	})
	if err != nil || len(newerPage.Objects) != 2 {
		t.Fatalf("observably newer snapshot = %+v, %v", newerPage, err)
	}
	for _, object := range newerPage.Objects {
		if object.ID == "settings" {
			t.Fatal("head-4 still contains the settings object deleted after the retained receipt")
		}
	}

	currentTargets, currentTarget := serviceTestExpandedTargetSet(t, 2)
	declareServiceTargetSetPages(t, client, currentTargets, currentTarget)
	if currentTarget.TargetsDigest == originalTarget.TargetsDigest || currentTarget.TargetCount == originalTarget.TargetCount {
		t.Fatal("target churn did not change membership and digest")
	}
	restarted := startProcessSourceDriverClient(t, driver)
	replayedView := readRetainedMutationView(t, restarted, originalTarget, receipt)
	if !reflect.DeepEqual(replayedView, baseline) {
		t.Fatalf("view after process restart differs\n got: %+v\nwant: %+v", replayedView, baseline)
	}
	currentView := readRetainedMutationView(t, restarted, currentTarget, receipt)
	if len(currentView.Snapshots) <= len(baseline.Snapshots) {
		t.Fatalf("changed target membership did not expand snapshot: %d <= %d",
			len(currentView.Snapshots), len(baseline.Snapshots))
	}

	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.InspectMutation(
		t.Context(), testAuthority, request.OperationID, requestDigest,
	)
	if err != nil || replayed != receipt {
		t.Fatalf("receipt after service restart = %+v, %v; want %+v", replayed, err, receipt)
	}

	acknowledgement := mutationSettlementForTest(
		t, request, receipt, sourcedriver.MutationSettlementAcknowledge,
	)
	restartedDriver := driver.restart()
	settlementClient := startSourceDriverClient(t, restartedDriver)
	if err := settlementClient.SettleMutation(t.Context(), testAuthority, acknowledgement); err != nil {
		t.Fatalf("acknowledge retained view: %v", err)
	}
	afterAckDriver := restartedDriver.restart()
	afterAck := startSourceDriverClient(t, afterAckDriver)
	if err := afterAck.SettleMutation(t.Context(), testAuthority, acknowledgement); err != nil {
		t.Fatalf("acknowledgement replay after restart and pin release: %v", err)
	}
	replayed, err = afterAck.InspectMutation(t.Context(), testAuthority, request.OperationID, requestDigest)
	if err != nil || replayed != receipt {
		t.Fatalf("receipt proof after acknowledgement = %+v, %v; want %+v", replayed, err, receipt)
	}

	retainedAfterAck := readRetainedMutationView(t, afterAck, currentTarget, receipt)
	if !reflect.DeepEqual(retainedAfterAck, currentView) {
		t.Fatalf("driver-retained view changed after acknowledgement")
	}

	forget := acknowledgement
	forget.Kind = sourcedriver.MutationSettlementForget
	if err := afterAck.SettleMutation(t.Context(), testAuthority, forget); err != nil {
		t.Fatalf("forget acknowledged receipt: %v", err)
	}
	forgotten, err := afterAck.InspectMutation(t.Context(), testAuthority, request.OperationID, requestDigest)
	if err != nil || forgotten.State != sourcedriver.MutationNotFound {
		t.Fatalf("forgotten receipt = %+v, %v", forgotten, err)
	}
	consumed := startProcessSourceDriverClient(t, afterAckDriver)
	for name, body := range map[string][]byte{
		"same request":      []byte("next-body"),
		"different request": []byte("other-body"),
	} {
		t.Run(name, func(t *testing.T) {
			reused, source := testMutation(originalTarget, "head-2", body)
			if _, err := consumed.ApplyMutation(t.Context(), testAuthority, reused, source); !errors.Is(err, sourcedriver.ErrConflict) {
				t.Fatalf("forgotten operation reuse = %v, want conflict", err)
			}
		})
	}
}

func TestAcknowledgementPermitsReleasingRetainedView(t *testing.T) {
	driver := newTestDriver()
	driver.releaseOnAck = true
	client := startSourceDriverClient(t, driver)
	targetSet := declareServiceTestTargetSet(t, client)
	request, content := testMutation(targetSet, "head-2", []byte("next-body"))
	receipt, err := client.ApplyMutation(t.Context(), testAuthority, request, content)
	if err != nil {
		t.Fatal(err)
	}
	_ = readRetainedMutationView(t, client, targetSet, receipt)
	acknowledgement := mutationSettlementForTest(
		t, request, receipt, sourcedriver.MutationSettlementAcknowledge,
	)
	if err := client.SettleMutation(t.Context(), testAuthority, acknowledgement); err != nil {
		t.Fatal(err)
	}
	restarted := startSourceDriverClient(t, driver.restart())
	if err := restarted.SettleMutation(t.Context(), testAuthority, acknowledgement); err != nil {
		t.Fatalf("ack replay after release: %v", err)
	}
	if _, err := restarted.Snapshot(t.Context(), testAuthority, sourcedriver.SnapshotRequest{
		TargetSet: targetSet, Revision: receipt.Committed, Limit: 1,
	}); !errors.Is(err, sourcedriver.ErrNotFound) {
		t.Fatalf("released snapshot = %v, want not found", err)
	}
	_, err = restarted.ChangesSince(t.Context(), testAuthority, sourcedriver.ChangesRequest{
		TargetSet: targetSet, From: receipt.Expected, To: receipt.Committed, Limit: 1,
	})
	var required *sourcedriver.SnapshotRequiredError
	if !errors.As(err, &required) || required.From != receipt.Expected || required.Head != receipt.Committed {
		t.Fatalf("released delta = %#v", err)
	}
	ref := *testCreatedObject(1).Content
	released, openErr := restarted.OpenContent(t.Context(), testAuthority, ref)
	if openErr == nil {
		_, readErr := io.ReadAll(released)
		settleErr := released.Settle(readErr)
		waitErr := released.Wait(t.Context())
		openErr = errors.Join(readErr, settleErr, waitErr)
	}
	if !errors.Is(openErr, sourcedriver.ErrNotFound) {
		t.Fatalf("released content = %v, want not found", openErr)
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.InspectMutation(t.Context(), testAuthority, request.OperationID, requestDigest)
	if err != nil || replayed != receipt {
		t.Fatalf("receipt proof after release = %+v, %v; want %+v", replayed, err, receipt)
	}
}

type retainedMutationView struct {
	Snapshots []sourcedriver.SnapshotPage
	Changes   []sourcedriver.ChangePage
	Bodies    map[sourcedriver.ContentRef]string
}

func readRetainedMutationView(
	t *testing.T,
	client *Client,
	targetSet sourcedriver.TargetSetRef,
	receipt sourcedriver.MutationReceipt,
) retainedMutationView {
	t.Helper()
	view := retainedMutationView{Bodies: make(map[sourcedriver.ContentRef]string)}
	var snapshotCursor *sourcedriver.PageCursor
	var result *sourcedriver.Projection
	for {
		page, err := client.Snapshot(t.Context(), testAuthority, sourcedriver.SnapshotRequest{
			TargetSet: targetSet, Revision: receipt.Committed, Cursor: snapshotCursor, Limit: 1,
		})
		if err != nil {
			t.Fatalf("retained snapshot page %d: %v", len(view.Snapshots), err)
		}
		view.Snapshots = append(view.Snapshots, page)
		for index := range page.Objects {
			if page.Objects[index].ID == receipt.Result {
				copy := page.Objects[index]
				result = &copy
			}
		}
		if page.Next == nil {
			break
		}
		snapshotCursor = page.Next
	}
	if result == nil || result.Content == nil || result.Name != "created.json" ||
		result.Content.Revision != receipt.Committed || result.Size != int64(len("next-body")) ||
		result.Hash != catalog.ContentHash(sha256.Sum256([]byte("next-body"))) {
		t.Fatalf("retained snapshot has no exact mutation result: %+v", result)
	}
	var changeCursor *sourcedriver.PageCursor
	foundResult := false
	for {
		page, err := client.ChangesSince(t.Context(), testAuthority, sourcedriver.ChangesRequest{
			TargetSet: targetSet, From: receipt.Expected, To: receipt.Committed,
			Cursor: changeCursor, Limit: 1,
		})
		if err != nil {
			t.Fatalf("retained change page %d: %v", len(view.Changes), err)
		}
		view.Changes = append(view.Changes, page)
		for _, change := range page.Changes {
			if change.ID == receipt.Result {
				if change.Object == nil || !reflect.DeepEqual(*change.Object, *result) {
					t.Fatalf("retained result change = %+v; want %+v", change, result)
				}
				foundResult = true
			}
		}
		if page.Next == nil {
			break
		}
		changeCursor = page.Next
	}
	if !foundResult || len(view.Changes) < 2 {
		t.Fatalf("retained changes did not page exact mutation result: %+v", view.Changes)
	}
	refs := make(map[sourcedriver.ContentRef]struct{})
	for _, page := range view.Snapshots {
		for _, object := range page.Objects {
			if object.Content != nil {
				refs[*object.Content] = struct{}{}
			}
		}
	}
	for _, page := range view.Changes {
		for _, change := range page.Changes {
			if change.Object != nil && change.Object.Content != nil {
				refs[*change.Object.Content] = struct{}{}
			}
		}
	}
	for ref := range refs {
		opened, err := client.OpenContent(t.Context(), testAuthority, ref)
		if err != nil {
			t.Fatalf("OpenContent(%+v): %v", ref, err)
		}
		body, readErr := io.ReadAll(opened)
		settleErr := opened.Settle(readErr)
		waitErr := opened.Wait(t.Context())
		if readErr != nil || settleErr != nil || waitErr != nil {
			t.Fatalf("retained content = %q, read %v settle %v wait %v", body, readErr, settleErr, waitErr)
		}
		view.Bodies[ref] = string(body)
	}
	if view.Bodies[*result.Content] != "next-body" {
		t.Fatalf("mutation result body = %q, want next-body", view.Bodies[*result.Content])
	}
	return view
}

func TestMutationSettlementTransitionsAreExactAndFailClosed(t *testing.T) {
	driver := newTestDriver()
	client := startSourceDriverClient(t, driver)
	targetSet := declareServiceTestTargetSet(t, client)
	request, content := testMutation(targetSet, "head-2", []byte("next-body"))
	receipt, err := client.ApplyMutation(t.Context(), testAuthority, request, content)
	if err != nil {
		t.Fatal(err)
	}
	applied := mutationSettlementForTest(t, request, receipt, sourcedriver.MutationSettlementAcknowledge)
	for name, mutate := range map[string]func(*causal.SourceAuthorityID, *sourcedriver.MutationSettlement){
		"authority": func(authority *causal.SourceAuthorityID, _ *sourcedriver.MutationSettlement) {
			*authority = "other-authority"
		},
		"generation": func(_ *causal.SourceAuthorityID, value *sourcedriver.MutationSettlement) {
			value.TargetSet.AuthorityGeneration++
		},
		"declaration": func(_ *causal.SourceAuthorityID, value *sourcedriver.MutationSettlement) {
			value.TargetSet.DeclarationDigest[0] ^= 0xff
		},
		"operation": func(_ *causal.SourceAuthorityID, value *sourcedriver.MutationSettlement) {
			value.OperationID[0] ^= 0xff
		},
		"request": func(_ *causal.SourceAuthorityID, value *sourcedriver.MutationSettlement) {
			value.RequestDigest[0] ^= 0xff
		},
		"receipt": func(_ *causal.SourceAuthorityID, value *sourcedriver.MutationSettlement) {
			value.ReceiptDigest[0] ^= 0xff
		},
	} {
		t.Run(name, func(t *testing.T) {
			authority, forged := testAuthority, applied
			mutate(&authority, &forged)
			err := client.SettleMutation(t.Context(), authority, forged)
			if errors.Is(err, sourcedriver.ErrIntegrity) {
				return
			}
			var remote *RemoteError
			if !errors.As(err, &remote) || remote.Code != sourcedriverproto.ErrorCodeConflict {
				t.Fatalf("forged settlement error = %#v", err)
			}
		})
	}
	if err := client.SettleMutation(t.Context(), testAuthority, applied); err != nil {
		t.Fatalf("acknowledge applied: %v", err)
	}
	if err := client.SettleMutation(t.Context(), testAuthority, applied); err != nil {
		t.Fatalf("acknowledge replay: %v", err)
	}
	abandonApplied := applied
	abandonApplied.Kind = sourcedriver.MutationSettlementAbandon
	err = client.SettleMutation(t.Context(), testAuthority, abandonApplied)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != sourcedriverproto.ErrorCodeConflict {
		t.Fatalf("abandon applied error = %#v", err)
	}
	forget := applied
	forget.Kind = sourcedriver.MutationSettlementForget
	if err := client.SettleMutation(t.Context(), testAuthority, forget); err != nil {
		t.Fatalf("forget acknowledged: %v", err)
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	forgotten, err := client.InspectMutation(t.Context(), testAuthority, request.OperationID, requestDigest)
	if err != nil || forgotten.State != sourcedriver.MutationNotFound {
		t.Fatalf("forgotten receipt = %+v, %v", forgotten, err)
	}

	preparedRequest := testContentlessMutation(targetSet)
	preparedRequest.OperationID[0] = 11
	preparedDigest, err := sourcedriver.MutationRequestDigest(preparedRequest)
	if err != nil {
		t.Fatal(err)
	}
	prepared := sourcedriver.MutationReceipt{
		OperationID: preparedRequest.OperationID, State: sourcedriver.MutationPrepared,
		RequestDigest: preparedDigest, Expected: preparedRequest.Expected,
	}
	prepared.Digest, err = sourcedriver.MutationReceiptDigest(prepared)
	if err != nil {
		t.Fatal(err)
	}
	driver.putReceipt(prepared)
	abandon := mutationSettlementForTest(t, preparedRequest, prepared, sourcedriver.MutationSettlementAbandon)
	if err := client.SettleMutation(t.Context(), testAuthority, abandon); err != nil {
		t.Fatalf("abandon prepared: %v", err)
	}
	if err := client.SettleMutation(t.Context(), testAuthority, abandon); err != nil {
		t.Fatalf("abandon replay: %v", err)
	}
	abandoned, err := client.InspectMutation(t.Context(), testAuthority, prepared.OperationID, preparedDigest)
	if err != nil || abandoned.State != sourcedriver.MutationNotFound {
		t.Fatalf("abandoned receipt = %+v, %v", abandoned, err)
	}
}

func TestTypedSnapshotRequiredAndStaleRevisionCrossWireExactly(t *testing.T) {
	driver := newTestDriver()
	client := startSourceDriverClient(t, driver)
	targetSet := declareServiceTestTargetSet(t, client)

	_, err := client.ChangesSince(context.Background(), testAuthority, sourcedriver.ChangesRequest{
		TargetSet: targetSet, From: "compacted", To: "head-2", Limit: 1,
	})
	var snapshot *sourcedriver.SnapshotRequiredError
	if !errors.As(err, &snapshot) || snapshot.From != "compacted" || snapshot.Head != "head-2" {
		t.Fatalf("ChangesSince error = %#v", err)
	}
	request, content := testMutation(targetSet, "head-1", []byte("next-body"))
	_, err = client.ApplyMutation(context.Background(), testAuthority, request, content)
	var stale *sourcedriver.StaleRevisionError
	if !errors.As(err, &stale) || stale.Expected != "head-1" || stale.Actual != "head-2" {
		t.Fatalf("ApplyMutation error = %#v", err)
	}
}

func TestExactBuildMismatchIsRejectedBeforeRegistrationOrDial(t *testing.T) {
	server := &wire.Server{Build: "old-build"}
	if _, err := Register(server, newTestDriver()); err == nil {
		t.Fatal("Register accepted a mismatched build")
	}
	_, err := NewClient(context.Background(), wire.ClientConfig{Build: "old-build"})
	if err == nil {
		t.Fatal("NewClient accepted a mismatched build")
	}
}

func TestTargetSetDeclarationReplayRestartAndABAFences(t *testing.T) {
	driver := newTestDriver()
	client := startSourceDriverClient(t, driver)
	targets := make([]sourcedriver.TargetDeclaration, sourcedriver.MaxTargetPageItems+1)
	for index := range targets {
		targets[index] = sourcedriver.TargetDeclaration{
			Tenant:     catalog.TenantID("target-" + string(rune('a'+index/26)) + string(rune('a'+index%26))),
			Generation: 1,
		}
	}
	refA1, err := sourcedriver.NewTargetSetRef(testAuthority, 1, 1, [32]byte{1}, targets)
	if err != nil {
		t.Fatal(err)
	}
	state, err := sourcedriver.NewTargetSetState(testAuthority, refA1)
	if err != nil {
		t.Fatal(err)
	}
	first, err := sourcedriver.NewTargetSetPage(state, targets[:sourcedriver.MaxTargetPageItems])
	if err != nil {
		t.Fatal(err)
	}
	partial, err := client.DeclareTargetSet(t.Context(), testAuthority, first)
	if err != nil || partial.Complete {
		t.Fatalf("first declaration page = %+v, %v", partial, err)
	}
	replayed, err := client.DeclareTargetSet(t.Context(), testAuthority, first)
	if err != nil || replayed != partial {
		t.Fatalf("exact declaration replay = %+v, %v, want %+v", replayed, err, partial)
	}
	forged := first
	forged.Targets = append([]sourcedriver.TargetDeclaration(nil), first.Targets...)
	forged.Targets[0].Generation++
	if _, err := client.DeclareTargetSet(t.Context(), testAuthority, forged); !errors.Is(err, sourcedriver.ErrIntegrity) {
		t.Fatalf("same digest with different declaration body = %v, want integrity", err)
	}

	restarted := startSourceDriverClient(t, driver)
	recovered, err := restarted.InspectTargetSet(t.Context(), testAuthority, refA1)
	if err != nil || recovered != partial {
		t.Fatalf("restarted declaration state = %+v, %v, want %+v", recovered, err, partial)
	}
	last, err := sourcedriver.NewTargetSetPage(recovered, targets[sourcedriver.MaxTargetPageItems:])
	if err != nil {
		t.Fatal(err)
	}
	complete, err := restarted.DeclareTargetSet(t.Context(), testAuthority, last)
	if err != nil || !complete.Complete || complete.Ref != refA1 {
		t.Fatalf("resumed declaration = %+v, %v", complete, err)
	}

	targetsB := []sourcedriver.TargetDeclaration{{Tenant: "target-z", Generation: 2}}
	refB2, err := sourcedriver.NewTargetSetRef(testAuthority, 1, 2, [32]byte{1}, targetsB)
	if err != nil {
		t.Fatal(err)
	}
	declareServiceTargetSetPages(t, restarted, targetsB, refB2)
	refA3, err := sourcedriver.NewTargetSetRef(testAuthority, 1, 3, [32]byte{1}, targets)
	if err != nil {
		t.Fatal(err)
	}
	declareServiceTargetSetPages(t, restarted, targets, refA3)
	if refA1.ID == refA3.ID || refA1.TargetEpoch >= refB2.TargetEpoch || refB2.TargetEpoch >= refA3.TargetEpoch {
		t.Fatal("target-set refs did not preserve monotonic epoch fencing")
	}
	_, err = restarted.Snapshot(t.Context(), testAuthority, sourcedriver.SnapshotRequest{
		TargetSet: refA1, Revision: "head-2", Limit: 1,
	})
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != sourcedriverproto.ErrorCodeConflict {
		t.Fatalf("ABA reuse of old target ref error = %#v", err)
	}
}

func TestPageProtocolCarriesOnlyTargetSetReferences(t *testing.T) {
	for _, value := range []any{
		sourcedriverproto.SnapshotRequest{},
		sourcedriverproto.ChangesSinceRequest{},
		sourcedriverproto.PageCursor{},
	} {
		typeOf := reflect.TypeOf(value)
		if _, found := typeOf.FieldByName("TargetSet"); !found {
			t.Fatalf("%s has no TargetSet reference", typeOf)
		}
		if _, found := typeOf.FieldByName("Targets"); found {
			t.Fatalf("%s still embeds target declarations", typeOf)
		}
	}
}

type testDriver struct {
	mu           sync.Mutex
	head         sourcedriver.RevisionToken
	targetSets   map[sourcedriver.TargetSetID]sourcedriver.TargetSetState
	targets      map[sourcedriver.TargetSetID][]sourcedriver.TargetDeclaration
	activeTarget sourcedriver.TargetSetRef
	receipts     map[catalog.MutationID]sourcedriver.MutationReceipt
	requests     map[catalog.MutationID][sha256.Size]byte
	mutationSets map[catalog.MutationID]sourcedriver.TargetSetRef
	acknowledged map[catalog.MutationID]sourcedriver.MutationSettlement
	terminal     map[catalog.MutationID]sourcedriver.MutationSettlement
	releaseOnAck bool
	blockApplied chan struct{}
	applied      chan struct{}
}

type testDriverDurableState struct {
	Head         sourcedriver.RevisionToken
	TargetSets   map[sourcedriver.TargetSetID]sourcedriver.TargetSetState
	Targets      map[sourcedriver.TargetSetID][]sourcedriver.TargetDeclaration
	ActiveTarget sourcedriver.TargetSetRef
	Receipts     map[catalog.MutationID]sourcedriver.MutationReceipt
	Requests     map[catalog.MutationID][sha256.Size]byte
	MutationSets map[catalog.MutationID]sourcedriver.TargetSetRef
	Acknowledged map[catalog.MutationID]sourcedriver.MutationSettlement
	Terminal     map[catalog.MutationID]sourcedriver.MutationSettlement
	ReleaseOnAck bool
}

func newTestDriver() *testDriver {
	return &testDriver{
		head:         "head-2",
		targetSets:   make(map[sourcedriver.TargetSetID]sourcedriver.TargetSetState),
		targets:      make(map[sourcedriver.TargetSetID][]sourcedriver.TargetDeclaration),
		receipts:     make(map[catalog.MutationID]sourcedriver.MutationReceipt),
		requests:     make(map[catalog.MutationID][sha256.Size]byte),
		mutationSets: make(map[catalog.MutationID]sourcedriver.TargetSetRef),
		acknowledged: make(map[catalog.MutationID]sourcedriver.MutationSettlement),
		terminal:     make(map[catalog.MutationID]sourcedriver.MutationSettlement),
	}
}

func (d *testDriver) restart() *testDriver {
	return newTestDriverFromDurableState(d.durableState())
}

func (d *testDriver) durableState() testDriverDurableState {
	d.mu.Lock()
	defer d.mu.Unlock()
	state := testDriverDurableState{
		Head: d.head, ActiveTarget: d.activeTarget, ReleaseOnAck: d.releaseOnAck,
		TargetSets:   make(map[sourcedriver.TargetSetID]sourcedriver.TargetSetState),
		Targets:      make(map[sourcedriver.TargetSetID][]sourcedriver.TargetDeclaration),
		Receipts:     make(map[catalog.MutationID]sourcedriver.MutationReceipt),
		Requests:     make(map[catalog.MutationID][sha256.Size]byte),
		MutationSets: make(map[catalog.MutationID]sourcedriver.TargetSetRef),
		Acknowledged: make(map[catalog.MutationID]sourcedriver.MutationSettlement),
		Terminal:     make(map[catalog.MutationID]sourcedriver.MutationSettlement),
	}
	for id, targetState := range d.targetSets {
		state.TargetSets[id] = targetState
	}
	for id, targets := range d.targets {
		state.Targets[id] = append([]sourcedriver.TargetDeclaration(nil), targets...)
	}
	for id, receipt := range d.receipts {
		state.Receipts[id] = receipt
	}
	for id, digest := range d.requests {
		state.Requests[id] = digest
	}
	for id, targetSet := range d.mutationSets {
		state.MutationSets[id] = targetSet
	}
	for id, settlement := range d.acknowledged {
		state.Acknowledged[id] = settlement
	}
	for id, settlement := range d.terminal {
		state.Terminal[id] = settlement
	}
	return state
}

func newTestDriverFromDurableState(state testDriverDurableState) *testDriver {
	restarted := newTestDriver()
	restarted.head = state.Head
	restarted.activeTarget = state.ActiveTarget
	restarted.releaseOnAck = state.ReleaseOnAck
	for id, targetState := range state.TargetSets {
		restarted.targetSets[id] = targetState
	}
	for id, targets := range state.Targets {
		restarted.targets[id] = append([]sourcedriver.TargetDeclaration(nil), targets...)
	}
	for id, receipt := range state.Receipts {
		restarted.receipts[id] = receipt
	}
	for id, digest := range state.Requests {
		restarted.requests[id] = digest
	}
	for id, targetSet := range state.MutationSets {
		restarted.mutationSets[id] = targetSet
	}
	for id, settlement := range state.Acknowledged {
		restarted.acknowledged[id] = settlement
	}
	for id, settlement := range state.Terminal {
		restarted.terminal[id] = settlement
	}
	return restarted
}

func (d *testDriver) Refresh(context.Context, causal.SourceAuthorityID) (sourcedriver.Head, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return sourcedriver.Head{Revision: d.head}, nil
}

func (d *testDriver) InspectTargetSet(
	_ context.Context,
	authority causal.SourceAuthorityID,
	ref sourcedriver.TargetSetRef,
) (sourcedriver.TargetSetState, error) {
	if authority != testAuthority {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrConflict
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	state, found := d.targetSets[ref.ID]
	if !found || state.Ref != ref {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrConflict
	}
	return state, nil
}

func (d *testDriver) DeclareTargetSet(
	_ context.Context,
	authority causal.SourceAuthorityID,
	page sourcedriver.TargetSetPage,
) (sourcedriver.TargetSetState, error) {
	if authority != testAuthority {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrConflict
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	state, found := d.targetSets[page.Ref.ID]
	if !found {
		var err error
		state, err = sourcedriver.NewTargetSetState(authority, page.Ref)
		if err != nil {
			return sourcedriver.TargetSetState{}, err
		}
	}
	next, err := sourcedriver.ApplyTargetSetPage(state, page)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	d.targetSets[page.Ref.ID] = next
	if next.NextPage != state.NextPage {
		d.targets[page.Ref.ID] = append(d.targets[page.Ref.ID], page.Targets...)
	}
	if next.Complete {
		d.activeTarget = next.Ref
	}
	return next, nil
}

func (d *testDriver) requireActiveTargetSet(ref sourcedriver.TargetSetRef) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ref != d.activeTarget || !d.targetSets[ref.ID].Complete {
		return sourcedriver.ErrConflict
	}
	return nil
}

func (d *testDriver) requireReadableTargetSet(
	ref sourcedriver.TargetSetRef,
	revision sourcedriver.RevisionToken,
) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if state, found := d.targetSets[ref.ID]; !found || state.Ref != ref || !state.Complete {
		return sourcedriver.ErrConflict
	}
	if ref == d.activeTarget {
		return nil
	}
	for operation, receipt := range d.receipts {
		if receipt.State == sourcedriver.MutationApplied && receipt.Committed == revision {
			if _, acknowledged := d.acknowledged[operation]; !acknowledged || !d.releaseOnAck {
				return nil
			}
		}
	}
	return sourcedriver.ErrConflict
}

func (d *testDriver) retainsRevision(revision sourcedriver.RevisionToken) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for operation, receipt := range d.receipts {
		if receipt.State == sourcedriver.MutationApplied && receipt.Committed == revision {
			if _, acknowledged := d.acknowledged[operation]; !acknowledged || !d.releaseOnAck {
				return true
			}
		}
	}
	return false
}

func (d *testDriver) Snapshot(_ context.Context, _ causal.SourceAuthorityID, request sourcedriver.SnapshotRequest) (sourcedriver.SnapshotPage, error) {
	if err := d.requireReadableTargetSet(request.TargetSet, request.Revision); err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	if request.Revision != "head-2" && !d.retainsRevision(request.Revision) {
		return sourcedriver.SnapshotPage{}, sourcedriver.ErrNotFound
	}
	objects := d.projections(request.TargetSet, request.Revision)
	start := 0
	pageNumber := uint32(1)
	if request.Cursor != nil {
		start = int(request.Cursor.Page) * request.Limit
		pageNumber = request.Cursor.Page + 1
	}
	end := min(start+request.Limit, len(objects))
	pageObjects := append([]sourcedriver.Projection(nil), objects[start:end]...)
	digest, err := sourcedriver.SnapshotPageDigest(request.Revision, pageObjects)
	if err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	page := sourcedriver.SnapshotPage{Revision: request.Revision, Objects: pageObjects, Digest: digest}
	if end < len(objects) {
		last := pageObjects[len(pageObjects)-1]
		next, err := sourcedriver.NewPageCursor(
			request.TargetSet, sourcedriver.PageSnapshot, "", request.Revision,
			pageNumber, request.Limit,
			sourcedriver.PagePosition{Tenant: last.Tenant, Generation: last.Generation, ID: last.ID},
			nil, digest,
		)
		if err != nil {
			return sourcedriver.SnapshotPage{}, err
		}
		page.Next = &next
	}
	return page, nil
}

func (d *testDriver) ChangesSince(_ context.Context, _ causal.SourceAuthorityID, request sourcedriver.ChangesRequest) (sourcedriver.ChangePage, error) {
	if err := d.requireReadableTargetSet(request.TargetSet, request.To); err != nil {
		return sourcedriver.ChangePage{}, err
	}
	if request.From == "compacted" {
		return sourcedriver.ChangePage{}, &sourcedriver.SnapshotRequiredError{From: request.From, Head: "head-2"}
	}
	if request.From != "head-1" || request.To != "head-2" {
		if request.From != "head-2" || !d.retainsRevision(request.To) {
			return sourcedriver.ChangePage{}, &sourcedriver.SnapshotRequiredError{
				From: request.From, Head: request.To,
			}
		}
	}
	objects := []sourcedriver.Projection{testObject()}
	if request.From == "head-2" && request.To == "head-3" {
		objects = []sourcedriver.Projection{
			testCreatedObject(causal.Generation(1)),
			testRootObject(causal.Generation(1)),
		}
	}
	changes := make([]sourcedriver.Change, len(objects))
	for index := range objects {
		object := objects[index]
		changes[index] = sourcedriver.Change{
			Kind: sourcedriver.ChangeUpsert, Tenant: object.Tenant, Generation: object.Generation,
			Sequence: uint64(index + 1), ID: object.ID, Object: &object,
		}
	}
	start := 0
	pageNumber := uint32(1)
	if request.Cursor != nil {
		start = int(request.Cursor.Page) * request.Limit
		pageNumber = request.Cursor.Page + 1
	}
	end := min(start+request.Limit, len(changes))
	pageChanges := append([]sourcedriver.Change(nil), changes[start:end]...)
	digest, err := sourcedriver.ChangePageDigest(request.From, request.To, pageChanges)
	if err != nil {
		return sourcedriver.ChangePage{}, err
	}
	page := sourcedriver.ChangePage{From: request.From, To: request.To, Changes: pageChanges, Digest: digest}
	if end < len(changes) {
		last := pageChanges[len(pageChanges)-1]
		next, err := sourcedriver.NewPageCursor(
			request.TargetSet, sourcedriver.PageChanges, request.From, request.To,
			pageNumber, request.Limit,
			sourcedriver.PagePosition{
				Tenant: last.Tenant, Generation: last.Generation, Sequence: last.Sequence, ID: last.ID,
			},
			nil, digest,
		)
		if err != nil {
			return sourcedriver.ChangePage{}, err
		}
		page.Next = &next
	}
	return page, nil
}

func (d *testDriver) projections(
	targetSet sourcedriver.TargetSetRef,
	revision sourcedriver.RevisionToken,
) []sourcedriver.Projection {
	d.mu.Lock()
	targets := append([]sourcedriver.TargetDeclaration(nil), d.targets[targetSet.ID]...)
	d.mu.Unlock()
	objects := make([]sourcedriver.Projection, 0, len(targets)+2)
	for _, target := range targets {
		switch target.Tenant {
		case "tenant-one":
			if revision != "head-2" {
				objects = append(objects,
					testCreatedObject(target.Generation),
					testRootObject(target.Generation),
				)
			}
			if revision != "head-4" {
				objects = append(objects, testSettingsObject(target.Generation))
			}
		default:
			objects = append(objects, sourcedriver.Projection{
				Tenant: target.Tenant, Generation: target.Generation,
				ID: "other", Parent: "", Name: "other", Kind: catalog.KindDirectory,
				Mode: 0o755, Visibility: catalog.Visibility{Mount: true, FileProvider: true},
			})
		}
	}
	return objects
}

func (d *testDriver) OpenContent(
	_ context.Context,
	authority causal.SourceAuthorityID,
	ref sourcedriver.ContentRef,
) (contentstream.Source, error) {
	if authority != testAuthority {
		return nil, sourcedriver.ErrConflict
	}
	body := []byte("settings-body")
	if ref.Object == "created" {
		body = []byte("next-body")
	}
	want := sourcedriver.ContentRef{
		Revision: ref.Revision, Tenant: ref.Tenant, Generation: ref.Generation,
		Object: ref.Object, Size: int64(len(body)), Hash: catalog.ContentHash(sha256.Sum256(body)),
	}
	if ref != want || ref.Revision != "head-2" && !d.retainsRevision(ref.Revision) {
		return nil, sourcedriver.ErrNotFound
	}
	return newByteSource(body), nil
}

func (d *testDriver) ApplyMutation(ctx context.Context, _ causal.SourceAuthorityID, request sourcedriver.MutationRequest, content contentstream.Source) (sourcedriver.MutationReceipt, error) {
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	d.mu.Lock()
	priorDigest, seen := d.requests[request.OperationID]
	priorReceipt := d.receipts[request.OperationID]
	_, consumed := d.terminal[request.OperationID]
	actualHead := d.head
	d.mu.Unlock()
	if consumed {
		return sourcedriver.MutationReceipt{}, sourcedriver.ErrConflict
	}
	if seen && priorDigest != requestDigest {
		return sourcedriver.MutationReceipt{}, sourcedriver.ErrConflict
	}
	if seen {
		if content != nil {
			body, err := io.ReadAll(content)
			if err != nil || string(body) != "next-body" {
				return sourcedriver.MutationReceipt{}, errors.Join(sourcedriver.ErrIntegrity, err)
			}
			if err := content.Settle(nil); err != nil {
				return sourcedriver.MutationReceipt{}, err
			}
			if err := content.Wait(ctx); err != nil {
				return sourcedriver.MutationReceipt{}, err
			}
		}
		return priorReceipt, nil
	}
	if err := d.requireActiveTargetSet(request.TargetSet); err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if request.Expected != actualHead {
		return sourcedriver.MutationReceipt{}, &sourcedriver.StaleRevisionError{
			Expected: request.Expected, Actual: actualHead,
		}
	}
	if content != nil {
		body, err := io.ReadAll(content)
		if err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
		if string(body) != "next-body" {
			return sourcedriver.MutationReceipt{}, sourcedriver.ErrIntegrity
		}
		if err := content.Settle(nil); err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
		if err := content.Wait(ctx); err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
	}
	committed := sourcedriver.RevisionToken("head-3")
	if actualHead == "head-3" {
		committed = "head-4"
	}
	result := sourcedriver.LogicalID("")
	if request.Context.Operation.Kind == catalog.MutationCreate {
		result = "created"
	}
	receipt := sourcedriver.MutationReceipt{
		OperationID: request.OperationID, State: sourcedriver.MutationApplied,
		RequestDigest: requestDigest, Expected: request.Expected, Committed: committed, Result: result,
	}
	digest, err := sourcedriver.MutationReceiptDigest(receipt)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	receipt.Digest = digest
	d.mu.Lock()
	d.requests[request.OperationID] = requestDigest
	d.receipts[request.OperationID] = receipt
	d.mutationSets[request.OperationID] = request.TargetSet
	d.head = receipt.Committed
	d.mu.Unlock()
	if d.applied != nil {
		close(d.applied)
		select {
		case <-d.blockApplied:
		case <-ctx.Done():
		}
	}
	return receipt, nil
}

func (d *testDriver) InspectMutation(_ context.Context, _ causal.SourceAuthorityID, id catalog.MutationID, requestDigest [sha256.Size]byte) (sourcedriver.MutationReceipt, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if receipt, ok := d.receipts[id]; ok {
		if receipt.RequestDigest != requestDigest {
			return sourcedriver.MutationReceipt{}, sourcedriver.ErrConflict
		}
		return receipt, nil
	}
	return sourcedriver.MutationReceipt{OperationID: id, State: sourcedriver.MutationNotFound}, nil
}

func (d *testDriver) SettleMutation(
	_ context.Context,
	authority causal.SourceAuthorityID,
	settlement sourcedriver.MutationSettlement,
) error {
	if err := sourcedriver.ValidateMutationSettlement(settlement); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if authority != testAuthority {
		return sourcedriver.ErrConflict
	}
	if terminal, found := d.terminal[settlement.OperationID]; found {
		if terminal == settlement {
			return nil
		}
		return sourcedriver.ErrConflict
	}
	receipt, found := d.receipts[settlement.OperationID]
	if !found || d.mutationSets[settlement.OperationID] != settlement.TargetSet ||
		receipt.RequestDigest != settlement.RequestDigest || receipt.Digest != settlement.ReceiptDigest {
		return sourcedriver.ErrConflict
	}
	switch settlement.Kind {
	case sourcedriver.MutationSettlementAcknowledge:
		if receipt.State != sourcedriver.MutationApplied {
			return sourcedriver.ErrConflict
		}
		if acknowledged, found := d.acknowledged[settlement.OperationID]; found && acknowledged != settlement {
			return sourcedriver.ErrConflict
		}
		d.acknowledged[settlement.OperationID] = settlement
	case sourcedriver.MutationSettlementAbandon:
		if receipt.State != sourcedriver.MutationPrepared {
			return sourcedriver.ErrConflict
		}
		delete(d.receipts, settlement.OperationID)
		delete(d.requests, settlement.OperationID)
		delete(d.mutationSets, settlement.OperationID)
		d.terminal[settlement.OperationID] = settlement
	case sourcedriver.MutationSettlementForget:
		acknowledged := settlement
		acknowledged.Kind = sourcedriver.MutationSettlementAcknowledge
		if receipt.State != sourcedriver.MutationApplied || d.acknowledged[settlement.OperationID] != acknowledged {
			return sourcedriver.ErrConflict
		}
		delete(d.receipts, settlement.OperationID)
		delete(d.requests, settlement.OperationID)
		delete(d.mutationSets, settlement.OperationID)
		delete(d.acknowledged, settlement.OperationID)
		d.terminal[settlement.OperationID] = settlement
	default:
		return sourcedriver.ErrInvalidValue
	}
	return nil
}

func (d *testDriver) putReceipt(receipt sourcedriver.MutationReceipt) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.receipts[receipt.OperationID] = receipt
	d.requests[receipt.OperationID] = receipt.RequestDigest
	d.mutationSets[receipt.OperationID] = d.activeTarget
}

func TestSourceDriverProcessFixture(t *testing.T) {
	statePath := os.Getenv("FUSEKIT_SOURCE_DRIVER_STATE")
	socketPath := os.Getenv("FUSEKIT_SOURCE_DRIVER_SOCKET")
	if statePath == "" && socketPath == "" {
		return
	}
	if statePath == "" || socketPath == "" {
		t.Fatal("source driver process fixture environment is incomplete")
	}
	file, err := os.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state testDriverDurableState
	decodeErr := gob.NewDecoder(file).Decode(&state)
	closeErr := file.Close()
	if decodeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(decodeErr, closeErr))
	}
	driver := newTestDriverFromDurableState(state)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	server := &wire.Server{Build: sourcedriverproto.Build, HandshakeTimeout: time.Second}
	if _, err := Register(server, driver); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
		t.Fatal(err)
	}
	admit := func() (func(), error) { return func() {}, nil }
	if err := server.Serve(context.Background(), listener, func() error { return nil }, admit, admit); err != nil {
		t.Fatal(err)
	}
}

func startProcessSourceDriverClient(t *testing.T, driver *testDriver) *Client {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fk-sd-proc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	statePath := filepath.Join(directory, "driver.gob")
	socketPath := filepath.Join(directory, "socket")
	file, err := os.OpenFile(statePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encodeErr := gob.NewEncoder(file).Encode(driver.durableState())
	closeErr := file.Close()
	if encodeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(encodeErr, closeErr))
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestSourceDriverProcessFixture$", "-test.v")
	command.Env = append(os.Environ(),
		"FUSEKIT_SOURCE_DRIVER_STATE="+statePath,
		"FUSEKIT_SOURCE_DRIVER_SOCKET="+socketPath,
	)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		_ = command.Wait()
	})
	ready := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		var transcript bytes.Buffer
		for {
			line, readErr := reader.ReadString('\n')
			transcript.WriteString(line)
			if line == "READY\n" {
				ready <- nil
				return
			}
			if readErr != nil {
				ready <- errors.Join(
					fmt.Errorf("source driver process fixture output:\n%s", transcript.String()), readErr,
				)
				return
			}
		}
	}()
	select {
	case err := <-ready:
		if err != nil {
			waitErr := command.Wait()
			t.Fatalf("source driver process fixture: %v; wait %v; stderr %s", err, waitErr, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("source driver process fixture did not start")
	}
	client, err := NewClient(context.Background(), wire.ClientConfig{Dial: wire.UnixDialer(socketPath)})
	if err != nil {
		t.Fatalf("NewClient to process fixture: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startSourceDriverClient(t *testing.T, driver sourcedriver.Driver) *Client {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-source-driver-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	server := &wire.Server{Build: sourcedriverproto.Build, HandshakeTimeout: time.Second}
	if _, err := Register(server, driver); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		admit := func() (func(), error) { return func() {}, nil }
		done <- server.Serve(ctx, listener, func() error { return nil }, admit, admit)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	client, err := NewClient(context.Background(), wire.ClientConfig{Dial: wire.UnixDialer(path)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func serviceTestTargetSet(t *testing.T, epoch uint64) ([]sourcedriver.TargetDeclaration, sourcedriver.TargetSetRef) {
	t.Helper()
	targets := []sourcedriver.TargetDeclaration{{Tenant: "tenant-one", Generation: 1}}
	ref, err := sourcedriver.NewTargetSetRef(testAuthority, 1, epoch, [32]byte{1}, targets)
	if err != nil {
		t.Fatal(err)
	}
	return targets, ref
}

func serviceTestExpandedTargetSet(t *testing.T, epoch uint64) ([]sourcedriver.TargetDeclaration, sourcedriver.TargetSetRef) {
	t.Helper()
	targets := []sourcedriver.TargetDeclaration{
		{Tenant: "tenant-one", Generation: 1},
		{Tenant: "tenant-two", Generation: 1},
	}
	ref, err := sourcedriver.NewTargetSetRef(testAuthority, 1, epoch, [32]byte{1}, targets)
	if err != nil {
		t.Fatal(err)
	}
	return targets, ref
}

func declareServiceTestTargetSet(t *testing.T, client *Client) sourcedriver.TargetSetRef {
	t.Helper()
	targets, ref := serviceTestTargetSet(t, 1)
	state := declareServiceTargetSetPages(t, client, targets, ref)
	replayed, err := client.InspectTargetSet(t.Context(), testAuthority, ref)
	if err != nil || replayed != state {
		t.Fatalf("InspectTargetSet = %+v, %v, want %+v", replayed, err, state)
	}
	return ref
}

func declareServiceTargetSetPages(
	t *testing.T,
	client *Client,
	targets []sourcedriver.TargetDeclaration,
	ref sourcedriver.TargetSetRef,
) sourcedriver.TargetSetState {
	t.Helper()
	state, err := sourcedriver.NewTargetSetState(testAuthority, ref)
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(targets); offset += sourcedriver.MaxTargetPageItems {
		end := min(offset+sourcedriver.MaxTargetPageItems, len(targets))
		page, err := sourcedriver.NewTargetSetPage(state, targets[offset:end])
		if err != nil {
			t.Fatal(err)
		}
		state, err = client.DeclareTargetSet(t.Context(), testAuthority, page)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !state.Complete || state.Ref != ref {
		t.Fatalf("DeclareTargetSet = %+v", state)
	}
	return state
}

func testObject() sourcedriver.Projection {
	return testSettingsObject(1)
}

func testSettingsObject(generation causal.Generation) sourcedriver.Projection {
	body := []byte("settings-body")
	hash := catalog.ContentHash(sha256.Sum256(body))
	content := sourcedriver.ContentRef{
		Revision: "head-2", Tenant: "tenant-one", Generation: generation,
		Object: "settings", Size: int64(len(body)), Hash: hash,
	}
	return sourcedriver.Projection{
		Tenant: "tenant-one", Generation: generation, ID: "settings", Parent: "root", Name: "settings.json",
		Kind: catalog.KindFile, Mode: 0o600, Visibility: catalog.Visibility{Mount: true, FileProvider: true},
		Size: content.Size, Hash: content.Hash, Content: &content,
	}
}

func testCreatedObject(generation causal.Generation) sourcedriver.Projection {
	body := []byte("next-body")
	hash := catalog.ContentHash(sha256.Sum256(body))
	content := sourcedriver.ContentRef{
		Revision: "head-3", Tenant: "tenant-one", Generation: generation,
		Object: "created", Size: int64(len(body)), Hash: hash,
	}
	return sourcedriver.Projection{
		Tenant: "tenant-one", Generation: generation, ID: "created", Parent: "root", Name: "created.json",
		Kind: catalog.KindFile, Mode: 0o600, Visibility: catalog.Visibility{Mount: true, FileProvider: true},
		Size: content.Size, Hash: content.Hash, Content: &content,
	}
}

func testRootObject(generation causal.Generation) sourcedriver.Projection {
	return sourcedriver.Projection{
		Tenant: "tenant-one", Generation: generation, ID: "root", Parent: "", Name: "root",
		Kind: catalog.KindDirectory, Mode: 0o755,
		Visibility: catalog.Visibility{Mount: true, FileProvider: true},
	}
}

func testMutation(
	targetSet sourcedriver.TargetSetRef,
	expected sourcedriver.RevisionToken,
	body []byte,
) (sourcedriver.MutationRequest, contentstream.Source) {
	hash := catalog.ContentHash(sha256.Sum256(body))
	locator := &catalog.SourceLocator{SourceAuthority: testAuthority, SourceKey: "root", SourceRevision: 2}
	return sourcedriver.MutationRequest{
		TargetSet: targetSet, Tenant: "tenant-one", Generation: 1,
		OperationID: catalog.MutationID{9}, Expected: expected,
		Context: catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationCreate, Name: "created.json", ObjectKind: catalog.KindFile, Mode: 0o600, HasContent: true,
			},
			Parent: locator,
		},
		HasContent: true, ContentSize: int64(len(body)), ContentHash: hash,
	}, newByteSource(body)
}

func testContentlessMutation(targetSet sourcedriver.TargetSetRef) sourcedriver.MutationRequest {
	object := &catalog.SourceLocator{SourceAuthority: testAuthority, SourceKey: "settings", SourceRevision: 3}
	parent := &catalog.SourceLocator{SourceAuthority: testAuthority, SourceKey: "root", SourceRevision: 3}
	return sourcedriver.MutationRequest{
		TargetSet: targetSet, Tenant: "tenant-one", Generation: 1,
		OperationID: catalog.MutationID{10}, Expected: "head-3",
		Context: catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationDelete, Name: "settings.json", ObjectKind: catalog.KindFile, Mode: 0o600,
			},
			Object: object, Parent: parent,
		},
	}
}

func mutationSettlementForTest(
	t *testing.T,
	request sourcedriver.MutationRequest,
	receipt sourcedriver.MutationReceipt,
	kind sourcedriver.MutationSettlementKind,
) sourcedriver.MutationSettlement {
	t.Helper()
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	if requestDigest != receipt.RequestDigest {
		t.Fatal("mutation request and receipt digests differ")
	}
	return sourcedriver.MutationSettlement{
		TargetSet:     request.TargetSet,
		OperationID:   request.OperationID,
		RequestDigest: requestDigest,
		ReceiptDigest: receipt.Digest,
		Kind:          kind,
	}
}

type byteSource struct {
	reader  *bytes.Reader
	mu      sync.Mutex
	settled bool
	err     error
	done    chan struct{}
}

func newByteSource(body []byte) *byteSource {
	return &byteSource{reader: bytes.NewReader(body), done: make(chan struct{})}
}

func (s *byteSource) Read(buffer []byte) (int, error) { return s.reader.Read(buffer) }

func (s *byteSource) Settle(cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.settled {
		s.settled = true
		s.err = cause
		close(s.done)
	}
	return s.err
}

func (s *byteSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ sourcedriver.Driver = (*testDriver)(nil)
var _ contentstream.Source = (*byteSource)(nil)
