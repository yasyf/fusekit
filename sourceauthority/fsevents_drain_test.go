package sourceauthority

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestObserverDrainReplaysRetainedBatchAndSettlesOnCursorOrNack(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		settle  func(*testing.T, spawnedCaller, uint64)
		wantErr string
	}{
		{
			name: "cursor advance acknowledges durable delivery",
			settle: func(t *testing.T, caller spawnedCaller, sequence uint64) {
				t.Helper()
				response := drainTestObserver(t, caller, sequence, 100*time.Millisecond)
				if response.Pending {
					t.Fatalf("acknowledging drain replayed sequence %d", response.Sequence)
				}
			},
		},
		{
			name: "negative acknowledgement surfaces the delivery error",
			settle: func(t *testing.T, caller spawnedCaller, sequence uint64) {
				t.Helper()
				nackTestObserver(t, caller, sequence, "sink refused the batch")
			},
			wantErr: "sink refused the batch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			native, caller := openTestObserverChild(t)

			delivery := make(chan error, 1)
			go func() { delivery <- native.emit(context.Background()) }()

			first := drainTestObserver(t, caller, 0, 2*time.Second)
			if !first.Pending || first.Sequence != 1 {
				t.Fatalf("first drain = pending %t sequence %d, want pending 1", first.Pending, first.Sequence)
			}
			if first.Batch.Cursor != 8 || first.Batch.Stream != "stream" {
				t.Fatalf("first drain batch = %+v", first.Batch)
			}

			replay := drainTestObserver(t, caller, 0, 2*time.Second)
			if !reflect.DeepEqual(replay, first) {
				t.Fatalf("replayed drain = %+v, want the retained %+v", replay, first)
			}

			select {
			case err := <-delivery:
				t.Fatalf("engine unblocked before the cursor advanced: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			test.settle(t, caller, first.Sequence)

			select {
			case err := <-delivery:
				switch {
				case test.wantErr == "" && err != nil:
					t.Fatalf("engine delivery = %v, want nil", err)
				case test.wantErr != "" && (err == nil || err.Error() != test.wantErr):
					t.Fatalf("engine delivery = %v, want %q", err, test.wantErr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("engine stayed blocked after its batch settled")
			}
		})
	}
}

func openTestObserverChild(t *testing.T) (*testObserverStream, spawnedCaller) {
	t.Helper()
	native := newTestObserverStream(false, false)
	child, cancel := newFSEventsObserverChild(context.Background(), &testObserverBackend{stream: native})
	session, err := startTestSourceSession(observerSpawnedContract(), child.handle)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := session.settle(); err != nil {
			t.Error(err)
		}
	})
	deadlines, err := observerSpawnedDeadlines(StandardOperationDeadlines())
	if err != nil {
		t.Fatal(err)
	}
	caller := spawnedCaller{client: session, deadlines: deadlines}

	roots := testProxyRoots()
	manifest, err := planObserverOpenPages(roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := sendObserverOpenPages(ctx, caller, roots, nil, manifest); err != nil {
		t.Fatal(err)
	}
	payload, err := marshalObserverControl(observerOpenRequest{
		Protocol: fseventsObserverProtocol, Config: manifest,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := caller.call(ctx, fseventsOpOpen, payload)
	if err != nil {
		t.Fatal(err)
	}
	var opened observerCheckpointResponse
	if err := decodeObserver(body, &opened); err != nil {
		t.Fatal(err)
	}
	return native, caller
}

func drainTestObserver(
	t *testing.T,
	caller spawnedCaller,
	cursor uint64,
	wait time.Duration,
) observerDrainResponse {
	t.Helper()
	payload, err := marshalObserverControl(observerDrainRequest{
		Protocol: fseventsObserverProtocol, Cursor: cursor, Wait: wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := caller.call(context.Background(), fseventsOpDrain, payload)
	if err != nil {
		t.Fatal(err)
	}
	var response observerDrainResponse
	if err := decodeObserver(body, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func nackTestObserver(t *testing.T, caller spawnedCaller, sequence uint64, message string) {
	t.Helper()
	payload, err := marshalObserverControl(observerNackRequest{
		Protocol: fseventsObserverProtocol, Sequence: sequence, Error: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := caller.call(context.Background(), fseventsOpNack, payload)
	if err != nil {
		t.Fatal(err)
	}
	var response observerRequest
	if err := decodeObserver(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Protocol != fseventsObserverProtocol {
		t.Fatalf("negative acknowledgement protocol = %d", response.Protocol)
	}
}
