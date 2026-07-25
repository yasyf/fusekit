package holder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
)

func TestIdempotentProvisionAndPrepareReplayUnknownDeliveryExactlyOnce(t *testing.T) {
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: "/Volumes/FuseKit/acct-18"},
		BackingRoot: "/tmp/backing", ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 1,
	}
	tests := []struct {
		name   string
		invoke func(context.Context, *ServiceClient) error
	}{
		{
			name: "provision",
			invoke: func(ctx context.Context, client *ServiceClient) error {
				response, err := client.Mount().ProvisionTenant(ctx, "acct-18", definition)
				if err != nil {
					return err
				}
				if response.Code != mountproto.ErrorCodeOk || response.Generation != 1 {
					return fmt.Errorf("ProvisionTenant response = %#v", response)
				}
				return nil
			},
		},
		{
			name: "prepare",
			invoke: func(ctx context.Context, client *ServiceClient) error {
				response, err := client.Catalog().PrepareTenant(ctx, "acct-18", catalogproto.PrepareTenantRequest{
					Protocol: catalogproto.Version, Generation: 1,
					Presentation: catalogproto.PresentationKindMount, ActivationGeneration: "activation-1",
				})
				if err != nil {
					return err
				}
				if response.Code != catalogproto.ErrorCodeOk || response.Proof == nil {
					return fmt.Errorf("PrepareTenant response = %#v", response)
				}
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var effects atomic.Int64
			var firstPayload []byte
			var mu sync.Mutex
			handle := func(request wire.Frame) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				if firstPayload == nil {
					firstPayload = bytes.Clone(request.Payload)
					effects.Add(1)
				} else if !bytes.Equal(firstPayload, request.Payload) {
					return nil, errors.New("replay changed the logical request payload")
				}
				switch request.Op {
				case wire.Op(mountproto.OperationTenantProvision):
					return mountproto.Encode(mountproto.ProvisionTenantResponse{
						Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
						TenantID: "acct-18", Generation: 1,
					})
				case wire.Op(catalogproto.OperationTenantPrepare):
					proof := serviceClientPreparationProof("acct-18", "/Volumes/FuseKit/acct-18")
					return catalogproto.Encode(catalogproto.PrepareTenantResponse{
						Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, Proof: &proof,
					})
				default:
					return nil, fmt.Errorf("unexpected operation %q", request.Op)
				}
			}
			first := serviceReplayPeer(t, handle, false)
			second := serviceReplayPeer(t, handle, true)
			peers := []net.Conn{first, second}
			var dials atomic.Int64
			client, err := NewServiceClient(wire.ClientConfig{
				WireBuild: transportproto.WireBuild,
				Dial: func(context.Context) (net.Conn, error) {
					index := int(dials.Add(1)) - 1
					if index >= len(peers) {
						return nil, errors.New("unexpected third generation")
					}
					return peers[index], nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := test.invoke(ctx, client); err != nil {
				t.Fatal(err)
			}
			if effects.Load() != 1 || dials.Load() != 2 {
				t.Fatalf("replay = effects %d, generations %d; want one effect across two generations", effects.Load(), dials.Load())
			}
		})
	}
}

func serviceReplayPeer(t *testing.T, handle func(wire.Frame) ([]byte, error), respond bool) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close() })
	go func() {
		defer server.Close()
		codec := wire.NewCodec(server)
		if _, err := codec.ReadFrame(); err != nil {
			return
		}
		hello, err := json.Marshal(wire.WireIdentity{
			Protocol: wire.ProtocolVersion, WireBuild: transportproto.WireBuild,
			Session: make([]byte, 16),
		})
		if err != nil || codec.WriteFrame(wire.Frame{Kind: wire.FrameHelloAck, Flags: wire.FlagEnd, Payload: hello}) != nil {
			return
		}
		if _, err := codec.ReadFrame(); err != nil { // event window
			return
		}
		readiness, err := codec.ReadFrame()
		if err != nil || readiness.Op != wire.Op("daemon.control.readiness") {
			return
		}
		readinessPayload, err := json.Marshal(struct {
			Protocol  uint16 `json:"protocol"`
			WireBuild string `json:"wire_build"`
			Ready     bool   `json:"ready"`
			Draining  bool   `json:"draining"`
		}{Protocol: wire.ProtocolVersion, WireBuild: transportproto.WireBuild, Ready: true})
		if err != nil || writeServicePeerResponse(codec, readiness.ID, readinessPayload) != nil {
			return
		}
		request, err := codec.ReadFrame()
		if err != nil {
			return
		}
		payload, err := handle(request)
		if err != nil || !respond {
			return
		}
		_ = writeServicePeerResponse(codec, request.ID, payload)
	}()
	return client
}

func writeServicePeerResponse(codec *wire.Codec, id uint64, payload []byte) error {
	encoded, err := json.Marshal(wire.Response{Payload: payload})
	if err != nil {
		return err
	}
	return codec.WriteFrame(wire.Frame{Kind: wire.FrameResponse, Flags: wire.FlagEnd, ID: id, Payload: encoded})
}

func serviceClientPreparationProof(tenant, publicPath string) catalogproto.TenantPreparationProof {
	const revision = 1
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenant), Generation: 1,
			Requested: revision, Desired: revision, Observed: revision, Verified: revision, Applied: revision,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindMount,
			Mount: &catalogproto.MountPresentationProof{
				TenantID: catalogproto.TenantID(tenant), Generation: 1,
				PublicPath: publicPath, ActivationGeneration: "activation-1",
			},
		},
		SourceAuthority: "source-main", SourceRevision: revision, CatalogRevision: revision,
		ChangeID: "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
	}
}

type serviceTestServer struct {
	listener *countingListener
	cancel   context.CancelFunc
	done     <-chan error
	once     sync.Once
	err      error
}

func startServiceTestServer(t *testing.T, socket string, register func(*wire.Server)) *serviceTestServer {
	t.Helper()
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingListener{Listener: listener}
	server := &wire.Server{WireBuild: transportproto.WireBuild}
	register(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		admit := func() (func(), error) { return func() {}, nil }
		done <- server.Serve(ctx, counting, func() error { return nil }, admit, admit)
	}()
	running := &serviceTestServer{listener: counting, cancel: cancel, done: done}
	t.Cleanup(func() { running.close(t) })
	return running
}

func (s *serviceTestServer) close(t *testing.T) {
	t.Helper()
	s.once.Do(func() {
		s.cancel()
		_ = s.listener.Close()
		select {
		case err := <-s.done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
				s.err = err
			}
		case <-time.After(5 * time.Second):
			s.err = errors.New("server did not stop")
		}
	})
	if s.err != nil {
		t.Errorf("Serve: %v", s.err)
	}
}

type countingListener struct {
	net.Listener
	accepts atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return connection, err
}

func TestServiceClientRejectsAnotherTransportSuite(t *testing.T) {
	if _, err := NewServiceClient(wire.ClientConfig{
		Dial:      func(context.Context) (net.Conn, error) { return nil, errors.New("unused") },
		WireBuild: "another-suite",
	}); err == nil {
		t.Fatal("service client accepted another transport suite")
	}
}

func TestObservationClientNeverCrossesRuntimeGeneration(t *testing.T) {
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "holder.sock")
	registerHealth := func(generation string) func(*wire.Server) {
		return func(server *wire.Server) {
			server.Register(wire.HandlerSpec{Op: wire.Op(mountproto.OperationRuntimeHealth), Concurrent: true, Handler: func(context.Context, wire.Request) (any, error) {
				return mountproto.RuntimeHealthResponse{
					Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
					RuntimeBuild: "runtime-v1", RuntimeProtocol: mountproto.RuntimeProtocolVersion,
					RuntimePID: 42, ProcessGeneration: generation, ActivationGeneration: "activation-1",
					State: mountproto.RuntimeStateHealthy, Ready: true,
					ReadinessPhase: mountproto.ReadinessPhaseReady, ReadinessStep: mountproto.ReadinessStepPublished,
					NativePhase: mountproto.NativePhaseDisabled, BrokerPhase: mountproto.BrokerPhaseLive,
				}, nil
			}})
		}
	}

	first := startServiceTestServer(t, socket, registerHealth("first"))
	observer, err := mountservice.NewObservationClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstHealth, err := observer.RuntimeHealth(t.Context())
	if err != nil || firstHealth.ProcessGeneration != "first" {
		t.Fatalf("first RuntimeHealth = %#v, %v", firstHealth, err)
	}
	first.close(t)
	second := startServiceTestServer(t, socket, registerHealth("second"))
	defer second.close(t)

	callContext, cancelCall := context.WithTimeout(t.Context(), time.Second)
	defer cancelCall()
	if response, err := observer.RuntimeHealth(callContext); err == nil {
		t.Fatalf("old observation crossed generation: %#v", response)
	}
	_ = observer.Close()

	replacement, err := mountservice.NewObservationClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	secondHealth, err := replacement.RuntimeHealth(t.Context())
	if err != nil || secondHealth.ProcessGeneration != "second" {
		t.Fatalf("second RuntimeHealth = %#v, %v", secondHealth, err)
	}
}
