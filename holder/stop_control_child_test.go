package holder

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/transportproto"
)

func TestRunStopControlChildRecognizesOnlyExactPrivateMode(t *testing.T) {
	for _, arguments := range [][]string{
		nil,
		{},
		{"consumer-mode"},
		{stopControlChildMode + "-unknown"},
		{"consumer-mode", stopControlChildMode},
	} {
		recognized, err := RunStopControlChild(t.Context(), arguments, StopControlChildConfig{})
		if err != nil || recognized {
			t.Fatalf("RunStopControlChild(%q) = %t, %v", arguments, recognized, err)
		}
	}
	for name, arguments := range map[string][]string{
		"extra argument": {stopControlChildMode, "extra"},
		"missing socket": StopControlChildArguments(),
	} {
		t.Run(name, func(t *testing.T) {
			recognized, err := RunStopControlChild(t.Context(), arguments, StopControlChildConfig{})
			if !recognized || err == nil {
				t.Fatalf("RunStopControlChild(%q) = %t, %v", arguments, recognized, err)
			}
		})
	}
	arguments := StopControlChildArguments()
	arguments[0] = "mutated"
	if got := StopControlChildArguments(); len(got) != 1 || got[0] != stopControlChildMode {
		t.Fatalf("StopControlChildArguments() = %q", got)
	}
}

func TestRunStopControlChildRejectsInexactSocket(t *testing.T) {
	for name, socket := range map[string]string{
		"relative":  "runtime.sock",
		"unclean":   filepath.Join(t.TempDir(), "nested") + "/../runtime.sock",
		"nul":       filepath.Join(t.TempDir(), "runtime\x00.sock"),
		"oversized": "/" + strings.Repeat("x", maxUnixSocketPath),
	} {
		t.Run(name, func(t *testing.T) {
			recognized, err := RunStopControlChild(
				t.Context(), StopControlChildArguments(), StopControlChildConfig{Socket: socket},
			)
			if !recognized || err == nil {
				t.Fatalf("RunStopControlChild(socket=%q) = %t, %v", socket, recognized, err)
			}
		})
	}
}

func TestRunStopControlChildDelegatesExactFuseConfiguration(t *testing.T) {
	socket := filepath.Join(shortTempDir(t), "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	previous := runDaemonStopControlChild
	t.Cleanup(func() { runDaemonStopControlChild = previous })
	delegated := false
	runDaemonStopControlChild = func(ctx context.Context, config service.StopControlClientConfig) (wire.StopResult, error) {
		delegated = true
		if config.WireBuild != transportproto.WireBuild ||
			config.RuntimeProtocol != int(mountproto.RuntimeProtocolVersion) {
			return wire.StopResult{}, errors.New("inexact delegated stop-control configuration")
		}
		connection, err := config.Dial(ctx)
		if err != nil {
			return wire.StopResult{}, err
		}
		return wire.StopResult{Stopped: true}, connection.Close()
	}
	accepted := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			acceptErr = connection.Close()
		}
		accepted <- acceptErr
	}()
	recognized, err := RunStopControlChild(
		t.Context(), StopControlChildArguments(), StopControlChildConfig{Socket: socket},
	)
	if err != nil || !recognized || !delegated {
		t.Fatalf("RunStopControlChild() = %t, delegated=%t, %v", recognized, delegated, err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}
