package holder

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/transportproto"
)

func TestBusinessHandlerControllerIsBoundToCallbackPublication(t *testing.T) {
	dir := shortTempDir(t)
	controllers := make(chan *LocalTenantController, 1)
	config := testConfig(dir, "business-v1", newTestNative(nil))
	config.BusinessHandlers = []BusinessHandlerSpec{{
		Op: "product.test", Concurrent: true,
		Handler: func(ctx context.Context, _ wire.Request, controller *LocalTenantController) (any, error) {
			readiness, err := controller.Readiness(ctx)
			if err != nil {
				return nil, err
			}
			if readiness.RuntimeBuild != "business-v1" || readiness.ActivationGeneration == "" ||
				readiness.ProcessGeneration == (proc.OwnerGeneration{}) {
				return nil, errors.New("business handler received incomplete readiness")
			}
			controllers <- controller
			return "ok", nil
		},
	}}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	_ = runRuntime(t, runtime)
	if _, err := runtime.LocalTenantController().Readiness(t.Context()); err != nil {
		t.Fatal(err)
	}
	client, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Call(t.Context(), "product.test", "", nil)
	if err != nil || result.Outcome != wire.Delivered {
		t.Fatalf("business call = %+v, %v", result, err)
	}
	escaped := <-controllers
	if _, err := escaped.State(t.Context(), catalog.TenantID("absent")); !errors.Is(err, ErrLocalTenantControllerUnavailable) {
		t.Fatalf("escaped controller State = %v, want unavailable", err)
	}
}
