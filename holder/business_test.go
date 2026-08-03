package holder

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
)

func TestBusinessHandlerControllerIsBoundToCallbackPublication(t *testing.T) {
	dir := shortTempDir(t)
	controllers := make(chan *LocalTenantController, 1)
	config := testConfig(dir, "business-v1", newTestNative(nil))
	config.BusinessHandlers = []BusinessHandlerSpec{{
		Op: "product.test", Concurrent: true,
		Handler: func(ctx context.Context, _ daemonkit.Request, controller *LocalTenantController) (any, error) {
			readiness, err := controller.Readiness(ctx)
			if err != nil {
				return nil, err
			}
			if readiness.RuntimeBuild != "business-v1" || readiness.ActivationGeneration == "" ||
				readiness.ProcessGeneration == (catalog.ProcessGeneration{}) {
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
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if _, err := runtime.LocalTenantController().Readiness(t.Context()); err != nil {
		t.Fatal(err)
	}
	reply, err := holderTestProduct(t, runtime).Handle(t.Context(), daemonkit.Request{
		Op: "product.test", Caller: daemonkit.Caller{UID: uint32(os.Geteuid()), PID: os.Getpid()},
	})
	if err != nil || string(reply.Body) != `"ok"` {
		t.Fatalf("business call = %q, %v", reply.Body, err)
	}
	escaped := <-controllers
	if _, err := escaped.State(t.Context(), catalog.TenantID("absent")); !errors.Is(err, ErrLocalTenantControllerUnavailable) {
		t.Fatalf("escaped controller State = %v, want unavailable", err)
	}
}
