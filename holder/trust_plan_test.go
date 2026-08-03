package holder

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit"
)

func testNativeOnlyPlan(t *testing.T, home string) RuntimePlan {
	t.Helper()
	application := testSignedApplication(testHelperAppPath(home), "com.example.notes", "ProductHelper")
	application.Broker = SignedExecutable{}
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "runtime"),
		Native: testNativeRuntimeSpec(filepath.Join(home, "presentation")), BuildID: testBuildID,
		Readiness: StandardReadinessContract(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testFileProviderOnlyPlan(t *testing.T, home string) RuntimePlan {
	t.Helper()
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testBothPresentationsPlan(t *testing.T, home string) RuntimePlan {
	t.Helper()
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestRuntimeTrustDerivesBusinessFromPlanPresentations(t *testing.T) {
	const home = "/Users/example"
	extension := testProcessRequirement("file-provider-extension")
	tests := []struct {
		name      string
		plan      RuntimePlan
		extension daemonkit.Requirement
		want      func(RuntimePlan) daemonkit.Requirements
	}{
		{
			"native only", testNativeOnlyPlan(t, home),
			daemonkit.Requirement{},
			func(plan RuntimePlan) daemonkit.Requirements {
				return daemonkit.Requirements{plan.RuntimeRequirement()}
			},
		},
		{
			"file provider only", testFileProviderOnlyPlan(t, home), extension,
			func(plan RuntimePlan) daemonkit.Requirements {
				broker, _ := plan.Broker()
				return daemonkit.Requirements{extension, broker.Requirement}
			},
		},
		{
			"native and file provider", testBothPresentationsPlan(t, home), extension,
			func(plan RuntimePlan) daemonkit.Requirements {
				broker, _ := plan.Broker()
				return daemonkit.Requirements{plan.RuntimeRequirement(), extension, broker.Requirement}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trust, err := runtimeTrust(Config{
				Plan: tt.plan,
				Trust: RuntimeTrust{
					Controller: testProcessRequirement("controller"), FileProviderExtension: tt.extension,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if want := tt.want(tt.plan); !reflect.DeepEqual(trust.Business, want) {
				t.Fatalf("business = %+v, want %+v", trust.Business, want)
			}
			if !reflect.DeepEqual(trust.Serving, daemonkit.ServingSigned(tt.plan.RuntimeRequirement())) {
				t.Fatalf("serving = %+v, want the plan's runtime requirement", trust.Serving)
			}
		})
	}
}

func TestRuntimeTrustRefusesExtensionRequirementWithoutBrokerPresentation(t *testing.T) {
	const home = "/Users/example"
	if _, err := runtimeTrust(Config{
		Plan: testNativeOnlyPlan(t, home),
		Trust: RuntimeTrust{
			Controller:            testProcessRequirement("controller"),
			FileProviderExtension: testProcessRequirement("file-provider-extension"),
		},
	}); err == nil {
		t.Fatal("native-only plan accepted a File Provider extension requirement")
	}
}
