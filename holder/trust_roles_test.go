package holder

import (
	"os"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/fusekit/trustroles"
)

func TestFuseKitProcessTrustRolesKeepOnlyBrokerInHandoff(t *testing.T) {
	wantRoles := map[trust.PeerRole]string{
		trustroles.NativeChild:           "fusekit.native-child.v1",
		trustroles.Broker:                "fusekit.broker.v1",
		trustroles.BrokerLifecycle:       "fusekit.broker-lifecycle.v1",
		trustroles.FileProviderExtension: "fusekit.file-provider-extension.v1",
		trustroles.StopController:        "fusekit.stop-controller.v1",
		trustroles.ReceiptController:     "fusekit.receipt-controller.v1",
		trustroles.ReadinessController:   "fusekit.readiness-controller.v1",
	}
	for role, want := range wantRoles {
		if string(role) != want {
			t.Fatalf("role = %q, want %q", role, want)
		}
	}
	native := testProcessRequirement("native")
	broker := testProcessRequirement("broker")
	extension := testProcessRequirement("extension")
	stop := testProcessRequirement("stop-controller")
	receipt := testProcessRequirement("receipt-controller")
	readiness := testProcessRequirement("readiness-controller")
	config, err := applyFuseKitProcessTrustRoles(trust.TrustPolicyConfig{
		Roles: map[trust.PeerRole]trust.Requirement{"consumer.ordinary.v1": testProcessRequirement("ordinary")},
	}, fuseKitProcessRequirements{
		nativeChild: &native, broker: &broker, brokerLifecycle: &broker,
		fileProviderExtension: &extension,
		stopController:        &stop, receiptController: &receipt, readinessController: &readiness,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config.HandoffRoles, []trust.PeerRole{trustroles.Broker}) {
		t.Fatalf("handoff roles = %q, want broker only", config.HandoffRoles)
	}
	for role, want := range map[trust.PeerRole]trust.Requirement{
		trustroles.NativeChild: native, trustroles.Broker: broker, trustroles.BrokerLifecycle: broker,
		trustroles.FileProviderExtension: extension,
		trustroles.StopController:        stop, trustroles.ReceiptController: receipt, trustroles.ReadinessController: readiness,
	} {
		if got := config.Roles[role]; !reflect.DeepEqual(got, want) {
			t.Fatalf("role %q requirement = %+v, want %+v", role, got, want)
		}
	}
	if !reflect.DeepEqual(config.StopRoles, []trust.PeerRole{trustroles.StopController}) ||
		!reflect.DeepEqual(config.ReceiptRoles, []trust.PeerRole{
			trustroles.ReceiptController, trustroles.BrokerLifecycle,
		}) ||
		!reflect.DeepEqual(config.ReadinessRoles, []trust.PeerRole{
			trustroles.ReadinessController, trustroles.BrokerLifecycle,
		}) {
		t.Fatalf(
			"controller roles = stop %q receipt %q readiness %q",
			config.StopRoles, config.ReceiptRoles, config.ReadinessRoles,
		)
	}
	config.ExpectedUID = os.Geteuid()
	if _, err := trust.NewTrustPolicy(config); err != nil {
		t.Fatalf("compile FuseKit process trust policy: %v", err)
	}
}

func TestBrokerLifecycleAndHandoffAuthoritiesRemainDisjoint(t *testing.T) {
	requirement := testProcessRequirement("broker")
	stop := testProcessRequirement("stop")
	config, err := applyFuseKitProcessTrustRoles(
		trust.TrustPolicyConfig{},
		fuseKitProcessRequirements{
			broker: &requirement, brokerLifecycle: &requirement, stopController: &stop,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(config.HandoffRoles, []trust.PeerRole{trustroles.Broker}) ||
		!reflect.DeepEqual(config.ReceiptRoles, []trust.PeerRole{trustroles.BrokerLifecycle}) ||
		!reflect.DeepEqual(config.ReadinessRoles, []trust.PeerRole{trustroles.BrokerLifecycle}) {
		t.Fatalf(
			"broker authorities = handoff %q receipt %q readiness %q",
			config.HandoffRoles, config.ReceiptRoles, config.ReadinessRoles,
		)
	}
	config.ExpectedUID = os.Geteuid()
	if _, err := trust.NewTrustPolicy(config); err != nil {
		t.Fatalf("compile disjoint broker policy: %v", err)
	}
}

func TestFuseKitProcessTrustRolesRejectForeignHandoff(t *testing.T) {
	_, err := applyFuseKitProcessTrustRoles(trust.TrustPolicyConfig{
		HandoffRoles: []trust.PeerRole{"consumer.handoff.v1"},
	}, fuseKitProcessRequirements{})
	if err == nil {
		t.Fatal("foreign handoff role was accepted")
	}
}

func TestFuseKitControllerRolesMayShareOneSignedRequirement(t *testing.T) {
	requirement := testProcessRequirement("controller")
	config, err := applyFuseKitProcessTrustRoles(trust.TrustPolicyConfig{}, fuseKitProcessRequirements{
		stopController: &requirement, receiptController: &requirement, readinessController: &requirement,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []trust.PeerRole{
		trustroles.StopController, trustroles.ReceiptController, trustroles.ReadinessController,
	} {
		if got := config.Roles[role]; !reflect.DeepEqual(got, requirement) {
			t.Fatalf("controller role %q requirement = %+v, want shared %+v", role, got, requirement)
		}
	}
}

func testProcessRequirement(suffix string) trust.Requirement {
	return trust.Requirement{TeamID: "ABCDE12345", SigningIdentifier: "com.example." + suffix}
}
