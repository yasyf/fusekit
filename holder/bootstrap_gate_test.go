package holder

import (
	"testing"

	"github.com/yasyf/fusekit/mountproto"
)

func TestBootstrapGateReportsExactPhaseAndLastStep(t *testing.T) {
	gate := &bootstrapGate{}
	if phase, step := gate.readiness(); phase != mountproto.ReadinessPhaseStarting || step != mountproto.ReadinessStepListener {
		t.Fatalf("initial readiness = %q/%q", phase, step)
	}
	gate.advance(bootstrapBroker)
	if phase, step := gate.readiness(); phase != mountproto.ReadinessPhaseStarting || step != mountproto.ReadinessStepBroker {
		t.Fatalf("broker readiness = %q/%q", phase, step)
	}
	gate.fail()
	if phase, step := gate.readiness(); phase != mountproto.ReadinessPhaseFailed || step != mountproto.ReadinessStepBroker {
		t.Fatalf("failed readiness = %q/%q", phase, step)
	}

	publishing := &bootstrapGate{}
	publishing.advance(bootstrapReceipts)
	publishing.publish()
	if phase, step := publishing.readiness(); phase != mountproto.ReadinessPhaseStarting || step != mountproto.ReadinessStepReceipts {
		t.Fatalf("publishing readiness = %q/%q", phase, step)
	}
	publishing.fail()
	if phase, step := publishing.readiness(); phase != mountproto.ReadinessPhaseFailed || step != mountproto.ReadinessStepReceipts {
		t.Fatalf("failed publish readiness = %q/%q", phase, step)
	}

	ready := &bootstrapGate{}
	ready.publish()
	ready.open()
	if phase, step := ready.readiness(); phase != mountproto.ReadinessPhaseReady || step != mountproto.ReadinessStepPublished {
		t.Fatalf("published readiness = %q/%q", phase, step)
	}
}
