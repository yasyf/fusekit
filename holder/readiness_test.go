package holder

import (
	"strings"
	"testing"
	"time"
)

func TestStandardReadinessContractCannotBePreemptedByObserver(t *testing.T) {
	contract := StandardReadinessContract()
	if err := contract.validate(); err != nil {
		t.Fatal(err)
	}
	minimum := contract.StartupTimeout() + contract.SettlementTimeout() + readinessObservationMargin
	if contract.ObservationTimeout() < minimum {
		t.Fatalf("observation timeout = %s, minimum %s", contract.ObservationTimeout(), minimum)
	}
}

func TestStandardReadinessContractDerivesPreparationNoProgressBudget(t *testing.T) {
	contract := StandardReadinessContract()
	want := standardCatalogOperationTimeout + contract.SettlementTimeout() +
		standardFrameWriteTimeout + preparationNoProgressMargin
	if got := contract.PreparationNoProgressTimeout(); got != want || got != 75*time.Second {
		t.Fatalf("preparation no-progress timeout = %s, want %s", got, want)
	}
	if contract.PreparationNoProgressTimeout() == contract.ObservationTimeout() {
		t.Fatalf("preparation no-progress timeout reused %s observation timeout", contract.ObservationTimeout())
	}
}

func TestRuntimeAndDeploymentPlansExposePreparationNoProgressBudget(t *testing.T) {
	contract := StandardReadinessContract()
	deployment := DeploymentPlan{readiness: contract}
	runtime := RuntimePlan{deployment: deployment}
	for name, got := range map[string]time.Duration{
		"deployment": deployment.Readiness().PreparationNoProgressTimeout(),
		"runtime":    runtime.Readiness().PreparationNoProgressTimeout(),
	} {
		if got != 75*time.Second {
			t.Fatalf("%s preparation no-progress timeout = %s, want 75s", name, got)
		}
	}
}

func TestReadinessContractRejectsPreemptiveOrMissingBudgets(t *testing.T) {
	for _, test := range []struct {
		name        string
		startup     time.Duration
		settlement  time.Duration
		observation time.Duration
		want        string
	}{
		{name: "startup", settlement: time.Second, observation: 10 * time.Second, want: "startup"},
		{name: "settlement", startup: time.Second, observation: 10 * time.Second, want: "settlement"},
		{name: "observer", startup: time.Second, settlement: time.Second, observation: 2 * time.Second, want: "preempt"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewReadinessContract(test.startup, test.settlement, test.observation)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("NewReadinessContract() = %v, want %q", err, test.want)
			}
		})
	}
}
