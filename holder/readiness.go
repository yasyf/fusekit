package holder

import (
	"errors"
	"time"
)

const (
	readinessObservationMargin      = 5 * time.Second
	standardCatalogOperationTimeout = 30 * time.Second
	standardFrameWriteTimeout       = 10 * time.Second
	preparationNoProgressMargin     = 5 * time.Second
)

// ReadinessContract is the one signed-runtime and service-observer deadline budget.
type ReadinessContract struct {
	startup     time.Duration
	settlement  time.Duration
	observation time.Duration
}

// NewReadinessContract constructs one exact readiness deadline budget.
func NewReadinessContract(startup, settlement, observation time.Duration) (ReadinessContract, error) {
	contract := ReadinessContract{startup: startup, settlement: settlement, observation: observation}
	if err := contract.validate(); err != nil {
		return ReadinessContract{}, err
	}
	return contract, nil
}

// StandardReadinessContract returns the production readiness deadline budget.
func StandardReadinessContract() ReadinessContract {
	contract, err := NewReadinessContract(30*time.Second, 30*time.Second, 65*time.Second)
	if err != nil {
		panic(err)
	}
	return contract
}

// StartupTimeout bounds signed-runtime readiness publication.
func (c ReadinessContract) StartupTimeout() time.Duration { return c.startup }

// SettlementTimeout bounds exact cleanup after startup failure.
func (c ReadinessContract) SettlementTimeout() time.Duration { return c.settlement }

// ObservationTimeout bounds the outer service readiness observation.
func (c ReadinessContract) ObservationTimeout() time.Duration { return c.observation }

// PreparationNoProgressTimeout bounds silence between semantic tenant
// preparation transitions. Heartbeats do not extend it.
func (c ReadinessContract) PreparationNoProgressTimeout() time.Duration {
	return standardCatalogOperationTimeout + c.settlement + standardFrameWriteTimeout + preparationNoProgressMargin
}

func (c ReadinessContract) validate() error {
	switch {
	case c.startup <= 0:
		return errors.New("FuseKit runtime: readiness startup timeout must be positive")
	case c.settlement <= 0:
		return errors.New("FuseKit runtime: readiness settlement timeout must be positive")
	case c.observation < c.startup+c.settlement+readinessObservationMargin:
		return errors.New("FuseKit runtime: readiness observation timeout can preempt startup settlement")
	default:
		return nil
	}
}
