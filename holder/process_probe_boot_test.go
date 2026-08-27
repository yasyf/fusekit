package holder

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func TestClassifyKeepsALiveProcessAliveUnderASlewedBootSession(t *testing.T) {
	record, err := captureCurrentProcessRecord(
		recoveryid.SourceOwner, holderOwnerGeneration("retired"),
	)
	if err != nil {
		t.Fatal(err)
	}
	record.Boot = "1787215836.000001"

	outcome, err := classifyRecordedProcess(record)
	if !errors.Is(err, errProcessAlive) {
		t.Fatalf("classified a live process as outcome %d (%v), want errProcessAlive", outcome, err)
	}
}

func TestClassifyReportsCrossBootForAnIdentityFromAnotherBoot(t *testing.T) {
	record, err := captureCurrentProcessRecord(
		recoveryid.SourceOwner, holderOwnerGeneration("retired"),
	)
	if err != nil {
		t.Fatal(err)
	}
	record.StartTime = "1.000001"
	record.Boot = "1787215836.000001"

	outcome, err := classifyRecordedProcess(record)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != catalog.ReapCrossBoot {
		t.Fatalf("outcome = %d, want ReapCrossBoot", outcome)
	}
}

func TestClassifyReportsIdentityReuseWithinTheSameBootSession(t *testing.T) {
	record, err := captureCurrentProcessRecord(
		recoveryid.SourceOwner, holderOwnerGeneration("retired"),
	)
	if err != nil {
		t.Fatal(err)
	}
	record.StartTime = "1.000001"

	outcome, err := classifyRecordedProcess(record)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != catalog.ReapIdentityReused {
		t.Fatalf("outcome = %d, want ReapIdentityReused", outcome)
	}
}
