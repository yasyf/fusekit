package holder

import (
	"errors"
	"fmt"
	"testing"

	"golang.org/x/sys/unix"

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

func TestBootSessionIsNotDerivedFromTheSlewingBootClock(t *testing.T) {
	session, err := bootSession()
	if err != nil {
		t.Fatal(err)
	}
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		t.Fatal(err)
	}
	for _, rendering := range []string{
		fmt.Sprintf("%d.%06d", boot.Sec, boot.Usec),
		fmt.Sprintf("%d", boot.Sec),
	} {
		if session == rendering {
			t.Fatalf(
				"boot session %q is a kern.boottime rendering; darwin slews it across one boot",
				session,
			)
		}
	}
}

func TestBootSessionIsStableAcrossReads(t *testing.T) {
	first, err := bootSession()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 36 {
		t.Fatalf("boot session %q is not a boot session UUID", first)
	}
	for range 8 {
		again, againErr := bootSession()
		if againErr != nil {
			t.Fatal(againErr)
		}
		if again != first {
			t.Fatalf("boot session moved from %q to %q within one boot", first, again)
		}
	}
}
