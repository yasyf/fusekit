package tenant

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
)

func TestWorkerInputRejectsOverOneMiB(t *testing.T) {
	reader, done, err := workerInput(bytes.Repeat([]byte{'x'}, maxWorkerInputBytes+1))
	if reader != nil || done != nil || !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("workerInput(over limit) = %v, %v, %v; want integrity rejection", reader, done, err)
	}
}

func TestWorkerInputFeederSettlesWhenKilledChildClosesPipe(t *testing.T) {
	before := openDescriptorCount(t)
	reader, done, err := workerInput(bytes.Repeat([]byte{'x'}, maxWorkerInputBytes))
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker input feeder retained its descriptor after child pipe closed")
	}
	if after := openDescriptorCount(t); after != before {
		t.Fatalf("open descriptors after feeder settlement = %d, want %d", after, before)
	}
}

func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		entries, err = os.ReadDir("/proc/self/fd")
	}
	if err != nil {
		t.Skipf("descriptor directory unavailable: %v", err)
	}
	return len(entries)
}
