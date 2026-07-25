package tenant

import (
	"bytes"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestWorkerInputRejectsOverOneMiB(t *testing.T) {
	input, err := workerInput(bytes.Repeat([]byte{'x'}, maxWorkerInputBytes+1))
	if input != nil || !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("workerInput(over limit) = %v, %v; want integrity rejection", input, err)
	}
}

func TestWorkerInputCopiesExactBoundedBytes(t *testing.T) {
	source := bytes.Repeat([]byte{'x'}, maxWorkerInputBytes)
	input, err := workerInput(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(input) != len(source) || !bytes.Equal(input, source) {
		t.Fatalf("workerInput length = %d, want %d exact bytes", len(input), len(source))
	}
	source[0] = 'y'
	if input[0] != 'x' {
		t.Fatal("workerInput retained caller-owned storage")
	}
}
