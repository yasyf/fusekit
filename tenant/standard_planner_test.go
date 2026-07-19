package tenant

import (
	"context"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

func TestStandardPlannerRejectsCommittedMutationWithoutSourcePlanner(t *testing.T) {
	err := (StandardPlanner{}).SourceMutationCommitted(context.Background(), SourceMutationCommit{
		OperationID: catalog.MutationID{1},
		SourceID:    "source",
	})
	if err == nil {
		t.Fatal("committed mutation without source planner was accepted")
	}
}
