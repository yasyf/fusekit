package sourceauthority

import (
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestDeltaPlanMergerIndexes4096SegmentsOnce(t *testing.T) {
	const count = 4096
	target := DeltaPlan{}
	merger := newDeltaPlanMerger(&target)
	for index := range count {
		suffix := fmt.Sprintf("%04d", index)
		logical := LogicalID("logical-" + suffix)
		tenant := catalog.TenantID("tenant-" + suffix)
		if err := merger.merge(DeltaPlan{
			AffectedKeys: []causal.LogicalKey{"key-" + causal.LogicalKey(suffix)},
			Roots: []TenantRoot{{
				Tenant: tenant, Generation: 1, Logical: LogicalID("root-" + suffix),
			}},
			Reads: []MaterializationRequest{{
				Logical: logical,
				Inputs:  []PathRef{{Root: "root", Relative: "input-" + suffix}},
			}},
			Deletes: []Delete{{
				Logical: LogicalID("delete-" + suffix),
				Tenants: []TenantFence{{Tenant: tenant, Generation: 1}},
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	merger.finish()
	if len(merger.affected) != count || len(merger.roots) != count ||
		len(merger.reads) != count || len(merger.deletes) != count {
		t.Fatalf("keyed merge indexes = affected %d roots %d reads %d deletes %d",
			len(merger.affected), len(merger.roots), len(merger.reads), len(merger.deletes))
	}
	if len(target.AffectedKeys) != count || len(target.Roots) != count ||
		len(target.Reads) != count || len(target.Deletes) != count {
		t.Fatalf("merged plan sizes = affected %d roots %d reads %d deletes %d",
			len(target.AffectedKeys), len(target.Roots), len(target.Reads), len(target.Deletes))
	}
	for index := 1; index < len(target.AffectedKeys); index++ {
		if target.AffectedKeys[index-1] >= target.AffectedKeys[index] {
			t.Fatalf("affected keys are not sorted at %d", index)
		}
	}
}
