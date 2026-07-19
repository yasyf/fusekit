package sourceauthority

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

type authorityView struct {
	fence   Fence
	roots   []RootSpec
	tenants []tenant.TenantSpec
	entries map[indexKey]IndexedEntry
}

type snapshotView struct {
	fence     Fence
	roots     []RootSpec
	tenants   []tenant.TenantSpec
	catalog   Store
	authority causal.SourceAuthorityID
	snapshot  string
}

type indexKey struct {
	root     RootID
	relative string
}

func (v authorityView) Fence() Fence {
	result := v.fence
	result.Streams = append([]StreamCheckpoint(nil), result.Streams...)
	return result
}

func (v authorityView) Roots() []RootSpec {
	return append([]RootSpec(nil), v.roots...)
}

func (v authorityView) Tenants() []tenant.TenantSpec {
	return append([]tenant.TenantSpec(nil), v.tenants...)
}

func (v authorityView) Entry(root RootID, relative string) (IndexedEntry, bool) {
	entry, found := v.entries[indexKey{root: root, relative: relative}]
	return cloneIndexedEntry(entry), found
}

func (v snapshotView) Fence() Fence {
	result := v.fence
	result.Streams = cloneCheckpoints(result.Streams)
	return result
}

func (v snapshotView) Roots() []RootSpec {
	return append([]RootSpec(nil), v.roots...)
}

func (v snapshotView) Tenants() []tenant.TenantSpec {
	return append([]tenant.TenantSpec(nil), v.tenants...)
}

func (v snapshotView) Scan(ctx context.Context, cursor ScanCursor, limit int) (ScanPage, error) {
	after, err := decodeScanCursor(cursor)
	if err != nil {
		return ScanPage{}, err
	}
	page, err := v.catalog.SourceSnapshotStagePage(ctx, v.authority, v.snapshot, after, limit)
	if err != nil {
		return ScanPage{}, err
	}
	result := ScanPage{Entries: make([]PhysicalEntry, len(page.Records))}
	for index, record := range page.Records {
		if err := json.Unmarshal(record.Payload, &result.Entries[index]); err != nil {
			return ScanPage{}, fmt.Errorf("%w: corrupt staged physical entry", ErrQuarantined)
		}
	}
	if page.Next.RootID != "" {
		encoded, err := json.Marshal(page.Next)
		if err != nil {
			return ScanPage{}, err
		}
		result.Next = ScanCursor(encoded)
	}
	return result, nil
}

func decodeScanCursor(cursor ScanCursor) (catalog.SourceIndexLocator, error) {
	if cursor == "" {
		return catalog.SourceIndexLocator{}, nil
	}
	var result catalog.SourceIndexLocator
	if err := json.Unmarshal([]byte(cursor), &result); err != nil || result.RootID == "" || result.Relative == "" {
		return catalog.SourceIndexLocator{}, fmt.Errorf("%w: invalid snapshot cursor", ErrInvalidPlan)
	}
	return result, nil
}

func cloneIndexedEntry(entry IndexedEntry) IndexedEntry {
	entry.Logical = append([]LogicalID(nil), entry.Logical...)
	return entry
}

func parentRelative(relative string) string {
	parent := path.Dir(relative)
	if parent == "." {
		return ""
	}
	return parent
}

func compareString(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
