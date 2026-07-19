package sourceauthority

import (
	"errors"
	"testing"
)

func TestValidateRequestsAllowsRetainedClaudeJSONCounterpart(t *testing.T) {
	private := indexKey{root: "private:acct-18", relative: ".claude.json"}
	canonical := indexKey{root: "canonical-json", relative: "."}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		private:   indexedLogical(private, "effective:claude:acct-18"),
		canonical: indexedLogical(canonical, "effective:claude:acct-18"),
	}}
	request := MaterializationRequest{
		Logical: "effective:claude:acct-18",
		Inputs:  []PathRef{pathRef(canonical), pathRef(private)},
	}
	if err := validateRequests([]MaterializationRequest{request}, view, map[indexKey]struct{}{private: {}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRequestsAllowsRetainedSettingsCounterpart(t *testing.T) {
	private := indexKey{root: "private:acct-18", relative: "settings.json"}
	canonical := indexKey{root: "shared", relative: "settings.json"}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		private:   indexedLogical(private, "effective:settings:acct-18"),
		canonical: indexedLogical(canonical, "effective:settings:acct-18"),
	}}
	request := MaterializationRequest{
		Logical: "effective:settings:acct-18",
		Inputs:  []PathRef{pathRef(private), pathRef(canonical)},
	}
	if err := validateRequests([]MaterializationRequest{request}, view, map[indexKey]struct{}{canonical: {}}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRequestsRejectsUnboundRetainedPathInjection(t *testing.T) {
	private := indexKey{root: "private:acct-18", relative: ".claude.json"}
	unrelated := indexKey{root: "shared", relative: "history.jsonl"}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		private:   indexedLogical(private, "effective:claude:acct-18"),
		unrelated: indexedLogical(unrelated, "shared:history"),
	}}
	request := MaterializationRequest{
		Logical: "effective:claude:acct-18",
		Inputs:  []PathRef{pathRef(private), pathRef(unrelated)},
	}
	err := validateRequests([]MaterializationRequest{request}, view, map[indexKey]struct{}{private: {}})
	if !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlan", err)
	}
}

func TestValidateRequestsRejectsNewLogicalWithUnrelatedInput(t *testing.T) {
	current := indexKey{root: "shared", relative: "new.json"}
	counterpart := indexKey{root: "private:acct-18", relative: "new.json"}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		current:     indexedLogical(current),
		counterpart: indexedLogical(counterpart),
	}}
	request := MaterializationRequest{
		Logical: "effective:new:acct-18",
		Inputs:  []PathRef{pathRef(counterpart), pathRef(current)},
	}
	err := validateRequests([]MaterializationRequest{request}, view, map[indexKey]struct{}{current: {}})
	if !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlan", err)
	}
}

func TestValidateRequestsRequiresCurrentEventInput(t *testing.T) {
	private := indexKey{root: "private:acct-18", relative: ".claude.json"}
	canonical := indexKey{root: "canonical-json", relative: "."}
	view := authorityView{entries: map[indexKey]IndexedEntry{
		private:   indexedLogical(private, "effective:claude:acct-18"),
		canonical: indexedLogical(canonical, "effective:claude:acct-18"),
	}}
	request := MaterializationRequest{
		Logical: "effective:claude:acct-18",
		Inputs:  []PathRef{pathRef(canonical)},
	}
	err := validateRequests([]MaterializationRequest{request}, view, map[indexKey]struct{}{private: {}})
	if !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlan", err)
	}
}

func indexedLogical(key indexKey, logical ...LogicalID) IndexedEntry {
	return IndexedEntry{
		Physical: PhysicalEntry{Root: key.root, Relative: key.relative, Exists: true},
		Logical:  logical,
	}
}

func pathRef(key indexKey) PathRef {
	return PathRef{Root: key.root, Relative: key.relative}
}
