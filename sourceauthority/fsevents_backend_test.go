package sourceauthority

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestTranslateFSEventsMapsOrderedFileEvents(t *testing.T) {
	t.Parallel()
	fence := testFSEventsFence()
	batches, cursor, halt, err := translateFSEvents(fence, 40, []nativeFSEvent{
		{path: "Users/test/source/a", flags: fseventItemCreated, id: 42},
		{path: "Users/test/source/b", flags: fseventItemModified, id: 42},
		{path: "Users/test/source/c", flags: fseventItemRemoved, id: 47},
	})
	if err != nil {
		t.Fatal(err)
	}
	if halt || cursor != 47 || len(batches) != 1 || len(batches[0].Events) != 3 {
		t.Fatalf("translation = (%+v, %d, %t), want one continuous batch through 47", batches, cursor, halt)
	}
	events := batches[0].Events
	if events[0].Relative != "a" || events[0].Kind != EventCreated || events[0].Ordinal != 1 ||
		events[1].Relative != "b" || events[1].Kind != EventModified || events[1].Ordinal != 2 ||
		events[2].Relative != "c" || events[2].Kind != EventRemoved || events[2].Ordinal != 1 {
		t.Fatalf("events = %+v", events)
	}
}

func TestTranslateFSEventsCollapsesOversizedCallbackToSnapshotFence(t *testing.T) {
	t.Parallel()
	native := make([]nativeFSEvent, maxEventBatchEvents+1)
	for index := range native {
		native[index] = nativeFSEvent{
			path: "Users/test/source/value", flags: fseventItemModified, id: 42,
		}
	}
	batches, cursor, halt, err := translateFSEvents(testFSEventsFence(), 40, native)
	if err != nil {
		t.Fatal(err)
	}
	if halt || cursor != 42 || len(batches) != 1 || len(batches[0].Events) != 1 {
		t.Fatalf("oversized translation = (%+v, %d, %t)", batches, cursor, halt)
	}
	event := batches[0].Events[0]
	if event.Relative != fseventsDiscontinuityRelative || !event.Flags.RequiresSnapshot() || event.ID != cursor {
		t.Fatalf("oversized callback snapshot event = %+v", event)
	}
	if err := validateBatch([]RootSpec{testFSEventsFence().root}, batches[0]); err != nil {
		t.Fatalf("bounded snapshot batch was rejected: %v", err)
	}
}

func TestTranslateFSEventsIgnoresHistoryDonePath(t *testing.T) {
	t.Parallel()
	batches, cursor, halt, err := translateFSEvents(testFSEventsFence(), 8, []nativeFSEvent{
		{path: "not/a/real/root/path", flags: fseventHistoryDone, id: 9},
	})
	if err != nil || halt || cursor != 8 || batches != nil {
		t.Fatalf("history sentinel = (%+v, %d, %t, %v), want ignored", batches, cursor, halt, err)
	}
}

func TestTranslateFSEventsMapsDroppedBatchToSnapshot(t *testing.T) {
	t.Parallel()
	batches, cursor, halt, err := translateFSEvents(testFSEventsFence(), 10, []nativeFSEvent{
		{path: "/", flags: fseventMustScanSubDirs | fseventKernelDropped, id: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := batches[0].Events[0]
	if halt || cursor != 20 || event.Relative != fseventsDiscontinuityRelative ||
		event.Flags != FlagMustScanSubDirs|FlagKernelDropped {
		t.Fatalf("dropped event = (%+v, %d, %t)", event, cursor, halt)
	}
}

func TestTranslateFSEventsMakesRootChangeTerminalDiscontinuity(t *testing.T) {
	t.Parallel()
	batches, cursor, halt, err := translateFSEvents(testFSEventsFence(), 99, []nativeFSEvent{
		{path: "Users/test/source", flags: fseventRootChanged, id: 0},
		{path: "Users/test/source/ignored", flags: fseventItemCreated, id: 100},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !halt || cursor != 99 || len(batches) != 1 {
		t.Fatalf("root change = (%+v, %d, %t)", batches, cursor, halt)
	}
	batch := batches[0]
	if batch.Predecessor != 0 || batch.Cursor != 0 || len(batch.Events) != 1 ||
		batch.Events[0].ID != 0 || batch.Events[0].Relative != "." || batch.Events[0].Flags&FlagRootChanged == 0 {
		t.Fatalf("root discontinuity = %+v", batch)
	}
}

func TestTranslateFSEventsMakesIDWrapTerminalDiscontinuity(t *testing.T) {
	t.Parallel()
	batches, cursor, halt, err := translateFSEvents(testFSEventsFence(), 1<<63, []nativeFSEvent{
		{path: "Users/test/source/a", flags: fseventIDsWrapped | fseventItemModified, id: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := batches[0].Events[0]
	if !halt || cursor != 1<<63 || event.ID != 0 ||
		event.Flags&(FlagEventIDsWrapped|FlagRootChanged) != FlagEventIDsWrapped|FlagRootChanged {
		t.Fatalf("wrapped event = (%+v, %d, %t)", event, cursor, halt)
	}
}

func TestTranslateFSEventsRejectsOrdinaryEscapedPath(t *testing.T) {
	t.Parallel()
	_, _, _, err := translateFSEvents(testFSEventsFence(), 3, []nativeFSEvent{
		{path: "Users/other/file", flags: fseventItemModified, id: 4},
	})
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("error = %v, want ErrInvalidEvent", err)
	}
}

func TestMapFSEventFlagsIncludesMountDiscontinuity(t *testing.T) {
	t.Parallel()
	flags := mapFSEventFlags(fseventUserDropped | fseventMount | fseventUnmount)
	if flags != FlagUserDropped|FlagMustScanSubDirs {
		t.Fatalf("flags = %b", flags)
	}
}

func TestValidateFSEventsOpenRejectsUnconsumedResume(t *testing.T) {
	t.Parallel()
	root := testFSEventsFence().root
	stale := root
	stale.Path = "/Users/test/stale"
	_, err := validateFSEventsOpen([]RootSpec{root}, []StreamCheckpoint{{
		Identity: fseventsStreamIdentity(stale), Cursor: 4, RootEpoch: "epoch",
	}})
	if !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("error = %v, want ErrInvalidEvent", err)
	}
}

func TestTranslateFSEventsExactFileIgnoresSiblings(t *testing.T) {
	t.Parallel()
	fence := testFSEventsFence()
	fence.root.Kind = RootFile
	fence.root.Path = "/Users/test/.claude.json"
	fence.devicePath = "Users/test/.claude.json"
	batches, cursor, halt, err := translateFSEvents(fence, 11, []nativeFSEvent{
		{path: "Users/test/settings.json", flags: fseventItemModified, id: 12},
		{path: "Users/test/.claude.json", flags: fseventItemRenamed, id: 14},
	})
	if err != nil {
		t.Fatal(err)
	}
	if halt || cursor != 14 || len(batches) != 1 || len(batches[0].Events) != 1 {
		t.Fatalf("file translation = (%+v, %d, %t)", batches, cursor, halt)
	}
	event := batches[0].Events[0]
	if event.Relative != "." || event.Kind != EventRenamed || event.ID != 14 {
		t.Fatalf("file event = %+v", event)
	}
}

func TestTranslateFSEventsExactFileRetainsDroppedDiscontinuity(t *testing.T) {
	t.Parallel()
	fence := testFSEventsFence()
	fence.root.Kind = RootFile
	fence.devicePath = "Users/test/.claude.json"
	batches, cursor, halt, err := translateFSEvents(fence, 20, []nativeFSEvent{
		{path: "/", flags: fseventMustScanSubDirs | fseventUserDropped, id: 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := batches[0].Events[0]
	if halt || cursor != 30 || event.Relative != "." || !event.Flags.RequiresSnapshot() {
		t.Fatalf("file drop = (%+v, %d, %t)", event, cursor, halt)
	}
}

func testFSEventsFence() fseventsRootFence {
	return fseventsRootFence{
		root: RootSpec{
			Authority: causal.SourceAuthorityID("authority"), ID: RootID("root"),
			Path: "/Users/test/source", Kind: RootDirectory, Generation: 1,
		},
		stream: "stream", epoch: "epoch", devicePath: "Users/test/source",
	}
}
