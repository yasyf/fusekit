package sourceauthority

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	fseventMustScanSubDirs uint32 = 0x00000001
	fseventUserDropped     uint32 = 0x00000002
	fseventKernelDropped   uint32 = 0x00000004
	fseventIDsWrapped      uint32 = 0x00000008
	fseventHistoryDone     uint32 = 0x00000010
	fseventRootChanged     uint32 = 0x00000020
	fseventMount           uint32 = 0x00000040
	fseventUnmount         uint32 = 0x00000080
	fseventItemCreated     uint32 = 0x00000100
	fseventItemRemoved     uint32 = 0x00000200
	fseventItemInodeMeta   uint32 = 0x00000400
	fseventItemRenamed     uint32 = 0x00000800
	fseventItemModified    uint32 = 0x00001000
	fseventItemFinderInfo  uint32 = 0x00002000
	fseventItemChangeOwner uint32 = 0x00004000
	fseventItemXattr       uint32 = 0x00008000
	fseventItemCloned      uint32 = 0x00400000
)

const fseventsDiscontinuityRelative = ".fusekit-source-discontinuity"

type nativeFSEvent struct {
	path  string
	flags uint32
	id    EventID
}

type fseventsRootFence struct {
	root       RootSpec
	stream     StreamIdentity
	epoch      RootEpoch
	devicePath string
}

func validateFSEventsOpen(roots []RootSpec, resume []StreamCheckpoint) (map[StreamIdentity]StreamCheckpoint, error) {
	if len(roots) == 0 {
		return nil, fmt.Errorf("%w: FSEvents roots are required", ErrInvalidEvent)
	}
	expected := make(map[StreamIdentity]struct{}, len(roots))
	rootIDs := make(map[RootID]struct{}, len(roots))
	rootPaths := make(map[string]struct{}, len(roots))
	authority := roots[0].Authority
	for _, root := range roots {
		if authority == "" || root.Authority != authority || root.ID == "" || root.Generation == 0 || (root.Kind != RootFile && root.Kind != RootDirectory) ||
			!filepath.IsAbs(root.Path) || filepath.Clean(root.Path) != root.Path {
			return nil, fmt.Errorf("%w: invalid FSEvents root", ErrInvalidEvent)
		}
		if err := validatePinnedTaskRoot(root); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
		}
		if _, duplicate := rootIDs[root.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate FSEvents root id", ErrInvalidEvent)
		}
		if _, duplicate := rootPaths[root.Path]; duplicate {
			return nil, fmt.Errorf("%w: duplicate FSEvents root path", ErrInvalidEvent)
		}
		rootIDs[root.ID], rootPaths[root.Path] = struct{}{}, struct{}{}
		identity := fseventsStreamIdentity(root)
		expected[identity] = struct{}{}
	}
	resumeByIdentity := make(map[StreamIdentity]StreamCheckpoint, len(resume))
	for _, checkpoint := range resume {
		if checkpoint.Identity == "" || checkpoint.RootEpoch == "" {
			return nil, fmt.Errorf("%w: incomplete FSEvents resume checkpoint", ErrInvalidEvent)
		}
		if _, duplicate := resumeByIdentity[checkpoint.Identity]; duplicate {
			return nil, fmt.Errorf("%w: duplicate FSEvents resume checkpoint", ErrInvalidEvent)
		}
		if _, ok := expected[checkpoint.Identity]; !ok {
			return nil, fmt.Errorf("%w: unconsumed FSEvents resume checkpoint", ErrInvalidEvent)
		}
		resumeByIdentity[checkpoint.Identity] = checkpoint
	}
	return resumeByIdentity, nil
}

func fseventsStreamIdentity(root RootSpec) StreamIdentity {
	digest := sha256.Sum256([]byte(fmt.Sprintf("fsevents-stream-v1\x00%s\x00%s\x00%s\x00%d\x00%d", root.Authority, root.ID, root.Path, root.Kind, root.Generation)))
	return StreamIdentity("fsevents:" + hex.EncodeToString(digest[:]))
}

// translateFSEvents converts one already-copied native callback. A zero-cursor
// batch is terminal for this stream instance: its caller must durably deliver
// it and reopen the device-relative stream before accepting more native IDs.
func translateFSEvents(fence fseventsRootFence, predecessor EventID, native []nativeFSEvent) ([]EventBatch, EventID, bool, error) {
	events := make([]PathEvent, 0, min(len(native), maxEventBatchEvents+1))
	ordinalByID := make(map[EventID]EventOrdinal)
	cursor := predecessor
	overflow := false

	for _, raw := range native {
		if raw.flags&fseventHistoryDone != 0 {
			continue
		}
		flags := mapFSEventFlags(raw.flags)
		if raw.flags&(fseventRootChanged|fseventIDsWrapped) != 0 {
			flags |= FlagRootChanged
			return []EventBatch{{
				Stream: fence.stream, Predecessor: 0, Cursor: 0, RootEpoch: fence.epoch,
				Events: []PathEvent{{
					Root: fence.root.ID, Relative: ".", Kind: EventMetadata,
					Flags: flags, ID: 0, Ordinal: 1,
				}},
			}}, predecessor, true, nil
		}
		if raw.id <= predecessor {
			continue
		}
		if overflow {
			if raw.id > cursor {
				cursor = raw.id
			}
			continue
		}

		var relative string
		if fence.root.Kind == RootFile {
			matched := sameFSEventPath(fence.devicePath, raw.path)
			if !matched && !flags.RequiresSnapshot() {
				continue
			}
			relative = "."
		} else {
			var atRoot bool
			var err error
			relative, atRoot, err = relativeFSEventPath(fence.devicePath, raw.path)
			if err != nil {
				if !flags.RequiresSnapshot() {
					return nil, predecessor, false, err
				}
				relative = fseventsDiscontinuityRelative
			}
			if atRoot {
				flags |= FlagMustScanSubDirs
				relative = fseventsDiscontinuityRelative
			}
		}
		ordinalByID[raw.id]++
		events = append(events, PathEvent{
			Root: fence.root.ID, Relative: relative, Kind: mapFSEventKind(raw.flags),
			Flags: flags, ID: raw.id, Ordinal: ordinalByID[raw.id],
		})
		if len(events) > maxEventBatchEvents {
			events = nil
			overflow = true
		}
		if raw.id > cursor {
			cursor = raw.id
		}
	}
	if len(events) == 0 && !overflow {
		return nil, predecessor, false, nil
	}
	if overflow {
		relative := fseventsDiscontinuityRelative
		if fence.root.Kind == RootFile {
			relative = "."
		}
		events = []PathEvent{{
			Root: fence.root.ID, Relative: relative, Kind: EventMetadata,
			Flags: FlagMustScanSubDirs, ID: cursor, Ordinal: 1,
		}}
	}
	return []EventBatch{{
		Stream: fence.stream, Predecessor: predecessor, Cursor: cursor,
		RootEpoch: fence.epoch, Events: events,
	}}, cursor, false, nil
}

func sameFSEventPath(expected, event string) bool {
	expected = strings.TrimPrefix(filepath.Clean("/"+expected), "/")
	event = strings.TrimPrefix(filepath.Clean("/"+event), "/")
	return expected == event
}

func mapFSEventFlags(flags uint32) BatchFlags {
	var mapped BatchFlags
	if flags&fseventMustScanSubDirs != 0 {
		mapped |= FlagMustScanSubDirs
	}
	if flags&fseventUserDropped != 0 {
		mapped |= FlagUserDropped
	}
	if flags&fseventKernelDropped != 0 {
		mapped |= FlagKernelDropped
	}
	if flags&fseventIDsWrapped != 0 {
		mapped |= FlagEventIDsWrapped
	}
	if flags&fseventRootChanged != 0 {
		mapped |= FlagRootChanged
	}
	if flags&(fseventMount|fseventUnmount) != 0 {
		mapped |= FlagMustScanSubDirs
	}
	return mapped
}

func mapFSEventKind(flags uint32) EventKind {
	switch {
	case flags&fseventItemRemoved != 0:
		return EventRemoved
	case flags&fseventItemRenamed != 0:
		return EventRenamed
	case flags&fseventItemCreated != 0:
		return EventCreated
	case flags&(fseventItemModified|fseventItemCloned) != 0:
		return EventModified
	case flags&(fseventItemInodeMeta|fseventItemFinderInfo|fseventItemChangeOwner|fseventItemXattr) != 0:
		return EventMetadata
	default:
		return EventModified
	}
}

func relativeFSEventPath(root, event string) (string, bool, error) {
	root = strings.TrimPrefix(filepath.Clean("/"+root), "/")
	event = strings.TrimPrefix(filepath.Clean("/"+event), "/")
	if event == root {
		return "", true, nil
	}
	prefix := root
	if prefix != "" {
		prefix += "/"
	}
	if !strings.HasPrefix(event, prefix) {
		return "", false, fmt.Errorf("%w: FSEvents path escaped watched root", ErrInvalidEvent)
	}
	relative := strings.TrimPrefix(event, prefix)
	if relative == "" || relative == "." || filepath.IsAbs(relative) || filepath.Clean(relative) != relative ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) || strings.ContainsRune(relative, 0) {
		return "", false, fmt.Errorf("%w: invalid FSEvents relative path", ErrInvalidEvent)
	}
	return relative, false, nil
}
