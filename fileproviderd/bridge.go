package fileproviderd

// The content bridge — its classification types, wire, server, and client — now
// lives in the neutral content package, shared by this File Provider backend and
// the fuse holder. These aliases keep the FP backend and its tests source-stable.

import "github.com/yasyf/fusekit/content"

type (
	EntryKind      = content.EntryKind
	Entry          = content.Entry
	ContentSource  = content.Source
	BridgeOp       = content.BridgeOp
	BridgeRequest  = content.BridgeRequest
	BridgeResponse = content.BridgeResponse
	BridgeServer   = content.BridgeServer
	BridgeClient   = content.BridgeClient
)

const (
	EntrySymlink = content.EntrySymlink
	EntrySynth   = content.EntrySynth
	EntryPrivate = content.EntryPrivate

	BridgeProtoVersion = content.BridgeProtoVersion
	BridgeOpManifest   = content.BridgeOpManifest
	BridgeOpRead       = content.BridgeOpRead
	BridgeOpWrite      = content.BridgeOpWrite
	BridgeOpClassify   = content.BridgeOpClassify
)

var (
	NewBridgeClient      = content.NewBridgeClient
	ErrBridgeUnavailable = content.ErrBridgeUnavailable
)
