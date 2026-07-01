package fileproviderd

// Aliases re-exporting the content bridge, keeping the FP backend and its
// tests source-stable.

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
