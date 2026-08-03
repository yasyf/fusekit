package transportproto

import "github.com/yasyf/daemonkit"

// frameEnvelopeReserve mirrors daemonkit's own terminal reserve, the bytes a
// frame spends outside the payload it carries.
const frameEnvelopeReserve daemonkit.Bytes = 4 << 10

// FrameForPayload sizes a contract's MaxFrame from the payload it must carry.
// daemonkit base64s a terminal's bytes and reserves the envelope on top, so a
// frame is never the payload ceiling: a contract declaring MaxFrame == payload
// silently loses a quarter of its budget. Every contract fusekit declares sizes
// through this, and pins the result with daemonkit.MaxDetail.
func FrameForPayload(payload daemonkit.Bytes) daemonkit.Bytes {
	if payload <= 0 {
		return 0
	}
	return (payload*4+2)/3 + frameEnvelopeReserve
}
