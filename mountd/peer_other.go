//go:build !darwin

package mountd

import "errors"

// errPeerUnsupported is returned by peerPID off darwin, where LOCAL_PEERPID has
// no portable equivalent. It is deliberately distinct from ErrUnreachable: a
// dialable socket whose peer simply cannot be read is not the same condition as
// "no peer," and a consumer that needs the peer-gated kill is darwin-only by
// construction (the fuse-t mount holder it identifies only runs on macOS).
var errPeerUnsupported = errors.New("peer pid lookup is darwin-only")

func peerPID(string) (int, error) {
	return 0, errPeerUnsupported
}
