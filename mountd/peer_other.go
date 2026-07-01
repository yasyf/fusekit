//go:build !darwin

package mountd

import "errors"

// errPeerUnsupported is returned by peerPID off darwin, where LOCAL_PEERPID has
// no portable equivalent; deliberately distinct from ErrUnreachable (an
// unreadable peer is not "no peer").
var errPeerUnsupported = errors.New("peer pid lookup is darwin-only")

func peerPID(string) (int, error) {
	return 0, errPeerUnsupported
}
