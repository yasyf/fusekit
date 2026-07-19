// Package sourcedriverservice binds SourceDriver exclusively to daemonkit wire.
package sourcedriverservice

import (
	"fmt"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

// RemoteError is one stable application error returned by a source driver.
type RemoteError struct {
	Code    sourcedriverproto.ErrorCode
	Message string
	Actual  string
}

// Error implements error.
func (e *RemoteError) Error() string { return e.Message }

// TransportError is an untyped daemonkit session or terminal failure.
type TransportError struct {
	Outcome wire.Outcome
	Message string
}

// Error implements error.
func (e *TransportError) Error() string { return e.Message }

func exactBuild(actual string) error {
	if actual != sourcedriverproto.Build {
		return fmt.Errorf("source driver service: daemonkit build %q does not match exact schema %q", actual, sourcedriverproto.Build)
	}
	return nil
}
