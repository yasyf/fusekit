// Package sourcedriverservice binds SourceDriver exclusively to daemonkit.
package sourcedriverservice

import (
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
