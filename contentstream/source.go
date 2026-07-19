// Package contentstream defines ownership transfer for streamed content.
package contentstream

import (
	"context"
	"io"
)

// Source is one owned producer stream. Settle is exact-idempotent: nil proves
// complete consumption, while a non-nil error aborts production. Settle never
// blocks on producer teardown. Wait must force-stop on context cancellation and
// return only after producer resources have joined.
type Source interface {
	io.Reader
	Settle(error) error
	Wait(context.Context) error
}
