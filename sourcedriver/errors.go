package sourcedriver

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidValue means a source driver value violates the closed contract.
	ErrInvalidValue = errors.New("source driver: invalid value")
	// ErrIntegrity means a source driver returned a contradictory proof.
	ErrIntegrity = errors.New("source driver: integrity failure")
	// ErrNotFound means an immutable source object does not exist.
	ErrNotFound = errors.New("source driver: not found")
	// ErrConflict means an operation ID or immutable source fact disagrees.
	ErrConflict = errors.New("source driver: conflict")
)

// SnapshotRequiredError means an incremental predecessor cannot be continued.
type SnapshotRequiredError struct {
	From RevisionToken
	Head RevisionToken
}

// Error implements error.
func (e *SnapshotRequiredError) Error() string {
	return fmt.Sprintf("source driver: snapshot required from %q at head %q", e.From, e.Head)
}

// StaleRevisionError means a mutation expected a source revision other than head.
type StaleRevisionError struct {
	Expected RevisionToken
	Actual   RevisionToken
}

// Error implements error.
func (e *StaleRevisionError) Error() string {
	return fmt.Sprintf("source driver: stale revision %q; current revision is %q", e.Expected, e.Actual)
}
