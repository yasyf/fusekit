package proc

import "time"

// Backoff is an exponential backoff: the wait doubles per failure from Base up
// to Cap.
type Backoff struct {
	// Base is the wait after the first failure.
	Base time.Duration
	// Cap is the ceiling the wait is clamped to.
	Cap time.Duration
}

// After returns the wait after `failures` consecutive failures; a non-positive
// count still waits Base.
func (b Backoff) After(failures int) time.Duration {
	d := b.Base
	for i := 1; i < failures && d < b.Cap; i++ {
		d *= 2
	}
	if d > b.Cap {
		d = b.Cap
	}
	return d
}
