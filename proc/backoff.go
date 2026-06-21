package proc

import "time"

// Backoff is an exponential backoff with a cap: the wait doubles per failure
// from Base up to Cap. Callers supply both bounds, so it carries no policy of
// its own.
type Backoff struct {
	// Base is the wait after the first failure and the floor for any positive
	// failure count.
	Base time.Duration
	// Cap is the ceiling the doubling wait is clamped to.
	Cap time.Duration
}

// After returns the wait after `failures` consecutive failures: Base doubling
// per failure, capped at Cap. Zero or negative failure counts never shrink the
// wait below Base.
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
