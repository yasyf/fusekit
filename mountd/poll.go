package mountd

// PollResult is one holder verdict from a single Health+List exchange — the
// reachability/liveness state a supervising consumer keys on, distilled from
// the two RPCs so the consumer never re-derives "alive but its mounts are
// unreadable" from raw call outcomes.
type PollResult struct {
	// Reachable is true when Health answered: the socket is live and the holder
	// is responsive. When false the holder is gone or wedged at the socket.
	Reachable bool
	// Degraded is true when Health succeeded but List failed — the holder is
	// alive at a known Version, but its live-mount set could not be read (e.g. a
	// wedged or slow mirror stalled List past its deadline). Mounts is nil; the
	// consumer must NOT treat the absence as "no mounts."
	Degraded bool
	// Version is the holder's reported version, set whenever Reachable.
	Version string
	// Mounts is the holder's live-mount set, set only when Reachable and not
	// Degraded.
	Mounts []MountInfo
}

// Poll runs Health then, if it answered, List, folding the two-stage outcome
// into a PollResult: a Health failure is an unreachable holder (Reachable
// false); a Health success with a List failure is a reachable-but-degraded
// holder (alive, version known, mounts unreadable). The returned error is the
// underlying RPC failure for the caller's log line — the verdict booleans
// already encode it, so callers route on the booleans and read err only for
// context.
func (c *Client) Poll() (PollResult, error) {
	ver, err := c.Health()
	if err != nil {
		return PollResult{}, err
	}
	mounts, err := c.List()
	if err != nil {
		return PollResult{Reachable: true, Degraded: true, Version: ver}, err
	}
	return PollResult{Reachable: true, Version: ver, Mounts: mounts}, nil
}
