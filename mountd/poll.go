package mountd

// PollResult is the holder verdict from one Health+List exchange.
type PollResult struct {
	// Reachable is true when Health answered; false means the holder is gone
	// or wedged at the socket.
	Reachable bool
	// Degraded is true when Health succeeded but List failed (e.g. a wedged
	// mirror stalled List): Mounts is nil and must NOT be read as "no mounts."
	Degraded bool
	// Version is the holder's reported version, set whenever Reachable.
	Version string
	// Mounts is the holder's live-mount set, set only when Reachable and not
	// Degraded.
	Mounts []MountInfo
}

// Poll folds a Health+List exchange into a PollResult. The returned error is the
// RPC failure for log context only — callers route on the verdict booleans.
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
