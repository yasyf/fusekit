//go:build !darwin

package carcass

// fuse-t is macOS-only: there are no go-nfsv4 servers to prove dead or reap
// off darwin.

func ensureServersDead(string) error { return nil }

// ReapOwnChildren is a no-op off darwin.
func ReapOwnChildren(string) {}

// ReapOrphaned is a no-op off darwin.
func ReapOrphaned([]string) []int { return nil }
