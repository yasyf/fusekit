//go:build fuse && cgo && darwin

package main

import (
	"io"
	"log"
	"testing"
)

// TestNewServerValidates pins P-17: the EXACT Server construction main runs
// passes Run's config validation — a holder that cannot start (missing
// LeaseDir, missing Host) must fail this test, not the user's launchd log.
func TestNewServerValidates(t *testing.T) {
	s, err := newServer("/tmp/fusekit-test-holder.sock", log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("newServer = %v, want a Validate-clean server", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	if s.LeaseDir == "" {
		t.Fatal("LeaseDir empty; the cask holder must wire lease.DefaultRoot()")
	}
	if s.JournalPath == "" || s.RetireSkew == nil || s.Probe == nil {
		t.Fatalf("holder wiring incomplete: journal=%q retireSkew=%v probe=%v", s.JournalPath, s.RetireSkew != nil, s.Probe != nil)
	}
}
