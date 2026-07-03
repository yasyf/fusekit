//go:build darwin

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMutateDetachedVictimAtomicExactAndFixedLength pins the drill mutation's
// contract: the consumer copy holds exactly the returned payload, the write
// leaves no sibling temp behind (temp + rename — an observer can never see a
// truncate window), and the payload length is identical across cycles (the
// equal-size re-attach case the coherence assertion exists to exercise).
func TestMutateDetachedVictimAtomicExactAndFixedLength(t *testing.T) {
	dir := t.TempDir()
	tn := muxTenant{name: "acct-99", consumerDir: dir}
	if err := os.WriteFile(filepath.Join(dir, privateSynthName), []byte("pre-existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	want0, err := mutateDetachedVictim(tn, 0)
	if err != nil {
		t.Fatalf("mutate cycle 0: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, privateSynthName))
	if err != nil {
		t.Fatalf("read consumer copy: %v", err)
	}
	if !bytes.Equal(got, want0) {
		t.Fatalf("consumer copy = %q, want the returned payload %q", got, want0)
	}

	want1, err := mutateDetachedVictim(tn, 1)
	if err != nil {
		t.Fatalf("mutate cycle 1: %v", err)
	}
	if len(want1) != len(want0) {
		t.Fatalf("payload length varies across cycles (%d vs %d) — the equal-size contract the drill's comment forbids breaking", len(want1), len(want0))
	}
	if bytes.Equal(want1, want0) {
		t.Fatal("cycle 1 payload equals cycle 0's — the nonce must change every cycle")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != privateSynthName {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("consumer dir holds %v, want only %q — the atomic write must not leave temp litter", names, privateSynthName)
	}
}

// TestReadSynthEnvelopeRetryRejectsEmptyPayload pins that a clean parse with an
// EMPTY payload is not success: no envelope in this harness legitimately renders
// one, so the retry helper must keep retrying and fail loud — never hand an
// empty payload to a caller whose assertions only check the domain.
func TestReadSynthEnvelopeRetryRejectsEmptyPayload(t *testing.T) {
	writeEnvelope := func(t *testing.T, dir string, payload []byte) {
		t.Helper()
		buf, err := json.Marshal(envelope{Domain: "d", Name: privateSynthName, Gen: 1, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, privateSynthName), append(buf, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("empty payload fails loud", func(t *testing.T) {
		dir := t.TempDir()
		writeEnvelope(t, dir, []byte{})
		_, _, err := readSynthEnvelopeRetry(dir)
		if err == nil || !strings.Contains(err.Error(), "empty payload") {
			t.Fatalf("err = %v, want the loud empty-payload failure", err)
		}
	})

	t.Run("non-empty payload succeeds", func(t *testing.T) {
		dir := t.TempDir()
		writeEnvelope(t, dir, []byte(`{"tenant":"acct-01"}`))
		env, _, err := readSynthEnvelopeRetry(dir)
		if err != nil {
			t.Fatalf("err = %v, want clean parse", err)
		}
		if env.Domain != "d" || string(env.Payload) != `{"tenant":"acct-01"}` {
			t.Fatalf("parsed envelope = %+v, want the written domain and payload", env)
		}
	})
}
