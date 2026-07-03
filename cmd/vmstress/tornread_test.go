//go:build darwin

package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// renderEnvelope builds the exact bytes ReadSynth serves (envelope JSON + newline).
func renderEnvelope(t *testing.T, gen int64, payload []byte) []byte {
	t.Helper()
	buf, err := json.Marshal(envelope{Domain: vmDomain, Name: synthName, Gen: gen, Payload: payload})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	return append(buf, '\n')
}

func renderWriterPayload(t *testing.T, seq int64, n int) []byte {
	t.Helper()
	buf, err := json.Marshal(tornPayload{Seq: seq, N: n, Fill: strings.Repeat("t", n)})
	if err != nil {
		t.Fatalf("render payload: %v", err)
	}
	return buf
}

func TestValidateEnvelope(t *testing.T) {
	writerBody := renderEnvelope(t, 7, renderWriterPayload(t, 42, 64))
	cases := []struct {
		name    string
		buf     []byte
		floor   int64
		wantGen int64
		wantSeq int64
		wantErr string
	}{
		{name: "seed payload is envelope-checked only", buf: renderEnvelope(t, 3, []byte(seedConfig)), wantGen: 3},
		{name: "writer payload yields seq", buf: writerBody, floor: 7, wantGen: 7, wantSeq: 42},
		{name: "clamped read fails to parse", buf: writerBody[:len(writerBody)/2], wantErr: "does not parse"},
		{name: "single-byte clamp fails to parse", buf: writerBody[:len(writerBody)-2], wantErr: "does not parse"},
		{name: "gen regression detected", buf: renderEnvelope(t, 4, []byte(seedConfig)), floor: 9, wantErr: "gen regressed"},
		{name: "fill shorter than declared", buf: renderEnvelope(t, 1, []byte(`{"seq":5,"n":10,"fill":"short"}`)), wantErr: "declares 10 fill bytes, carries 5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gen, seq, err := validateEnvelope(tc.buf, tc.floor)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gen != tc.wantGen || seq != tc.wantSeq {
				t.Fatalf("got gen=%d seq=%d, want gen=%d seq=%d", gen, seq, tc.wantGen, tc.wantSeq)
			}
		})
	}
}
