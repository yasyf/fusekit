package content

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func writeFile(t *testing.T, p, s string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
}

// bumpMtime advances p's mtime by a whole second so a same-content re-hash still
// observes a different (mtime_ns, size) tuple.
func bumpMtime(t *testing.T, p string) {
	t.Helper()
	fi, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	next := fi.ModTime().Add(time.Second)
	if err := os.Chtimes(p, next, next); err != nil {
		t.Fatal(err)
	}
}

// TestFreshnessVersion pins the deterministic freshness key: it moves on an mtime
// bump, a size change, and an absent->present transition, stays stable across an
// unchanged re-read, and fails loud on a non-ENOENT lstat errno.
func TestFreshnessVersion(t *testing.T) {
	t.Run("deterministic across an unchanged re-read", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "a")
		writeFile(t, f, "hello")
		v1, err := FreshnessVersion([]string{f})
		if err != nil {
			t.Fatal(err)
		}
		v2, err := FreshnessVersion([]string{f})
		if err != nil {
			t.Fatal(err)
		}
		if v1 != v2 {
			t.Fatalf("FreshnessVersion not deterministic: %q vs %q", v1, v2)
		}
	})

	t.Run("mutations that must move the version", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(t *testing.T, f string)
		}{
			{name: "mtime bump", mutate: func(t *testing.T, f string) { bumpMtime(t, f) }},
			{name: "size change", mutate: func(t *testing.T, f string) { writeFile(t, f, "hello world") }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				f := filepath.Join(dir, "a")
				writeFile(t, f, "hello")
				before, err := FreshnessVersion([]string{f})
				if err != nil {
					t.Fatal(err)
				}
				tc.mutate(t, f)
				after, err := FreshnessVersion([]string{f})
				if err != nil {
					t.Fatal(err)
				}
				if before == after {
					t.Fatalf("%s did not move the version: %q", tc.name, before)
				}
			})
		}
	})

	t.Run("absent file is a stable marker, distinct from present", func(t *testing.T) {
		dir := t.TempDir()
		absent := filepath.Join(dir, "missing")
		gone1, err := FreshnessVersion([]string{absent})
		if err != nil {
			t.Fatalf("absent path errored, want a stable marker: %v", err)
		}
		gone2, err := FreshnessVersion([]string{absent})
		if err != nil {
			t.Fatal(err)
		}
		if gone1 != gone2 {
			t.Fatalf("absent marker not stable: %q vs %q", gone1, gone2)
		}
		writeFile(t, absent, "born")
		present, err := FreshnessVersion([]string{absent})
		if err != nil {
			t.Fatal(err)
		}
		if present == gone1 {
			t.Fatal("absent->present transition did not move the version")
		}
	})

	t.Run("non-ENOENT errno fails loud", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		writeFile(t, file, "x")
		notdir := filepath.Join(file, "child") // lstat through a non-dir -> ENOTDIR
		if _, err := FreshnessVersion([]string{notdir}); err == nil {
			t.Fatal("FreshnessVersion over an ENOTDIR path = nil, want a loud error")
		}
	})
}

// TestFingerprint pins the manifest fingerprint: order-independent (sorted by
// Name), moved by any identity field, moved by a Freshness path's mtime/size, and
// loud on a non-ENOENT Freshness lstat errno.
func TestFingerprint(t *testing.T) {
	t.Run("order-independent", func(t *testing.T) {
		a := synth("a", "v1")
		b := synth("b", "v1")
		fp1, err := Fingerprint([]Entry{a, b})
		if err != nil {
			t.Fatal(err)
		}
		fp2, err := Fingerprint([]Entry{b, a})
		if err != nil {
			t.Fatal(err)
		}
		if fp1 != fp2 {
			t.Fatalf("Fingerprint depends on manifest order: %q vs %q", fp1, fp2)
		}
	})

	t.Run("each identity field moves the fingerprint", func(t *testing.T) {
		base := Entry{Name: "x", Kind: EntrySynth, Target: "/t", Private: false, Version: "v1", Size: 10}
		baseFP, err := Fingerprint([]Entry{base})
		if err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name string
			edit func(e *Entry)
		}{
			{name: "name", edit: func(e *Entry) { e.Name = "y" }},
			{name: "kind", edit: func(e *Entry) { e.Kind = EntryPrivate }},
			{name: "target", edit: func(e *Entry) { e.Target = "/u" }},
			{name: "private", edit: func(e *Entry) { e.Private = true }},
			{name: "version", edit: func(e *Entry) { e.Version = "v2" }},
			{name: "size", edit: func(e *Entry) { e.Size = 11 }},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := base
				tc.edit(&e)
				got, err := Fingerprint([]Entry{e})
				if err != nil {
					t.Fatal(err)
				}
				if got == baseFP {
					t.Fatalf("changing %s did not move the fingerprint", tc.name)
				}
			})
		}
	})

	t.Run("a Freshness path's mtime/size moves the fingerprint", func(t *testing.T) {
		dir := t.TempDir()
		f := filepath.Join(dir, "fresh")
		writeFile(t, f, "hi")
		e := Entry{Name: "x", Kind: EntrySynth, Version: "v1", Freshness: []string{f}}
		before, err := Fingerprint([]Entry{e})
		if err != nil {
			t.Fatal(err)
		}
		bumpMtime(t, f)
		after, err := Fingerprint([]Entry{e})
		if err != nil {
			t.Fatal(err)
		}
		if before == after {
			t.Fatal("a Freshness mtime bump did not move the fingerprint")
		}
	})

	t.Run("absent Freshness path is stable and transitions on creation", func(t *testing.T) {
		dir := t.TempDir()
		absent := filepath.Join(dir, "later")
		e := Entry{Name: "x", Kind: EntrySynth, Version: "v1", Freshness: []string{absent}}
		gone, err := Fingerprint([]Entry{e})
		if err != nil {
			t.Fatalf("absent Freshness path errored: %v", err)
		}
		writeFile(t, absent, "now")
		present, err := Fingerprint([]Entry{e})
		if err != nil {
			t.Fatal(err)
		}
		if gone == present {
			t.Fatal("absent->present Freshness transition did not move the fingerprint")
		}
	})

	t.Run("non-ENOENT Freshness errno fails loud", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		writeFile(t, file, "x")
		e := Entry{Name: "x", Kind: EntrySynth, Version: "v1", Freshness: []string{filepath.Join(file, "child")}}
		if _, err := Fingerprint([]Entry{e}); err == nil {
			t.Fatal("Fingerprint over an ENOTDIR Freshness path = nil, want a loud error")
		}
	})
}

// synth builds a Freshness-free synth entry for the order-independence assertion.
func synth(name, version string) Entry {
	return Entry{Name: name, Kind: EntrySynth, Version: version}
}

// oldFingerprint reproduces the pre-fix framing (raw NUL field separators, newline
// entry terminators) so the injection cases below can PROVE they collided before the
// length-prefix fix — a self-validating regression, not a trivially-different pair.
func oldFingerprint(entries []Entry) string {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	h := sha256.New()
	for _, e := range sorted {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%t\x00%s\x00%d\n", e.Name, e.Kind, e.Target, e.Private, e.Version, e.Size)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestFingerprintDelimiterInjection pins that a crafted Version can no longer forge
// an entry boundary. Each pair is a two-entry manifest and a ONE-entry manifest whose
// Version injects the second entry's serialized bytes; both collided under the raw
// NUL/newline framing (oldFingerprint proves it) and must now differ.
func TestFingerprintDelimiterInjection(t *testing.T) {
	tests := []struct {
		name string
		real []Entry // the honest, distinct manifest
		fake []Entry // a different manifest that collided with real under old framing
	}{
		{
			name: "synth entry boundary",
			real: []Entry{
				{Name: "a", Kind: EntrySynth, Version: "v1", Size: 1},
				{Name: "b", Kind: EntrySynth, Version: "v2", Size: 2},
			},
			fake: []Entry{
				{Name: "a", Kind: EntrySynth, Version: "v1\x001\nb\x00synth\x00\x00false\x00v2", Size: 2},
			},
		},
		{
			name: "mixed-kind entry boundary",
			real: []Entry{
				{Name: "m", Kind: EntrySymlink, Target: "/x", Version: "1", Size: 3},
				{Name: "n", Kind: EntryPrivate, Version: "2", Size: 4},
			},
			fake: []Entry{
				{Name: "m", Kind: EntrySymlink, Target: "/x", Version: "1\x003\nn\x00private\x00\x00false\x002", Size: 4},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if oldFingerprint(tc.real) != oldFingerprint(tc.fake) {
				t.Fatalf("test premise broken: the pair does not collide under old framing, so it proves nothing")
			}
			realFP, err := Fingerprint(tc.real)
			if err != nil {
				t.Fatal(err)
			}
			fakeFP, err := Fingerprint(tc.fake)
			if err != nil {
				t.Fatal(err)
			}
			if realFP == fakeFP {
				t.Fatalf("delimiter injection still collides: a %d-entry manifest and a crafted 1-entry manifest share a fingerprint", len(tc.real))
			}
		})
	}
}
