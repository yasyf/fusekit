package mountd

import (
	"strings"
	"testing"
)

// TestPoll pins Poll's three-way verdict: healthy, degraded, unreachable.
func TestPoll(t *testing.T) {
	const ver = "v1.2.3 (deadbee)"
	healthOK := `{"proto":1,"ok":true,"version":"` + ver + `"}`
	listOK := `{"proto":1,"ok":true,"mounts":[{"dir":"/d","base":"/b","live":true}]}`

	t.Run("healthy returns version and mounts", func(t *testing.T) {
		socket, _ := startRawHolder(t, func(req string) string {
			if strings.Contains(req, `"op":"health"`) {
				return healthOK
			}
			return listOK
		})
		got, err := (&Client{Socket: socket}).Poll()
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		if !got.Reachable || got.Degraded || got.Version != ver || len(got.Mounts) != 1 {
			t.Fatalf("Poll = %+v, want reachable, not degraded, version %q, 1 mount", got, ver)
		}
	})

	t.Run("list failure is degraded with version kept", func(t *testing.T) {
		socket, _ := startRawHolder(t, func(req string) string {
			if strings.Contains(req, `"op":"health"`) {
				return healthOK
			}
			return "" // hang up on List
		})
		got, err := (&Client{Socket: socket}).Poll()
		if err == nil {
			t.Fatal("Poll: want the underlying List error, got nil")
		}
		if !got.Reachable || !got.Degraded || got.Version != ver || got.Mounts != nil {
			t.Fatalf("Poll = %+v, want reachable+degraded, version %q, nil mounts", got, ver)
		}
	})

	t.Run("health failure is unreachable", func(t *testing.T) {
		socket, _ := startRawHolder(t, func(string) string {
			return "" // hang up on Health
		})
		got, err := (&Client{Socket: socket}).Poll()
		if err == nil {
			t.Fatal("Poll: want the underlying Health error, got nil")
		}
		if got.Reachable || got.Degraded || got.Version != "" || got.Mounts != nil {
			t.Fatalf("Poll = %+v, want the zero-value unreachable verdict", got)
		}
	})
}
