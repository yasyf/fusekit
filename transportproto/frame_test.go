package transportproto

import (
	"testing"

	"github.com/yasyf/daemonkit"
)

func TestFrameForPayloadCarriesTheWholePayload(t *testing.T) {
	tests := []struct {
		name    string
		payload daemonkit.Bytes
	}{
		{"spawned source observer", 2 << 20},
		{"spawned source task", 2 << 20},
		{"spawned source driver", 2 << 20},
		{"catalog worker child", 8 << 20},
		{"catalog mutation apply", 4 << 20},
		{"one byte", 1},
		{"envelope reserve", frameEnvelopeReserve},
		{"odd remainder", 3<<20 + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := FrameForPayload(tt.payload)
			if detail := daemonkit.MaxDetail(frame); detail < tt.payload {
				t.Fatalf("MaxDetail(%d) = %d, want >= %d", frame, detail, tt.payload)
			}
		})
	}
}

func TestFrameForPayloadIsTheSmallestFrameThatFits(t *testing.T) {
	for payload := daemonkit.Bytes(1); payload <= 1<<12; payload++ {
		frame := FrameForPayload(payload)
		if daemonkit.MaxDetail(frame) < payload {
			t.Fatalf("payload %d: MaxDetail(%d) = %d", payload, frame, daemonkit.MaxDetail(frame))
		}
		if daemonkit.MaxDetail(frame-1) >= payload {
			t.Fatalf("payload %d: frame %d is not minimal", payload, frame)
		}
	}
}

func TestFrameForPayloadDefersToDaemonkitDefault(t *testing.T) {
	if frame := FrameForPayload(0); frame != 0 {
		t.Fatalf("FrameForPayload(0) = %d, want 0", frame)
	}
}
