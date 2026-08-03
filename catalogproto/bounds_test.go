package catalogproto

import (
	"bytes"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/transportproto"
)

// The successor lanes are chunked unary calls, so every chunk this protocol
// permits has to survive one frame of the session that carries it. daemonkit
// base64s a terminal and reserves its envelope, so MaxFrame is never the
// ceiling — MaxDetail is, and a bound declared against MaxFrame fails open.
func TestChunkBoundsFitOneDeclaredSessionFrame(t *testing.T) {
	t.Parallel()
	detail := int(daemonkit.MaxDetail(transportproto.FrameForPayload(daemonkit.Bytes(MaxSessionPayloadBytes))))
	tests := []struct {
		name  string
		value any
	}{
		{"read chunk", ReadResponse{
			Protocol: Version, Code: ErrorCodeOk,
			Data: bytes.Repeat([]byte{'x'}, int(MaxReadChunkBytes)), EOF: true,
		}},
		{"mutation chunk", MutationChunkRequest{
			Protocol: Version, RequestID: requestOne, Sequence: 1,
			Payload: bytes.Repeat([]byte{'x'}, int(MaxMutationChunkBytes)),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := Encode(tt.value)
			if err != nil {
				t.Fatalf("Encode(exact %s): %v", tt.name, err)
			}
			if len(encoded) > detail {
				t.Fatalf("encoded %s = %d bytes, want at most MaxDetail %d", tt.name, len(encoded), detail)
			}
		})
	}
}

func TestChunkBoundsRefuseAnOverlongPayload(t *testing.T) {
	t.Parallel()
	read := ReadResponse{
		Protocol: Version, Code: ErrorCodeOk,
		Data: bytes.Repeat([]byte{'x'}, int(MaxReadChunkBytes)+1),
	}
	if err := Validate(read); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong read chunk) = %v, want ErrInvalidMessage", err)
	}
	chunk := MutationChunkRequest{
		Protocol: Version, RequestID: requestOne, Sequence: 1,
		Payload: bytes.Repeat([]byte{'x'}, int(MaxMutationChunkBytes)+1),
	}
	if err := Validate(chunk); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Validate(overlong mutation chunk) = %v, want ErrInvalidMessage", err)
	}
}
