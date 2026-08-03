package catalog

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/yasyf/fusekit/internal/recoveryid"
)

const (
	auditTokenLength           = 32
	processGenerationTextChars = 32
)

var (
	_ encoding.TextMarshaler   = ProcessGeneration{}
	_ encoding.TextUnmarshaler = (*ProcessGeneration)(nil)
)

// ProcessGeneration is one exact process-owner generation.
type ProcessGeneration [16]byte

// String returns the exact lowercase hexadecimal wire representation.
func (g ProcessGeneration) String() string { return hex.EncodeToString(g[:]) }

// MarshalText encodes the exact nonzero v1 scalar representation.
func (g ProcessGeneration) MarshalText() ([]byte, error) {
	if g == (ProcessGeneration{}) {
		return nil, fmt.Errorf("%w: process generation is zero", ErrInvalidObject)
	}
	return []byte(g.String()), nil
}

// UnmarshalText replaces g only after strict validation.
func (g *ProcessGeneration) UnmarshalText(text []byte) error {
	if len(text) != processGenerationTextChars {
		return fmt.Errorf(
			"%w: process generation must be 32 lowercase hexadecimal bytes", ErrInvalidObject,
		)
	}
	var parsed ProcessGeneration
	if _, err := hex.Decode(parsed[:], text); err != nil ||
		hex.EncodeToString(parsed[:]) != string(text) {
		return fmt.Errorf(
			"%w: process generation must be 32 lowercase hexadecimal bytes", ErrInvalidObject,
		)
	}
	if parsed == (ProcessGeneration{}) {
		return fmt.Errorf("%w: process generation is zero", ErrInvalidObject)
	}
	*g = parsed
	return nil
}

// AuditToken is Darwin's stable (pid, pidversion) process execution identity.
type AuditToken [32]byte

// UnmarshalJSON decodes one exact JSON byte array.
func (t *AuditToken) UnmarshalJSON(data []byte) error {
	var elements []json.RawMessage
	if err := json.Unmarshal(data, &elements); err != nil {
		return fmt.Errorf("%w: decode audit token: %w", ErrInvalidObject, err)
	}
	if len(elements) != auditTokenLength {
		return fmt.Errorf(
			"%w: audit token has %d elements, want %d", ErrInvalidObject, len(elements), auditTokenLength,
		)
	}
	var token AuditToken
	for index, element := range elements {
		if bytes.Equal(bytes.TrimSpace(element), []byte("null")) {
			return fmt.Errorf("%w: audit token element %d is null", ErrInvalidObject, index)
		}
		if err := json.Unmarshal(element, &token[index]); err != nil {
			return fmt.Errorf("%w: audit token element %d: %w", ErrInvalidObject, index, err)
		}
	}
	*t = token
	return nil
}

// PID returns the process ID embedded in the audit token.
func (t AuditToken) PID() int {
	return int(binary.NativeEndian.Uint32(t[20:24]))
}

// PIDVersion returns the kernel execution version embedded in the audit token.
func (t AuditToken) PIDVersion() uint32 {
	return binary.NativeEndian.Uint32(t[28:32])
}

// Valid reports whether the token carries a usable process execution identity.
func (t AuditToken) Valid() bool {
	return t.PID() > 0 && t.PIDVersion() != 0
}

// ProcessRecord is the exact durable identity of one FuseKit-spawned process.
type ProcessRecord struct {
	// RecoveryID names the consumer barrier that must settle before the
	// retirement receipt can be acknowledged.
	RecoveryID recoveryid.ID `json:"recovery_id"`
	// PID is the spawned child's process id.
	PID int `json:"pid"`
	// StartTime is the prober's opaque, platform-native process start stamp.
	StartTime string `json:"start_time"`
	// Boot is the kernel boot session in which StartTime was captured.
	Boot string `json:"boot"`
	// Comm is the child's initial OS-reported (truncated) process name.
	Comm string `json:"comm"`
	// Executable is the exact kernel-resolved path, bound to AuditToken.
	Executable string `json:"executable"`
	// AuditToken is Darwin's stable kill authority for a protected peer.
	// Spawned disposable workers use the zero value.
	AuditToken AuditToken `json:"audit_token"`
	// Generation tags the daemon instance that spawned the child.
	Generation ProcessGeneration `json:"generation"`
	// ProcessGroup means PID is also the process-group id and signals target the
	// entire group after its dedicated session membership is revalidated.
	ProcessGroup bool `json:"process_group"`
	// SessionID is the dedicated session created with a process-group leader.
	// It remains the group's durable kernel identity after the leader exits.
	SessionID int `json:"session_id"`
}

// Validate rejects an incomplete durable process identity.
func (r ProcessRecord) Validate() error {
	if err := r.RecoveryID.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidObject, err)
	}
	if r.PID <= 0 {
		return fmt.Errorf("%w: process record pid is required", ErrInvalidObject)
	}
	if r.StartTime == "" {
		return fmt.Errorf("%w: process record start time is required", ErrInvalidObject)
	}
	if r.Boot == "" {
		return fmt.Errorf("%w: process record boot is required", ErrInvalidObject)
	}
	if r.AuditToken != (AuditToken{}) {
		if !r.AuditToken.Valid() {
			return fmt.Errorf("%w: process record audit token is unusable", ErrInvalidObject)
		}
		if r.AuditToken.PID() != r.PID {
			return fmt.Errorf(
				"%w: audit-token pid %d is not process pid %d", ErrInvalidObject, r.AuditToken.PID(), r.PID,
			)
		}
		if r.Executable == "" {
			return fmt.Errorf("%w: audit-token process record requires an executable", ErrInvalidObject)
		}
	}
	if r.Generation == (ProcessGeneration{}) {
		return fmt.Errorf("%w: process record generation is required", ErrInvalidObject)
	}
	if r.ProcessGroup {
		if r.PID <= 1 || r.SessionID != r.PID {
			return fmt.Errorf(
				"%w: process group requires a dedicated session leader", ErrInvalidObject,
			)
		}
	} else if r.SessionID != 0 {
		return fmt.Errorf("%w: non-group process record has a session id", ErrInvalidObject)
	}
	return nil
}
