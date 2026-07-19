package convergence

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
)

// EffectiveFingerprint hashes named effective values independent of resolver ordering.
func EffectiveFingerprint(values []EffectiveValue) (Fingerprint, error) {
	ordered := append([]EffectiveValue(nil), values...)
	slices.SortFunc(ordered, func(a, b EffectiveValue) int { return compareString(string(a.Key), string(b.Key)) })
	digest := sha256.New()
	digest.Write([]byte("fusekit-convergence-v1\x00"))
	var length [8]byte
	for index, value := range ordered {
		if value.Key == "" {
			return Fingerprint{}, fmt.Errorf("%w: empty effective value name", ErrInvalidResolution)
		}
		if index > 0 && ordered[index-1].Key == value.Key {
			return Fingerprint{}, fmt.Errorf("%w: duplicate effective value %q", ErrInvalidResolution, value.Key)
		}
		binary.BigEndian.PutUint64(length[:], uint64(len(value.Key)))
		digest.Write(length[:])
		digest.Write([]byte(value.Key))
		binary.BigEndian.PutUint64(length[:], uint64(len(value.Bytes)))
		digest.Write(length[:])
		digest.Write(value.Bytes)
	}
	var fingerprint Fingerprint
	copy(fingerprint[:], digest.Sum(nil))
	return fingerprint, nil
}

func compareString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
