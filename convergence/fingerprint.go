package convergence

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

func initialAffectedDigest() [32]byte {
	return sha256.Sum256([]byte("fusekit-convergence-affected-v1\x00"))
}

func appendAffectedDigest(current [32]byte, key LogicalKey) [32]byte {
	digest := sha256.New()
	_, _ = digest.Write(current[:])
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write([]byte(key))
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func summarizeAffected(keys []LogicalKey) (uint64, [32]byte, error) {
	digest := initialAffectedDigest()
	for index, key := range keys {
		if key == "" || (index > 0 && keys[index-1] >= key) {
			return 0, [32]byte{}, fmt.Errorf("%w: affected keys are not sorted and unique", ErrInvalidChange)
		}
		digest = appendAffectedDigest(digest, key)
	}
	return uint64(len(keys)), digest, nil
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
