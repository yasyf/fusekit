package transportproto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// BuildFor derives the exact transport identity from every constituent schema.
func BuildFor(version uint16, catalog, mount string) string {
	raw := fmt.Sprintf("version:%d\ncatalog:%s\nmount:%s\n", version, catalog, mount)
	digest := sha256.Sum256([]byte(raw))
	return "fusekit.transport." + hex.EncodeToString(digest[:])
}
