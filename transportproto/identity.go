package transportproto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// BuildFor derives the exact transport identity from every constituent schema.
func BuildFor(version uint16, catalog, catalogWorker, mount, sourceDriver string) string {
	raw := fmt.Sprintf(
		"version:%d\ncatalog:%s\ncatalog_worker:%s\nmount:%s\nsource_driver:%s\n",
		version, catalog, catalogWorker, mount, sourceDriver,
	)
	digest := sha256.Sum256([]byte(raw))
	return "fusekit.transport." + hex.EncodeToString(digest[:])
}
