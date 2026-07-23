package transportproto

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// WireBuildFor derives the exact transport identity from every constituent schema.
func WireBuildFor(version uint16, catalog, catalogWorker, mount, sourceDriver string) string {
	raw := fmt.Sprintf(
		"version:%d\ncatalog:%s\ncatalog_worker:%s\nmount:%s\nsource_driver:%s\n",
		version, catalog, catalogWorker, mount, sourceDriver,
	)
	digest := sha256.Sum256([]byte(raw))
	return "com.yasyf.fusekit.transport/" + hex.EncodeToString(digest[:]) + "/v1"
}
