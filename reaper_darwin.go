//go:build darwin

package fusekit

import "github.com/yasyf/fusekit/internal/carcass"

// reapOrphanedServers force-kills any go-nfsv4 child of this process serving
// dir; call ONLY after confirming dir is no longer a mountpoint.
func reapOrphanedServers(dir string) { carcass.ReapOwnChildren(dir) }

// ReapOrphanedServers force-kills orphaned go-nfsv4 servers of ANY generation
// under roots — proven-dead only (an immediately-answered dead errno; a
// hanging stat is never a carcass), re-confirmed pid-reuse-proof at kill
// time, never a live mount's. Returns the PIDs killed. See ccn doc 501ce12.
func ReapOrphanedServers(roots []string) []int { return carcass.ReapOrphaned(roots) }
