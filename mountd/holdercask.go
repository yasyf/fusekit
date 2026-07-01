package mountd

import (
	"os"
	"path/filepath"
)

// Where the fusekit-holder cask installs; consumers set Spawn.ExecPath to HolderExe.
const (
	HolderCask = "yasyf/tap/fusekit-holder"
	HolderApp  = "/Applications/fusekit-holder.app"
	HolderExe  = HolderApp + "/Contents/MacOS/fusekit-holder"
)

// DefaultHolderSocket is the shared socket the cask-launched holder binds and every consumer connects to.
func DefaultHolderSocket() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".fusekit", "holder.sock")
}
