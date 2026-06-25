package mountd

// Where the fusekit-holder cask installs; consumers set Spawn.ExecPath to HolderExe.
const (
	HolderCask = "yasyf/tap/fusekit-holder"
	HolderApp  = "/Applications/fusekit-holder.app"
	HolderExe  = HolderApp + "/Contents/MacOS/fusekit-holder"
)
