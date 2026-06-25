package mountd

// Where the dedicated, Developer-ID-signed fusekit-holder cask installs. Consumers
// set Spawn.ExecPath to HolderExe so every consumer drives one shared holder (one
// TCC grant) instead of self-exec'ing their own binary.
const (
	// HolderCask installs the bundle: `brew install --cask <HolderCask>`.
	HolderCask = "yasyf/tap/fusekit-holder"
	// HolderApp is the installed app bundle path.
	HolderApp = "/Applications/fusekit-holder.app"
	// HolderExe is the holder executable — a consumer's Spawn.ExecPath value.
	HolderExe = HolderApp + "/Contents/MacOS/fusekit-holder"
)
