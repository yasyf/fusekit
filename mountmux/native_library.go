package mountmux

import (
	"errors"
	"path/filepath"
)

func bundledNativeLibrary(executable string) (string, error) {
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", errors.New("native executable path is not exact and absolute")
	}
	macOS := filepath.Dir(executable)
	contents := filepath.Dir(macOS)
	application := filepath.Dir(contents)
	if filepath.Base(macOS) != "MacOS" || filepath.Base(contents) != "Contents" || filepath.Ext(application) != ".app" {
		return "", errors.New("native executable is not inside a fixed application bundle")
	}
	return filepath.Join(application, "Contents", "Frameworks", "libfuse-t.dylib"), nil
}
