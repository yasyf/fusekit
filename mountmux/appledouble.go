package mountmux

import "strings"

func appleDouble(name string) bool { return strings.HasPrefix(name, "._") }
