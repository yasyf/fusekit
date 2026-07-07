//go:build darwin

package overlay

import "testing"

// TestPluginkitElected pins the multi-line election parse: pluginkit lists every
// registered copy (stale Trash duplicates included), so an enabled '+' flag on
// any line wins and a leading disabled duplicate cannot mask a live election.
func TestPluginkitElected(t *testing.T) {
	cases := map[string]struct {
		out  string
		want bool
	}{
		"not registered (empty output)":       {"", false},
		"whitespace only":                     {"  \n\t\n", false},
		"single enabled":                      {"+    com.example.fp(1.0)\tUUID\t/Applications/App.app/...\n", true},
		"single disabled":                     {"-    com.example.fp(1.0)\tUUID\t/Applications/App.app/...\n", false},
		"single problem flag":                 {"!    com.example.fp(1.0)\tUUID\t/Applications/App.app/...\n", false},
		"stale disabled first, live enabled":  {"-    com.example.fp(0.9)\tUUID1\t/Users/x/.Trash/App.app/...\n+    com.example.fp(1.0)\tUUID2\t/Applications/App.app/...\n", true},
		"live enabled first, stale disabled":  {"+    com.example.fp(1.0)\tUUID2\t/Applications/App.app/...\n-    com.example.fp(0.9)\tUUID1\t/Users/x/.Trash/App.app/...\n", true},
		"all duplicates disabled":             {"-    com.example.fp(0.9)\tUUID1\t...\n-    com.example.fp(1.0)\tUUID2\t...\n", false},
		"leading-whitespace enabled (indent)": {"  +  com.example.fp(1.0)\tUUID\t...\n", true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := pluginkitElected(tc.out); got != tc.want {
				t.Errorf("pluginkitElected(%q) = %v, want %v", tc.out, got, tc.want)
			}
		})
	}
}
