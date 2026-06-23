package version

import "testing"

func TestString(t *testing.T) {
	cases := []struct {
		version string
		commit  string
		want    string
	}{
		{version: "v1.2.3", commit: "", want: "v1.2.3"},                  // no commit: bare version
		{version: "v1.2.3", commit: "abc1234", want: "v1.2.3 (abc1234)"}, // commit appended in parens
	}
	for _, tc := range cases {
		Version, Commit = tc.version, tc.commit
		if got := String(); got != tc.want {
			t.Errorf("String(Version=%q, Commit=%q) = %q, want %q", tc.version, tc.commit, got, tc.want)
		}
	}
	Version, Commit = "dev", "" // restore the package defaults
}
