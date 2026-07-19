package mountmux

import "testing"

func TestAppleDoublePolicy(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		want bool
	}{
		{name: "._settings.json", want: true},
		{name: "._", want: true},
		{name: ".settings.json", want: false},
		{name: "settings.json", want: false},
		{name: "prefix._settings.json", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := appleDouble(test.name); got != test.want {
				t.Fatalf("appleDouble(%q) = %t, want %t", test.name, got, test.want)
			}
		})
	}
}
