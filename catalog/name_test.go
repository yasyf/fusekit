package catalog

import (
	"errors"
	"strings"
	"testing"
)

func TestNameHasExactPortableUTF8Bound(t *testing.T) {
	if err := validateName(strings.Repeat("a", MaxNameBytes)); err != nil {
		t.Fatalf("validateName(exact max): %v", err)
	}
	for name, value := range map[string]string{
		"over max":     strings.Repeat("a", MaxNameBytes+1),
		"invalid utf8": string([]byte{0xff}),
		"control":      "bad\u0001name",
		"slash":        "bad/name",
		"NUL":          "bad\x00name",
		"dot":          ".",
		"dot dot":      "..",
	} {
		if err := validateName(value); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("validateName(%s) = %v, want ErrInvalidObject", name, err)
		}
	}
}
