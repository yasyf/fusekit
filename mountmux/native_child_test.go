package mountmux

import (
	"encoding/base64"
	"testing"
)

func TestNativeChildArgumentsRoundTripExactContract(t *testing.T) {
	want := NativeChildConfig{
		Socket:  "/tmp/fusekit-runtime/socket",
		Root:    "/Volumes/FuseKit",
		Options: []string{"-ovolname=FuseKit", "-oallow_other"},
	}
	arguments, err := NativeChildArguments(want)
	if err != nil {
		t.Fatal(err)
	}
	got, recognized, err := ParseNativeChildArguments(arguments)
	if err != nil || !recognized {
		t.Fatalf("ParseNativeChildArguments = %#v, %t, %v", got, recognized, err)
	}
	if got.Socket != want.Socket || got.Root != want.Root || len(got.Options) != len(want.Options) {
		t.Fatalf("native child config = %#v, want %#v", got, want)
	}
	for index := range want.Options {
		if got.Options[index] != want.Options[index] {
			t.Fatalf("option %d = %q, want %q", index, got.Options[index], want.Options[index])
		}
	}
}

func TestNativeChildArgumentsRejectUnknownOrMalformedContracts(t *testing.T) {
	if _, recognized, err := ParseNativeChildArguments([]string{"consumer-mode"}); err != nil || recognized {
		t.Fatalf("consumer arguments = recognized %t, %v", recognized, err)
	}
	unknown := base64.RawURLEncoding.EncodeToString([]byte(`{"protocol":1,"config":{"socket":"/tmp/s","root":"/Volumes/FuseKit"},"legacy":true}`))
	if _, recognized, err := ParseNativeChildArguments([]string{nativeChildMode, unknown}); err == nil || !recognized {
		t.Fatalf("unknown contract = recognized %t, %v", recognized, err)
	}
	old := base64.RawURLEncoding.EncodeToString([]byte(`{"protocol":0,"config":{"socket":"/tmp/s","root":"/Volumes/FuseKit"}}`))
	if _, recognized, err := ParseNativeChildArguments([]string{nativeChildMode, old}); err == nil || !recognized {
		t.Fatalf("old protocol = recognized %t, %v", recognized, err)
	}
	if _, err := NativeChildArguments(NativeChildConfig{Socket: "/tmp/s", Root: "/Volumes/FuseKit/../Other"}); err == nil {
		t.Fatal("non-canonical root encoded")
	}
	if _, err := NativeChildArguments(NativeChildConfig{Socket: "holder.sock", Root: "/Volumes/FuseKit"}); err == nil {
		t.Fatal("relative socket encoded")
	}
	if _, err := NativeChildArguments(NativeChildConfig{Socket: "/tmp/s", Root: "/Volumes/FuseKit", Options: []string{"bad\x00option"}}); err == nil {
		t.Fatal("NUL mount option encoded")
	}
}
