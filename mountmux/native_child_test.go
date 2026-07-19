package mountmux

import (
	"encoding/base64"
	"testing"
)

func TestNativeChildArgumentsRoundTripExactContract(t *testing.T) {
	want := NativeChildConfig{
		Socket:        "/tmp/fusekit-runtime/socket",
		Root:          "/Volumes/FuseKit",
		Library:       "/Applications/FuseKit.app/Contents/Frameworks/libfuse-t.dylib",
		LibrarySHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Options:       []string{"-ovolname=FuseKit", "-oallow_other"},
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
	unknown := base64.RawURLEncoding.EncodeToString([]byte(`{"protocol":1,"config":{"socket":"/tmp/s","root":"/Volumes/FuseKit","library":"/Applications/FuseKit.app/Contents/Frameworks/libfuse-t.dylib","library_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},"legacy":true}`))
	if _, recognized, err := ParseNativeChildArguments([]string{nativeChildMode, unknown}); err == nil || !recognized {
		t.Fatalf("unknown contract = recognized %t, %v", recognized, err)
	}
	wrongEpoch := base64.RawURLEncoding.EncodeToString([]byte(`{"protocol":2,"config":{"socket":"/tmp/s","root":"/Volumes/FuseKit","library":"/Applications/FuseKit.app/Contents/Frameworks/libfuse-t.dylib","library_sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}}`))
	if _, recognized, err := ParseNativeChildArguments([]string{nativeChildMode, wrongEpoch}); err == nil || !recognized {
		t.Fatalf("wrong protocol epoch = recognized %t, %v", recognized, err)
	}
	valid := NativeChildConfig{Socket: "/tmp/s", Root: "/Volumes/FuseKit", Library: "/Applications/FuseKit.app/Contents/Frameworks/libfuse-t.dylib", LibrarySHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}
	badRoot := valid
	badRoot.Root = "/Volumes/FuseKit/../Other"
	if _, err := NativeChildArguments(badRoot); err == nil {
		t.Fatal("non-canonical root encoded")
	}
	badSocket := valid
	badSocket.Socket = "holder.sock"
	if _, err := NativeChildArguments(badSocket); err == nil {
		t.Fatal("relative socket encoded")
	}
	badOption := valid
	badOption.Options = []string{"bad\x00option"}
	if _, err := NativeChildArguments(badOption); err == nil {
		t.Fatal("NUL mount option encoded")
	}
	badLibrary := valid
	badLibrary.Library = "/Applications/FuseKit.app/Contents/Frameworks/../libfuse-t.dylib"
	if _, err := NativeChildArguments(badLibrary); err == nil {
		t.Fatal("external library path encoded")
	}
	badDigest := valid
	badDigest.LibrarySHA256 = "not-a-digest"
	if _, err := NativeChildArguments(badDigest); err == nil {
		t.Fatal("invalid library digest encoded")
	}
}

func TestBundledNativeLibraryIsExactFixedAppLeaf(t *testing.T) {
	want := "/Applications/Example.app/Contents/Frameworks/libfuse-t.dylib"
	got, err := bundledNativeLibrary("/Applications/Example.app/Contents/MacOS/Example")
	if err != nil || got != want {
		t.Fatalf("bundled library = %q, %v; want %q", got, err, want)
	}
	for _, invalid := range []string{
		"/usr/local/bin/Example",
		"/Applications/Example.app/Contents/Helpers/Example",
		"/Applications/Example/Contents/MacOS/Example",
	} {
		if _, err := bundledNativeLibrary(invalid); err == nil {
			t.Fatalf("external executable %q accepted", invalid)
		}
	}
}
