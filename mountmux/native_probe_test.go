package mountmux

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestNativeProbeChildArgumentsRoundTripExactContract(t *testing.T) {
	config := NativeProbeChildConfig{Root: "/Volumes/FuseKit", Token: strings.Repeat("a", 64)}
	arguments, err := NativeProbeChildArguments(config)
	if err != nil {
		t.Fatalf("NativeProbeChildArguments: %v", err)
	}
	parsed, recognized, err := ParseNativeProbeChildArguments(arguments)
	if err != nil || !recognized || !reflect.DeepEqual(parsed, config) {
		t.Fatalf("ParseNativeProbeChildArguments = %#v, %t, %v", parsed, recognized, err)
	}
	if _, recognized, err := ParseNativeProbeChildArguments([]string{"consumer-mode"}); err != nil || recognized {
		t.Fatalf("unrelated mode = %t, %v", recognized, err)
	}
	for _, arguments := range [][]string{
		{nativeProbeChildMode},
		{nativeProbeChildMode, "relative", config.Token},
		{nativeProbeChildMode, config.Root, "short"},
		{nativeProbeChildMode, config.Root, strings.Repeat("A", 64)},
	} {
		if _, recognized, err := ParseNativeProbeChildArguments(arguments); err == nil || !recognized {
			t.Fatalf("malformed arguments = %v, recognized=%t err=%v", arguments, recognized, err)
		}
	}
}

func TestRunNativeProbeChildRequiresENOENTAndHonorsPreCancellation(t *testing.T) {
	root := t.TempDir()
	config := NativeProbeChildConfig{Root: root, Token: strings.Repeat("b", 64)}
	if err := RunNativeProbeChild(t.Context(), config); err != nil {
		t.Fatalf("missing probe path: %v", err)
	}
	path, err := nativeProbeFilesystemPath(root, config.Token)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := RunNativeProbeChild(t.Context(), config); err == nil || !strings.Contains(err.Error(), "unexpectedly exists") {
		t.Fatalf("existing probe path = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := RunNativeProbeChild(ctx, config); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled probe = %v", err)
	}
}

func TestNewNativeProbeTokenIsExactAndUnique(t *testing.T) {
	first, err := NewNativeProbeToken()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewNativeProbeToken()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || validateNativeProbeToken(first) != nil || validateNativeProbeToken(second) != nil {
		t.Fatalf("tokens are not unique exact values: %q %q", first, second)
	}
}
