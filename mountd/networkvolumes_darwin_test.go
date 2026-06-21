//go:build darwin

package mountd

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestOpenNetworkVolumesSettings(t *testing.T) {
	tests := []struct {
		name string
		// failUpTo is the number of leading URLs that fail before one succeeds;
		// failAll forces every URL to fail.
		failUpTo int
		failAll  bool
		wantURLs []string
		wantErr  bool
	}{
		{
			name:     "first url succeeds",
			failUpTo: 0,
			wantURLs: networkVolumesSettingsURLs[:1],
			wantErr:  false,
		},
		{
			name:     "first two fail third succeeds",
			failUpTo: 2,
			wantURLs: networkVolumesSettingsURLs,
			wantErr:  false,
		},
		{
			name:     "all fail",
			failAll:  true,
			wantURLs: networkVolumesSettingsURLs,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := openRunner
			t.Cleanup(func() { openRunner = orig })

			var attempted []string
			boom := errors.New("boom")
			openRunner = func(_ context.Context, url string) error {
				attempted = append(attempted, url)
				if tt.failAll || len(attempted) <= tt.failUpTo {
					return boom
				}
				return nil
			}

			err := OpenNetworkVolumesSettings(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("OpenNetworkVolumesSettings() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, boom) {
				t.Fatalf("OpenNetworkVolumesSettings() error = %v, want wrapped %v", err, boom)
			}
			if !reflect.DeepEqual(attempted, tt.wantURLs) {
				t.Fatalf("attempted URLs = %v, want %v", attempted, tt.wantURLs)
			}
		})
	}
}
