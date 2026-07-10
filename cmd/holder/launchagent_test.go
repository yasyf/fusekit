package main

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/mountd"
)

type fakeKeepAlive struct {
	installs, uninstalls int
	err                  error
}

func (f *fakeKeepAlive) Install() error   { f.installs++; return f.err }
func (f *fakeKeepAlive) Uninstall() error { f.uninstalls++; return f.err }

func TestHolderKeepAliveTargetsCaskBundle(t *testing.T) {
	k := holderKeepAlive()
	if k.Label != "com.yasyf.fusekit-holder" {
		t.Errorf("Label = %q, want %q", k.Label, "com.yasyf.fusekit-holder")
	}
	if k.AppPath != mountd.HolderApp {
		t.Errorf("AppPath = %q, want mountd.HolderApp %q", k.AppPath, mountd.HolderApp)
	}
}

func TestLaunchAgentRun(t *testing.T) {
	boom := errors.New("boom")
	cases := []struct {
		name               string
		install, uninstall bool
		err                error
		wantHandled        bool
		wantInstalls       int
		wantUninstalls     int
	}{
		{name: "neither flag serves", wantHandled: false},
		{name: "install", install: true, wantHandled: true, wantInstalls: 1},
		{name: "uninstall", uninstall: true, wantHandled: true, wantUninstalls: 1},
		{name: "install error", install: true, err: boom, wantHandled: true, wantInstalls: 1},
		{name: "uninstall error", uninstall: true, err: boom, wantHandled: true, wantUninstalls: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeKeepAlive{err: tc.err}
			handled, err := launchAgentRun(tc.install, tc.uninstall, f)
			if handled != tc.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, tc.wantHandled)
			}
			if f.installs != tc.wantInstalls || f.uninstalls != tc.wantUninstalls {
				t.Fatalf("installs/uninstalls = %d/%d, want %d/%d",
					f.installs, f.uninstalls, tc.wantInstalls, tc.wantUninstalls)
			}
			if tc.err == nil {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want wrapped %v", err, tc.err)
			}
		})
	}
}
