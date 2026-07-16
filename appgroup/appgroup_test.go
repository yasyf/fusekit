package appgroup

import (
	"errors"
	"strings"
	"testing"
)

func TestGroupContainerDir(t *testing.T) {
	injected := errors.New("resolver blew up")
	const group = "group.com.yasyf.cc-pool"

	cases := []struct {
		name      string
		group     string
		resolve   func(string) (string, error)
		wantDir   string
		wantErrIs error
	}{
		{
			name:    "happy path returns the resolved container dir",
			group:   group,
			resolve: func(g string) (string, error) { return "/fake/Group Containers/" + g, nil },
			wantDir: "/fake/Group Containers/" + group,
		},
		{
			name:      "nil container yields ErrNoGroupContainer",
			group:     group,
			resolve:   func(string) (string, error) { return "", ErrNoGroupContainer },
			wantErrIs: ErrNoGroupContainer,
		},
		{
			name:      "error wrapping preserves the group id",
			group:     "group.com.yasyf.absent",
			resolve:   func(string) (string, error) { return "", injected },
			wantErrIs: injected,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := resolveContainer
			resolveContainer = tc.resolve
			t.Cleanup(func() { resolveContainer = restore })

			dir, err := GroupContainerDir(tc.group)

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("GroupContainerDir(%q) err = %v, want errors.Is %v", tc.group, err, tc.wantErrIs)
				}
				if !strings.Contains(err.Error(), tc.group) {
					t.Fatalf("GroupContainerDir(%q) err = %q, want the group id in the message", tc.group, err)
				}
				if dir != "" {
					t.Fatalf("GroupContainerDir(%q) dir = %q, want empty on error", tc.group, dir)
				}
				return
			}

			if err != nil {
				t.Fatalf("GroupContainerDir(%q) unexpected err = %v", tc.group, err)
			}
			if dir != tc.wantDir {
				t.Fatalf("GroupContainerDir(%q) dir = %q, want %q", tc.group, dir, tc.wantDir)
			}
		})
	}
}
