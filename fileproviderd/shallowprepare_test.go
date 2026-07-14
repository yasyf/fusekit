package fileproviderd

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestAppClientProbeDomainShallow pins ProbeDomainShallow's happy paths: a new app
// that honors the shallow flag returns its listed verdict, and an OLD app that
// ignores the flag and answers a DEEP probe (Listed absent) has its verdict derived
// from the deep JSONBytes shape — the designed skew.
func TestAppClientProbeDomainShallow(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(a *fakeApp)
		wantListed bool
	}{
		{
			name: "new app honors shallow: listed true",
			setup: func(a *fakeApp) {
				a.setProbeShallow(func(string) Response { return Response{OK: true, Listed: boolptr(true)} })
			},
			wantListed: true,
		},
		{
			name: "new app honors shallow: listed false",
			setup: func(a *fakeApp) {
				a.setProbeShallow(func(string) Response { return Response{OK: true, Listed: boolptr(false)} })
			},
			wantListed: false,
		},
		{
			name: "old app deep-answers a shallow request: JSONBytes present -> listed",
			setup: func(a *fakeApp) {
				a.setProbe(func(string) Response { return Response{OK: true, JSONBytes: int64ptr(7)} })
			},
			wantListed: true,
		},
		{
			name:       "old app deep-answers a shallow request: JSONBytes absent -> not listed",
			setup:      func(a *fakeApp) { a.setProbe(func(string) Response { return Response{OK: true} }) },
			wantListed: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			tc.setup(a)
			listed, err := NewAppClient(a.socket).ProbeDomainShallow(context.Background(), "acct-01")
			if err != nil {
				t.Fatalf("ProbeDomainShallow = %v, want nil", err)
			}
			if listed != tc.wantListed {
				t.Errorf("listed = %v, want %v", listed, tc.wantListed)
			}
			seen := a.seen()
			if len(seen) != 1 {
				t.Fatalf("fake app saw %d requests, want 1: %+v", len(seen), seen)
			}
			want := Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-01", Shallow: true}
			if seen[0] != want {
				t.Errorf("request = %+v, want %+v (shallow flag on the frozen wire)", seen[0], want)
			}
		})
	}
}

// TestAppClientProbeDomainShallowErrorClasses pins the failure mapping: it matches
// ProbeDomain exactly, and an app so old it predates probe-domain ENTIRELY (its
// unknown-op default arm: ok:false, EMPTY err_class) becomes ErrOpUnsupported,
// distinct from both the transient blip and the retreat.
func TestAppClientProbeDomainShallowErrorClasses(t *testing.T) {
	tests := []struct {
		name   string
		script *Response // nil: leave probe-domain unscripted so the unknown-op arm answers
		wantIs error
	}{
		{name: "unregistered domain is ErrNoDomain", script: &Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"}, wantIs: ErrNoDomain},
		{name: "not-serving is ErrDomainNotServing", script: &Response{OK: false, ErrClass: ClassDomainNotServing, Error: "materializing"}, wantIs: ErrDomainNotServing},
		{name: "busy is ErrBusy", script: &Response{OK: false, ErrClass: ClassBusy, Error: "inflight"}, wantIs: ErrBusy},
		{name: "ancient app unknown-op arm is ErrOpUnsupported", script: nil, wantIs: ErrOpUnsupported},
		{name: "unknown class stays transient, never retreat", script: &Response{OK: false, ErrClass: "future", Error: "?"}, wantIs: ErrAppUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			if tc.script != nil {
				resp := *tc.script
				a.setProbeShallow(func(string) Response { return resp })
			}
			listed, err := NewAppClient(a.socket).ProbeDomainShallow(context.Background(), "acct-01")
			if listed {
				t.Errorf("listed = true, want false on a failure")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if errors.Is(err, ErrCannotControl) {
				t.Errorf("err = %v, want NEVER the retreat condition", err)
			}
		})
	}
}

// TestAppClientPrepareDomain pins PrepareDomain's success and failure mapping,
// including that a materialization timeout or a not-enumerated item both surface as
// ErrDomainNotServing, and an app too old to know the op becomes ErrOpUnsupported.
func TestAppClientPrepareDomain(t *testing.T) {
	tests := []struct {
		name    string
		script  *Response // nil: leave prepare-domain unscripted so the unknown-op arm answers
		wantErr bool
		wantIs  error
	}{
		{name: "completed materialization is nil", script: &Response{OK: true}},
		{name: "not-enumerated item is ErrDomainNotServing", script: &Response{OK: false, ErrClass: ClassDomainNotServing, Error: "settings.json not enumerated"}, wantErr: true, wantIs: ErrDomainNotServing},
		{name: "download timeout is ErrDomainNotServing", script: &Response{OK: false, ErrClass: ClassDomainNotServing, Error: "download timed out"}, wantErr: true, wantIs: ErrDomainNotServing},
		{name: "busy is ErrBusy", script: &Response{OK: false, ErrClass: ClassBusy, Error: "inflight"}, wantErr: true, wantIs: ErrBusy},
		{name: "old app unknown-op arm is ErrOpUnsupported", script: nil, wantErr: true, wantIs: ErrOpUnsupported},
		{name: "unknown class stays transient, never retreat", script: &Response{OK: false, ErrClass: "future", Error: "?"}, wantErr: true, wantIs: ErrAppUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			if tc.script != nil {
				resp := *tc.script
				a.setPrepare(func(string) Response { return resp })
			}
			err := NewAppClient(a.socket).PrepareDomain(context.Background(), "acct-01", 30*time.Second)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("PrepareDomain = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantIs)
			}
			if errors.Is(err, ErrCannotControl) {
				t.Errorf("err = %v, want NEVER the retreat condition", err)
			}
		})
	}
}

// TestAppClientPrepareDomainSendsDeadline pins the deadline_ms wire: a positive
// deadline crosses in milliseconds, a zero deadline omits the field so the app
// applies its own default.
func TestAppClientPrepareDomainSendsDeadline(t *testing.T) {
	tests := []struct {
		name     string
		deadline time.Duration
		wantMS   int64
	}{
		{name: "positive deadline sends milliseconds", deadline: 12 * time.Second, wantMS: 12000},
		{name: "sub-millisecond positive deadline floors to 1", deadline: 500 * time.Microsecond, wantMS: 1},
		{name: "zero deadline omits deadline_ms (app default)", deadline: 0, wantMS: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := startFakeApp(t)
			a.setPrepare(func(string) Response { return Response{OK: true} })
			if err := NewAppClient(a.socket).PrepareDomain(context.Background(), "acct-01", tc.deadline); err != nil {
				t.Fatalf("PrepareDomain = %v, want nil", err)
			}
			seen := a.seen()
			if len(seen) != 1 {
				t.Fatalf("fake app saw %d requests, want 1", len(seen))
			}
			want := Request{Proto: 1, Op: OpPrepareDomain, Domain: "acct-01", DeadlineMS: tc.wantMS}
			if seen[0] != want {
				t.Errorf("request = %+v, want %+v", seen[0], want)
			}
		})
	}
}
