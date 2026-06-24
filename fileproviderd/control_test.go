package fileproviderd

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// These tests pin the proto-1 control wire byte-for-byte: JSON field names, op
// strings, and error-class strings are FROZEN (see the package godoc). The
// signed companion app and the Go client ship and skew on different cadences, so
// ANY change that makes a golden literal here fail is a protocol break — do not
// "fix" the literal; fix the change. New capability means a new op or a new
// optional field with a new name, which extends these tests without altering an
// existing golden.

func TestWireFreezeControlProtoVersion(t *testing.T) {
	if ControlProtoVersion != 1 {
		t.Fatalf("ControlProtoVersion = %d; proto-1 is frozen — new capability never bumps it", ControlProtoVersion)
	}
}

func TestWireFreezeOps(t *testing.T) {
	want := map[string]Op{
		"health":   OpHealth,
		"probe":    OpProbe,
		"register": OpRegister,
		"path":     OpPath,
		"signal":   OpSignal,
		"remove":   OpRemove,
	}
	for frozen, op := range want {
		if string(op) != frozen {
			t.Errorf("op drifted: %q, frozen value is %q", op, frozen)
		}
	}
}

func TestWireFreezeErrClasses(t *testing.T) {
	want := map[string]string{
		"no-entitlement":  ClassNoEntitlement,
		"app-unreachable": ClassAppUnreachable,
		"register-failed": ClassRegisterFailed,
		"no-domain":       ClassNoDomain,
		"busy":            ClassBusy,
	}
	for frozen, class := range want {
		if class != frozen {
			t.Errorf("error class drifted: %q, frozen value is %q", class, frozen)
		}
	}
}

func TestWireFreezeRequest(t *testing.T) {
	tests := []struct {
		name string
		in   Request
		want string
	}{
		{
			name: "domain op",
			in:   Request{Proto: 1, Op: OpRegister, Domain: "acct-01"},
			want: `{"proto":1,"op":"register","domain":"acct-01"}`,
		},
		{
			name: "domainless op omits domain",
			in:   Request{Proto: 1, Op: OpHealth},
			want: `{"proto":1,"op":"health"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact)", got, tc.want)
			}
			var back Request
			if err := json.Unmarshal([]byte(tc.want), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Fatalf("round-trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}

func TestWireFreezeResponse(t *testing.T) {
	tests := []struct {
		name string
		in   Response
		want string
	}{
		{
			name: "every field set",
			in: Response{
				Proto:    1,
				OK:       false,
				Error:    "boom",
				ErrClass: ClassNoEntitlement,
				Version:  "1.2.3 (abc1234)",
				FPOK:     true,
				Path:     "/Users/u/Library/CloudStorage/App-acct-01",
			},
			want: `{"proto":1,"ok":false,"error":"boom","err_class":"no-entitlement","version":"1.2.3 (abc1234)","fp_ok":true,"path":"/Users/u/Library/CloudStorage/App-acct-01"}`,
		},
		{
			name: "optional fields omitted when empty",
			in:   Response{Proto: 1, OK: true},
			want: `{"proto":1,"ok":true}`,
		},
		{
			name: "register reply carries only the path",
			in:   Response{Proto: 1, OK: true, Path: "/cloud/acct-01"},
			want: `{"proto":1,"ok":true,"path":"/cloud/acct-01"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact)", got, tc.want)
			}
			var back Response
			if err := json.Unmarshal([]byte(tc.want), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Fatalf("round-trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}

// TestClassRoundTrip pins the class<->sentinel mapping in both directions and,
// load-bearing, that an UNKNOWN class maps to the transient ErrAppUnavailable —
// never the permanent ErrCannotControl. A new app class behind an old client
// must fail toward retry, since only ErrCannotControl retreats an account.
func TestClassRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		want     error
		wantBack string // errToClass(want), "" when want has no class (unknown)
	}{
		{name: "no-entitlement -> ErrCannotControl", class: ClassNoEntitlement, want: ErrCannotControl, wantBack: ClassNoEntitlement},
		{name: "app-unreachable -> ErrAppUnavailable", class: ClassAppUnreachable, want: ErrAppUnavailable, wantBack: ClassAppUnreachable},
		{name: "register-failed -> ErrRegisterFailed", class: ClassRegisterFailed, want: ErrRegisterFailed, wantBack: ClassRegisterFailed},
		{name: "no-domain -> ErrNoDomain", class: ClassNoDomain, want: ErrNoDomain, wantBack: ClassNoDomain},
		{name: "busy -> ErrBusy", class: ClassBusy, want: ErrBusy, wantBack: ClassBusy},
		{name: "unknown class fails toward retry (NOT retreat)", class: "future-class", want: ErrAppUnavailable, wantBack: ClassAppUnreachable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classToErr(tc.class)
			if !errors.Is(got, tc.want) {
				t.Fatalf("classToErr(%q) = %v, want errors.Is %v", tc.class, got, tc.want)
			}
			// The unknown class must NEVER read as the retreat condition.
			if tc.class == "future-class" && errors.Is(got, ErrCannotControl) {
				t.Fatalf("unknown class %q mapped to the retreat condition ErrCannotControl", tc.class)
			}
			if back := errToClass(got); back != tc.wantBack {
				t.Errorf("errToClass(%v) = %q, want %q", got, back, tc.wantBack)
			}
		})
	}
}

// TestSentinelsDistinct pins that the transient app-availability condition and
// the permanent retreat condition are NOT errors.Is-confusable in either
// direction — the single most safety-critical invariant of this package.
func TestSentinelsDistinct(t *testing.T) {
	if errors.Is(ErrCannotControl, ErrAppUnavailable) {
		t.Error("ErrCannotControl errors.Is ErrAppUnavailable; the retreat condition must never read as transient")
	}
	if errors.Is(ErrAppUnavailable, ErrCannotControl) {
		t.Error("ErrAppUnavailable errors.Is ErrCannotControl; a transient blip must never read as the retreat")
	}
}

func TestRespErr(t *testing.T) {
	tests := []struct {
		name    string
		resp    Response
		wantNil bool
		wantIs  error
		wantMsg string // substring expected in the error
	}{
		{name: "ok response is no error", resp: Response{OK: true}, wantNil: true},
		{
			name:    "classed error wraps the sentinel and keeps the message",
			resp:    Response{OK: false, ErrClass: ClassNoEntitlement, Error: "enable it in Settings"},
			wantIs:  ErrCannotControl,
			wantMsg: "enable it in Settings",
		},
		{
			name:    "classless error is the bare message",
			resp:    Response{OK: false, Error: "kaboom"},
			wantMsg: "kaboom",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := respErr(&tc.resp)
			if tc.wantNil {
				if err != nil {
					t.Fatalf("respErr = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("respErr = nil, want an error")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("respErr = %v, want errors.Is %v", err, tc.wantIs)
			}
			if tc.wantMsg != "" && !contains(err.Error(), tc.wantMsg) {
				t.Errorf("respErr = %q, want it to contain %q", err.Error(), tc.wantMsg)
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
