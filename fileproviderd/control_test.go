package fileproviderd

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// These tests pin the proto-1 control wire byte-for-byte — JSON field names, op
// strings, and error-class strings are FROZEN (see the package godoc). App and
// client skew on different cadences, so a failing golden is a protocol break: fix
// the change, not the literal. New capability adds a new op or optional field,
// never an altered golden.

func TestWireFreezeControlProtoVersion(t *testing.T) {
	if ControlProtoVersion != 1 {
		t.Fatalf("ControlProtoVersion = %d; proto-1 is frozen — new capability never bumps it", ControlProtoVersion)
	}
}

func TestWireFreezeOps(t *testing.T) {
	want := map[string]Op{
		"health":         OpHealth,
		"probe":          OpProbe,
		"register":       OpRegister,
		"path":           OpPath,
		"signal":         OpSignal,
		"remove":         OpRemove,
		"probe-domain":   OpProbeDomain,
		"prepare-domain": OpPrepareDomain,
	}
	for frozen, op := range want {
		if string(op) != frozen {
			t.Errorf("op drifted: %q, frozen value is %q", op, frozen)
		}
	}
}

func TestWireFreezeErrClasses(t *testing.T) {
	want := map[string]string{
		"no-entitlement":     ClassNoEntitlement,
		"app-unreachable":    ClassAppUnreachable,
		"register-failed":    ClassRegisterFailed,
		"no-domain":          ClassNoDomain,
		"busy":               ClassBusy,
		"domain-not-serving": ClassDomainNotServing,
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
		{
			name: "probe-domain carries the domain",
			in:   Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-01"},
			want: `{"proto":1,"op":"probe-domain","domain":"acct-01"}`,
		},
		{
			name: "shallow probe-domain carries the shallow flag",
			in:   Request{Proto: 1, Op: OpProbeDomain, Domain: "acct-01", Shallow: true},
			want: `{"proto":1,"op":"probe-domain","domain":"acct-01","shallow":true}`,
		},
		{
			name: "prepare-domain carries the deadline",
			in:   Request{Proto: 1, Op: OpPrepareDomain, Domain: "acct-01", DeadlineMS: 30000},
			want: `{"proto":1,"op":"prepare-domain","domain":"acct-01","deadline_ms":30000}`,
		},
		{
			name: "prepare-domain omits a zero deadline (app default)",
			in:   Request{Proto: 1, Op: OpPrepareDomain, Domain: "acct-01"},
			want: `{"proto":1,"op":"prepare-domain","domain":"acct-01"}`,
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
		{
			name: "probe-domain reply carries the .claude.json byte count",
			in:   Response{Proto: 1, OK: true, JSONBytes: int64ptr(1234)},
			want: `{"proto":1,"ok":true,"json_bytes":1234}`,
		},
		{
			name: "probe-domain empty .claude.json: 0 survives the pointer field",
			in:   Response{Proto: 1, OK: true, JSONBytes: int64ptr(0)},
			want: `{"proto":1,"ok":true,"json_bytes":0}`,
		},
		{
			name: "probe-domain absent .claude.json omits json_bytes",
			in:   Response{Proto: 1, OK: true},
			want: `{"proto":1,"ok":true}`,
		},
		{
			name: "shallow probe-domain reply carries listed=true",
			in:   Response{Proto: 1, OK: true, Listed: boolptr(true)},
			want: `{"proto":1,"ok":true,"listed":true}`,
		},
		{
			name: "shallow probe-domain reply carries listed=false (present, not absent)",
			in:   Response{Proto: 1, OK: true, Listed: boolptr(false)},
			want: `{"proto":1,"ok":true,"listed":false}`,
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

// TestClassRoundTrip pins the class<->sentinel mapping both ways and, load-bearing,
// that an UNKNOWN class maps to transient ErrAppUnavailable — never the retreat
// ErrCannotControl. A new app class behind an old client must fail toward retry.
func TestClassRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		want     error
		wantBack string // errToClass(want)
	}{
		{name: "no-entitlement -> ErrCannotControl", class: ClassNoEntitlement, want: ErrCannotControl, wantBack: ClassNoEntitlement},
		{name: "app-unreachable -> ErrAppUnavailable", class: ClassAppUnreachable, want: ErrAppUnavailable, wantBack: ClassAppUnreachable},
		{name: "register-failed -> ErrRegisterFailed", class: ClassRegisterFailed, want: ErrRegisterFailed, wantBack: ClassRegisterFailed},
		{name: "no-domain -> ErrNoDomain", class: ClassNoDomain, want: ErrNoDomain, wantBack: ClassNoDomain},
		{name: "busy -> ErrBusy", class: ClassBusy, want: ErrBusy, wantBack: ClassBusy},
		{name: "domain-not-serving -> ErrDomainNotServing", class: ClassDomainNotServing, want: ErrDomainNotServing, wantBack: ClassDomainNotServing},
		{name: "unknown class fails toward retry (NOT retreat)", class: "future-class", want: ErrAppUnavailable, wantBack: ClassAppUnreachable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classToErr(tc.class)
			if !errors.Is(got, tc.want) {
				t.Fatalf("classToErr(%q) = %v, want errors.Is %v", tc.class, got, tc.want)
			}
			if tc.class == "future-class" && errors.Is(got, ErrCannotControl) {
				t.Fatalf("unknown class %q mapped to the retreat condition ErrCannotControl", tc.class)
			}
			if back := errToClass(got); back != tc.wantBack {
				t.Errorf("errToClass(%v) = %q, want %q", got, back, tc.wantBack)
			}
		})
	}
}

// TestSentinelsDistinct pins that the transient and the permanent retreat
// conditions are NOT errors.Is-confusable either way — this package's single
// most safety-critical invariant.
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
		wantMsg string
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
