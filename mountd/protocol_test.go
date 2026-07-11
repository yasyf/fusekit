package mountd

import (
	"encoding/json"
	"reflect"
	"testing"
)

// These tests pin the proto-2 wire artifacts byte-for-byte: a failing golden
// is a protocol break — fix the change, not the literal.

func TestWireFreezeProtoVersion(t *testing.T) {
	if MountProtoVersion != 2 {
		t.Fatalf("MountProtoVersion = %d; holder v2 speaks proto 2 — within it, evolution is additive", MountProtoVersion)
	}
}

func TestWireFreezeOps(t *testing.T) {
	want := map[string]Op{
		"hello":        OpHello,
		"health":       OpHealth,
		"probe":        OpProbe,
		"mount":        OpMount,
		"unmount":      OpUnmount,
		"list":         OpList,
		"reclaim":      OpReclaim,
		"leases":       OpLeases,
		"addbridge":    OpAddBridge,
		"removebridge": OpRemoveBridge,
		"bridges":      OpBridges,
	}
	for frozen, op := range want {
		if string(op) != frozen {
			t.Errorf("op drifted: %q, frozen value is %q", op, frozen)
		}
	}
}

func TestWireFreezeFeatures(t *testing.T) {
	want := []string{"mux", "bridge", "tree", "lease-gate"}
	if !reflect.DeepEqual(HolderFeatures, want) {
		t.Fatalf("HolderFeatures = %v, want %v (frozen wire artifacts)", HolderFeatures, want)
	}
}

func TestWireFreezeErrClasses(t *testing.T) {
	want := map[string]string{
		"tcc":                   ClassTCC,
		"mount-timeout":         ClassMountTimeout,
		"mount-failed":          ClassMountFailed,
		"wedged":                ClassWedged,
		"foreign-mount":         ClassForeignMount,
		"busy":                  ClassBusy,
		"base-mismatch":         ClassBaseMismatch,
		"content-unavailable":   ClassContentUnavailable,
		"foreign-bridge":        ClassForeignBridge,
		"invalid-owner":         ClassInvalidOwner,
		"bridge-socket-changed": ClassBridgeSocketChanged,
		"owner-mismatch":        ClassOwnerMismatch,
		"mux-mismatch":          ClassMuxMismatch,
		"proto-mismatch":        ClassProtoMismatch,
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
			name: "mount fields",
			in:   Request{Proto: 2, Op: OpMount, Base: "/pool/base", Dir: "/pool/acct-01", Owner: "cc-pool"},
			want: `{"proto":2,"op":"mount","base":"/pool/base","dir":"/pool/acct-01","owner":"cc-pool"}`,
		},
		{
			name: "optional fields omitted when empty",
			in:   Request{Proto: 2, Op: OpHealth},
			want: `{"proto":2,"op":"health"}`,
		},
		{
			name: "all flag",
			in:   Request{Proto: 2, Op: OpList, Owner: "doctor", All: true},
			want: `{"proto":2,"op":"list","owner":"doctor","all":true}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-2 wire artifact — see file comment)", got, tc.want)
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

// TestWireIgnoresLegacyPolicyFields pins that a proto-1 sender's deleted
// fields decode away silently (Go's default unknown-field ignoring) — the
// proto gate, not a decode error, is what refuses them.
func TestWireIgnoresLegacyPolicyFields(t *testing.T) {
	var req Request
	line := `{"proto":1,"op":"mount","base":"/b","dir":"/d","idle_policy":"attest","carcass_policy":"force","dirs":["/d"],"ttl":60000000000}`
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		t.Fatalf("legacy request failed to decode: %v", err)
	}
	if req.Proto != 1 || req.Op != OpMount || req.Base != "/b" || req.Dir != "/d" {
		t.Fatalf("legacy request decoded wrong: %+v", req)
	}
}

func TestWireFreezeResponse(t *testing.T) {
	tests := []struct {
		name string
		in   Response
		want string
	}{
		{
			name: "every core field set",
			in: Response{
				Proto:    2,
				OK:       false,
				Error:    "boom",
				ErrClass: ClassTCC,
				Version:  "1.2.3 (abc1234)",
				FuseOK:   true,
				Mounts:   []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: true}},
			},
			want: `{"proto":2,"ok":false,"error":"boom","err_class":"tcc","version":"1.2.3 (abc1234)","fuse_ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":true}]}`,
		},
		{
			name: "optional fields omitted when empty",
			in:   Response{Proto: 2, OK: true},
			want: `{"proto":2,"ok":true}`,
		},
		{
			name: "hello features",
			in:   Response{Proto: 2, OK: true, Version: "v1.0.0", Features: []string{"mux", "bridge", "tree", "lease-gate"}},
			want: `{"proto":2,"ok":true,"version":"v1.0.0","features":["mux","bridge","tree","lease-gate"]}`,
		},
		{
			name: "MountInfo always carries all three fields",
			in:   Response{Proto: 2, OK: true, Mounts: []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: false}}},
			want: `{"proto":2,"ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":false}]}`,
		},
		{
			name: "MountInfo epoch and mount-time fields",
			in:   Response{Proto: 2, OK: true, Mounts: []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: false, Epoch: 3, MountedAt: 1765500000}}},
			want: `{"proto":2,"ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":false,"epoch":3,"mounted_at":1765500000}]}`,
		},
		{
			name: "lease diagnostic",
			in:   Response{Proto: 2, OK: true, Leases: []LeaseInfo{{File: "/l/ab.lease", Held: true, Dir: "/pool/acct-01", Owner: "cc-pool", PID: 42, Argv0: "claude", Started: 1765500000}}},
			want: `{"proto":2,"ok":true,"leases":[{"file":"/l/ab.lease","held":true,"dir":"/pool/acct-01","owner":"cc-pool","pid":42,"argv0":"claude","started":1765500000}]}`,
		},
		{
			name: "health status fields",
			in: Response{
				Proto:                2,
				OK:                   true,
				Retiring:             true,
				ParkedUntil:          1765500000,
				JournalMounts:        2,
				JournalBridges:       1,
				LeasesTotal:          3,
				LeasesHeld:           1,
				RetireStrikes:        []int64{1765490000, 1765499000},
				RetireDeferredDir:    "/pool/acct-01",
				RetireDeferredReason: "installed bundle is v9.9.10, this holder is v9.9.9",
			},
			want: `{"proto":2,"ok":true,"retiring":true,"parked_until":1765500000,"journal_mounts":2,"journal_bridges":1,"leases_total":3,"leases_held":1,"retire_strikes":[1765490000,1765499000],"retire_deferred_dir":"/pool/acct-01","retire_deferred_reason":"installed bundle is v9.9.10, this holder is v9.9.9"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-2 wire artifact — see file comment)", got, tc.want)
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

// TestWireFreezeEmptyMountsOmitted pins that a non-nil empty Mounts slice
// (a clean sweep's failed-dirs) marshals to no field at all.
func TestWireFreezeEmptyMountsOmitted(t *testing.T) {
	got, err := json.Marshal(Response{Proto: 2, OK: true, Mounts: []MountInfo{}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"proto":2,"ok":true}`; string(got) != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}
}

// TestWireNoForceField grep-proofs the ladder contract at the type level: no
// wire field named "force" exists on Request or Response.
func TestWireNoForceField(t *testing.T) {
	for _, v := range []any{Request{}, Response{}, MountInfo{}, LeaseInfo{}, BridgeInfo{}} {
		typ := reflect.TypeOf(v)
		for i := range typ.NumField() {
			tag := typ.Field(i).Tag.Get("json")
			if tag == "force" || len(tag) >= 6 && tag[:6] == "force," {
				t.Errorf("%s.%s carries a wire field named force", typ.Name(), typ.Field(i).Name)
			}
		}
	}
}
