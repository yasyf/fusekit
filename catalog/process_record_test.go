package catalog

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/internal/recoveryid"
)

func processRecordForTest() ProcessRecord {
	return ProcessRecord{
		RecoveryID: recoveryid.SourceOwner,
		PID:        4242,
		StartTime:  "start",
		Boot:       "boot",
		Comm:       "holder",
		Generation: ProcessGeneration{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
}

func processAuditTokenForTest(pid int, version uint32) AuditToken {
	var token AuditToken
	binary.NativeEndian.PutUint32(token[20:24], uint32(pid))
	binary.NativeEndian.PutUint32(token[28:32], version)
	return token
}

func TestProcessRecordValidateRejectsIncompleteIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutate  func(*ProcessRecord)
		wantErr bool
	}{
		{name: "complete", mutate: func(*ProcessRecord) {}},
		{
			name:    "unknown recovery id",
			mutate:  func(r *ProcessRecord) { r.RecoveryID = "fusekit.source-owner" },
			wantErr: true,
		},
		{name: "no pid", mutate: func(r *ProcessRecord) { r.PID = 0 }, wantErr: true},
		{name: "no start time", mutate: func(r *ProcessRecord) { r.StartTime = "" }, wantErr: true},
		{name: "no boot", mutate: func(r *ProcessRecord) { r.Boot = "" }, wantErr: true},
		{
			name:    "zero generation",
			mutate:  func(r *ProcessRecord) { r.Generation = ProcessGeneration{} },
			wantErr: true,
		},
		{
			name:   "process group leader",
			mutate: func(r *ProcessRecord) { r.ProcessGroup = true; r.SessionID = r.PID },
		},
		{
			name:    "process group without session",
			mutate:  func(r *ProcessRecord) { r.ProcessGroup = true },
			wantErr: true,
		},
		{
			name:    "session without process group",
			mutate:  func(r *ProcessRecord) { r.SessionID = r.PID },
			wantErr: true,
		},
		{
			name: "audit token identity",
			mutate: func(r *ProcessRecord) {
				r.AuditToken = processAuditTokenForTest(r.PID, 3)
				r.Executable = "/usr/local/bin/fusekit"
			},
		},
		{
			name: "audit token for another pid",
			mutate: func(r *ProcessRecord) {
				r.AuditToken = processAuditTokenForTest(r.PID+1, 3)
				r.Executable = "/usr/local/bin/fusekit"
			},
			wantErr: true,
		},
		{
			name: "audit token without executable",
			mutate: func(r *ProcessRecord) {
				r.AuditToken = processAuditTokenForTest(r.PID, 3)
			},
			wantErr: true,
		},
		{
			name: "audit token without execution version",
			mutate: func(r *ProcessRecord) {
				r.AuditToken = processAuditTokenForTest(r.PID, 0)
				r.Executable = "/usr/local/bin/fusekit"
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			record := processRecordForTest()
			tt.mutate(&record)
			err := record.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() = %v, want error %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("Validate() = %v, want ErrInvalidObject", err)
			}
		})
	}
}

func TestProcessRecordEncodesItsExactDurableShape(t *testing.T) {
	t.Parallel()
	record := processRecordForTest()
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal process record: %v", err)
	}
	token := "[" + strings.TrimSuffix(strings.Repeat("0,", auditTokenLength), ",") + "]"
	want := `{"recovery_id":"fusekit.source-owner.v1","pid":4242,"start_time":"start",` +
		`"boot":"boot","comm":"holder","executable":"","audit_token":` + token +
		`,"generation":"0102030405060708090a0b0c0d0e0f10","process_group":false,"session_id":0}`
	if string(encoded) != want {
		t.Fatalf("encoded process record = %s, want %s", encoded, want)
	}
	var decoded ProcessRecord
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal process record: %v", err)
	}
	if decoded != record {
		t.Fatalf("decoded process record = %+v, want %+v", decoded, record)
	}
}

func TestProcessRecordRefusesNoncanonicalScalars(t *testing.T) {
	t.Parallel()
	if _, err := json.Marshal(ProcessRecord{}); err == nil {
		t.Fatal("marshalled a record with a zero generation and unknown recovery id")
	}
	tests := []struct {
		name    string
		encoded string
	}{
		{name: "uppercase generation", encoded: `{"generation":"0102030405060708090A0B0C0D0E0F10"}`},
		{name: "short generation", encoded: `{"generation":"0102"}`},
		{name: "zero generation", encoded: `{"generation":"` + strings.Repeat("0", 32) + `"}`},
		{name: "invalid recovery id", encoded: `{"recovery_id":"FuseKit.Source-Owner.v1"}`},
		{name: "short audit token", encoded: `{"audit_token":[0,1]}`},
		{name: "null audit token element", encoded: `{"audit_token":[` +
			strings.TrimSuffix(strings.Repeat("null,", auditTokenLength), ",") + `]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var record ProcessRecord
			if err := json.Unmarshal([]byte(tt.encoded), &record); err == nil {
				t.Fatalf("decoded %s into %+v", tt.encoded, record)
			}
		})
	}
}

func TestReapReceiptSealsTheExactRetirementProof(t *testing.T) {
	t.Parallel()
	record := processRecordForTest()
	successor := ProcessGeneration{9}
	receipt, err := NewReapReceipt(ReceiptLedgerID{1}, 7, record, successor, ReapAbsent)
	if err != nil {
		t.Fatalf("NewReapReceipt: %v", err)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ReapReceipt)
	}{
		{name: "no ledger", mutate: func(r *ReapReceipt) { r.LedgerID = ReceiptLedgerID{} }},
		{name: "no sequence", mutate: func(r *ReapReceipt) { r.Sequence = 0 }},
		{name: "other sequence", mutate: func(r *ReapReceipt) { r.Sequence = 8 }},
		{name: "other record", mutate: func(r *ReapReceipt) { r.Record.PID = 99 }},
		{name: "invalid record", mutate: func(r *ReapReceipt) { r.Record.Boot = "" }},
		{name: "self-settling", mutate: func(r *ReapReceipt) { r.ReaperGeneration = r.Record.Generation }},
		{name: "no successor", mutate: func(r *ReapReceipt) { r.ReaperGeneration = ProcessGeneration{} }},
		{name: "unknown outcome", mutate: func(r *ReapReceipt) { r.Outcome = ReapOutcome(9) }},
		{name: "forged digest", mutate: func(r *ReapReceipt) { r.Digest[0] ^= 0xff }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tampered := receipt
			tt.mutate(&tampered)
			if err := tampered.Validate(); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("Validate() = %v, want ErrInvalidObject", err)
			}
		})
	}
}

func TestNewReapReceiptRefusesAnInvalidProof(t *testing.T) {
	t.Parallel()
	record := processRecordForTest()
	if _, err := NewReapReceipt(
		ReceiptLedgerID{}, 1, record, ProcessGeneration{9}, ReapAbsent,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("NewReapReceipt without a ledger = %v, want ErrInvalidObject", err)
	}
	if _, err := NewReapReceipt(
		ReceiptLedgerID{1}, 1, ProcessRecord{}, ProcessGeneration{9}, ReapAbsent,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("NewReapReceipt with an invalid record = %v, want ErrInvalidObject", err)
	}
	if _, err := NewReapReceipt(
		ReceiptLedgerID{1}, 1, record, record.Generation, ReapAbsent,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("NewReapReceipt with a self-settling generation = %v, want ErrInvalidObject", err)
	}
}
