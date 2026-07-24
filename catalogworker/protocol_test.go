package catalogworker

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yasyf/fusekit/catalog"
)

func TestRemoteErrorMessageHasExactUTF8ByteBound(t *testing.T) {
	exact := strings.Repeat("a", maxRemoteErrorBytes)
	if encoded := encodeRemoteError(errors.New(exact)); encoded.Message != exact {
		t.Fatalf("exact error length = %d, want %d", len(encoded.Message), len(exact))
	}
	if encoded := encodeRemoteError(errors.New(exact + "b")); len(encoded.Message) != maxRemoteErrorBytes {
		t.Fatalf("max+1 error length = %d, want %d", len(encoded.Message), maxRemoteErrorBytes)
	}
	oversized := strings.Repeat("a", maxRemoteErrorBytes-1) + "€"
	encoded := encodeRemoteError(fmtError(catalog.ErrIntegrity, oversized))
	if len(encoded.Message) > maxRemoteErrorBytes || !utf8.ValidString(encoded.Message) {
		t.Fatalf("bounded error = %d bytes, valid=%t", len(encoded.Message), utf8.ValidString(encoded.Message))
	}
	if encoded.Code != "integrity" {
		t.Fatalf("bounded sentinel code = %q, want integrity", encoded.Code)
	}
}

func TestTopologyStaleErrorKeepsResnapshotClassification(t *testing.T) {
	encoded := encodeRemoteError(&catalog.StaleTopologyRevisionError{Revision: 2, Floor: 9})
	if encoded.Code != "topology_revision_stale" {
		t.Fatalf("stale topology code = %q", encoded.Code)
	}
	if err := decodeRemoteError(encoded); !errors.Is(err, catalog.ErrTopologyRevisionStale) {
		t.Fatalf("decoded stale topology error = %v", err)
	}
}

func TestTenantLifecycleErrorsKeepExactClassification(t *testing.T) {
	for _, sentinel := range []error{
		catalog.ErrTenantLifecycleStale,
		catalog.ErrTenantLifecycleRetryDeferred,
		catalog.ErrTenantMutationConflict,
		catalog.ErrTenantTargetingChanged,
		catalog.ErrTenantPreparationOwnershipConflict,
	} {
		encoded := encodeRemoteError(sentinel)
		if encoded.Code == "" {
			t.Fatalf("encoded %v without an exact code", sentinel)
		}
		if err := decodeRemoteError(encoded); !errors.Is(err, sentinel) {
			t.Fatalf("decoded %v = %v", sentinel, err)
		}
	}
}

func fmtError(sentinel error, detail string) error {
	return errors.Join(sentinel, errors.New(detail))
}
