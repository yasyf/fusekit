package catalogproto

import (
	"strings"
	"testing"
)

func TestDomainIDIsDeterministicOpaqueAndIdentitySensitive(t *testing.T) {
	owner := OwnerID("com.yasyf.cc-pool")
	account := PresentationInstanceID("account-instance-secret-shape")
	first, err := DeriveDomainID(owner, account)
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	second, err := DeriveDomainID(owner, account)
	if err != nil || second != first {
		t.Fatalf("second derivation = %q, %v; want %q", second, err, first)
	}
	otherOwner, _ := DeriveDomainID(OwnerID("com.yasyf.other"), account)
	otherAccount, _ := DeriveDomainID(owner, PresentationInstanceID("other-account"))
	if otherOwner == first || otherAccount == first {
		t.Fatal("domain derivation ignored an immutable identity component")
	}
	if strings.Contains(string(first), string(owner)) || strings.Contains(string(first), string(account)) || strings.ContainsAny(string(first), "/\\") {
		t.Fatalf("derived domain leaks source identity: %q", first)
	}
	if err := validateOpaque(string(first)); err != nil {
		t.Fatalf("derived domain is not opaque: %v", err)
	}
}
