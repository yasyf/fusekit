package catalogproto

import (
	"crypto/sha256"
	"encoding/hex"
)

const domainIdentityPrefix = "fusekit.domain.v1\x00"

// DeriveDomainID returns the stable path-free identity for one owner account instance.
func DeriveDomainID(owner OwnerID, account AccountInstanceID) (DomainID, error) {
	if err := validateOpaque(string(owner)); err != nil {
		return "", err
	}
	if err := validateOpaque(string(account)); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(domainIdentityPrefix + string(owner) + "\x00" + string(account)))
	return DomainID("fk-" + hex.EncodeToString(digest[:])), nil
}
