package catalogproto

import "github.com/yasyf/fusekit/causal"

// DeriveDomainID returns the stable path-free identity for one owner account instance.
func DeriveDomainID(owner OwnerID, account AccountInstanceID) (DomainID, error) {
	if err := validateOpaque(string(owner)); err != nil {
		return "", err
	}
	if err := validateOpaque(string(account)); err != nil {
		return "", err
	}
	id, err := causal.DeriveDomainID(string(owner), string(account))
	return DomainID(id), err
}
