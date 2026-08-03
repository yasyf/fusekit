// Package recoveryid defines FuseKit's durable process-recovery barriers.
package recoveryid

import (
	"encoding"
	"errors"
	"fmt"
	"strings"
)

// ID names one stable consumer-owned recovery barrier.
type ID string

var (
	_ encoding.TextMarshaler   = ID("")
	_ encoding.TextUnmarshaler = (*ID)(nil)
)

const (
	// SourceOwner settles retired source-authority runtime owners.
	SourceOwner ID = "fusekit.source-owner.v1"
	// SourceDriver settles retired semantic source-driver processes.
	SourceDriver ID = "fusekit.source-driver.v1"
	// Broker settles retired File Provider broker processes.
	Broker ID = "fusekit.broker.v1"
	// NativeMount settles retired native mount processes.
	NativeMount ID = "fusekit.native-mount.v1"
	// CatalogWorker settles retired catalog worker processes.
	CatalogWorker ID = "fusekit.catalog-worker.v1"
	// SourceObserver settles retired physical source-observer processes.
	SourceObserver ID = "fusekit.source-observer.v1"
	// SourceTask settles retired one-task source children.
	SourceTask ID = "fusekit.source-task.v1"
	// Holder settles retired FuseKit runtime owners.
	Holder ID = "fusekit.holder.v1"
)

const maxIDBytes = 127

// String returns the exact namespaced v1 representation.
func (id ID) String() string { return string(id) }

// MarshalText encodes one validated recovery identifier.
func (id ID) MarshalText() ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return []byte(id), nil
}

// UnmarshalText replaces id only after strict validation.
func (id *ID) UnmarshalText(text []byte) error {
	parsed := ID(text)
	if err := parsed.Validate(); err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Validate rejects a noncanonical namespaced v1 recovery identifier.
func (id ID) Validate() error {
	if len(id) == 0 || len(id) > maxIDBytes {
		return errors.New("recoveryid: recovery id length is invalid")
	}
	segmentStart := true
	segments := 1
	for _, value := range []byte(id) {
		switch {
		case value == '.':
			if segmentStart {
				return fmt.Errorf("recoveryid: invalid recovery id %q", id)
			}
			segmentStart = true
			segments++
		case segmentStart && value >= 'a' && value <= 'z':
			segmentStart = false
		case !segmentStart && ((value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '-'):
		default:
			return fmt.Errorf("recoveryid: invalid recovery id %q", id)
		}
	}
	if segmentStart || segments < 3 || !strings.HasSuffix(string(id), ".v1") {
		return fmt.Errorf("recoveryid: invalid recovery id %q", id)
	}
	return nil
}
