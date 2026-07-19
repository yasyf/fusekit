package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

const (
	// SourceKeyedLookupLimit is the exact per-call key maximum.
	SourceKeyedLookupLimit = 256
	// SourceKeyedLookupByteLimit is the exact encoded request and response maximum.
	SourceKeyedLookupByteLimit = 1 << 20
)

// SourcePhysicalIndexLookupRequest is one ordered keyed physical-index batch.
type SourcePhysicalIndexLookupRequest struct {
	Authority causal.SourceAuthorityID
	Cursor    uint32
	Locators  []SourceIndexLocator
	Digest    [32]byte
}

// SourcePhysicalIndexLookupEntry preserves one requested locator, including absence.
type SourcePhysicalIndexLookupEntry struct {
	Locator SourceIndexLocator
	Record  *SourcePhysicalIndexRecord
}

// SourcePhysicalIndexLookupPage is the exact replay proof for one keyed batch.
type SourcePhysicalIndexLookupPage struct {
	Cursor        uint32
	Next          uint32
	RequestDigest [32]byte
	Entries       []SourcePhysicalIndexLookupEntry
	Digest        [32]byte
}

// SourceSnapshotPhysicalLookupRequest is one ordered staged-snapshot physical batch.
type SourceSnapshotPhysicalLookupRequest struct {
	Authority causal.SourceAuthorityID
	Snapshot  string
	Cursor    uint32
	Locators  []SourceIndexLocator
	Digest    [32]byte
}

// SourceSnapshotPhysicalLookupPage is the exact replay proof for one staged-snapshot batch.
type SourceSnapshotPhysicalLookupPage struct {
	Authority     causal.SourceAuthorityID
	Snapshot      string
	Cursor        uint32
	Next          uint32
	RequestDigest [32]byte
	Entries       []SourcePhysicalIndexLookupEntry
	Digest        [32]byte
}

// SourceAuthorityBindingLookupRequest is one ordered logical binding batch.
type SourceAuthorityBindingLookupRequest struct {
	Authority causal.SourceAuthorityID
	Cursor    uint32
	Logicals  []string
	Digest    [32]byte
}

// SourceAuthorityBindingLookupEntry preserves one requested logical, including absence.
type SourceAuthorityBindingLookupEntry struct {
	Logical string
	Record  *SourceAuthorityBindingRecord
}

// SourceAuthorityBindingLookupPage is the exact replay proof for one binding batch.
type SourceAuthorityBindingLookupPage struct {
	Cursor        uint32
	Next          uint32
	RequestDigest [32]byte
	Entries       []SourceAuthorityBindingLookupEntry
	Digest        [32]byte
}

// SourceSnapshotRootLookupRequest is one ordered staged-root batch.
type SourceSnapshotRootLookupRequest struct {
	Authority causal.SourceAuthorityID
	Snapshot  string
	Cursor    uint32
	Tenants   []TenantID
	Digest    [32]byte
}

// SourceSnapshotRootLookupEntry preserves one requested tenant, including absence.
type SourceSnapshotRootLookupEntry struct {
	Tenant TenantID
	Root   *SourceSnapshotRoot
}

// SourceSnapshotRootLookupPage is the exact replay proof for one staged-root batch.
type SourceSnapshotRootLookupPage struct {
	Cursor        uint32
	Next          uint32
	RequestDigest [32]byte
	Entries       []SourceSnapshotRootLookupEntry
	Digest        [32]byte
}

// NewSourcePhysicalIndexLookupPage constructs one exact keyed response.
func NewSourcePhysicalIndexLookupPage(
	request SourcePhysicalIndexLookupRequest,
	entries []SourcePhysicalIndexLookupEntry,
) (SourcePhysicalIndexLookupPage, error) {
	page := SourcePhysicalIndexLookupPage{
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Locators)),
		RequestDigest: request.Digest,
		Entries:       append([]SourcePhysicalIndexLookupEntry(nil), entries...),
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourcePhysicalIndexLookupPage{}, err
	}
	return page, nil
}

// NewSourceSnapshotPhysicalLookupRequest constructs one exact staged-snapshot request.
func NewSourceSnapshotPhysicalLookupRequest(
	authority causal.SourceAuthorityID,
	snapshot string,
	cursor uint32,
	locators []SourceIndexLocator,
) (SourceSnapshotPhysicalLookupRequest, error) {
	request := SourceSnapshotPhysicalLookupRequest{
		Authority: authority, Snapshot: snapshot, Cursor: cursor,
		Locators: append([]SourceIndexLocator(nil), locators...),
	}
	request.Digest = sourceKeyedLookupDigest(struct {
		Authority causal.SourceAuthorityID
		Snapshot  string
		Cursor    uint32
		Locators  []SourceIndexLocator
	}{
		Authority: request.Authority, Snapshot: request.Snapshot,
		Cursor: request.Cursor, Locators: request.Locators,
	})
	if err := request.Validate(); err != nil {
		return SourceSnapshotPhysicalLookupRequest{}, err
	}
	return request, nil
}

// Validate verifies one staged-snapshot request without database access.
func (r SourceSnapshotPhysicalLookupRequest) Validate() error {
	if r.Authority == "" || !validSourceLookupText(r.Snapshot, sourceSnapshotCursorLimit) ||
		len(r.Locators) == 0 || len(r.Locators) > SourceKeyedLookupLimit ||
		r.Cursor > ^uint32(0)-uint32(len(r.Locators)) ||
		r.Digest != sourceKeyedLookupDigest(struct {
			Authority causal.SourceAuthorityID
			Snapshot  string
			Cursor    uint32
			Locators  []SourceIndexLocator
		}{
			Authority: r.Authority, Snapshot: r.Snapshot,
			Cursor: r.Cursor, Locators: r.Locators,
		}) {
		return fmt.Errorf("%w: invalid source snapshot physical keyed lookup", ErrInvalidObject)
	}
	for _, locator := range r.Locators {
		if !validSourceLookupText(locator.RootID, 255) ||
			!validSourceLookupText(locator.Relative, 4096) {
			return fmt.Errorf("%w: invalid source snapshot physical locator", ErrInvalidObject)
		}
	}
	return validateSourceKeyedEncoded(r, ErrInvalidObject)
}

// NewSourceSnapshotPhysicalLookupPage constructs one exact staged-snapshot response.
func NewSourceSnapshotPhysicalLookupPage(
	request SourceSnapshotPhysicalLookupRequest,
	entries []SourcePhysicalIndexLookupEntry,
) (SourceSnapshotPhysicalLookupPage, error) {
	page := SourceSnapshotPhysicalLookupPage{
		Authority: request.Authority, Snapshot: request.Snapshot,
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Locators)),
		RequestDigest: request.Digest,
		Entries:       append([]SourcePhysicalIndexLookupEntry(nil), entries...),
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourceSnapshotPhysicalLookupPage{}, err
	}
	return page, nil
}

// Validate verifies one staged-snapshot response against its request.
func (p SourceSnapshotPhysicalLookupPage) Validate(request SourceSnapshotPhysicalLookupRequest) error {
	if request.Validate() != nil || p.Authority != request.Authority || p.Snapshot != request.Snapshot ||
		p.Cursor != request.Cursor || p.Next != request.Cursor+uint32(len(request.Locators)) ||
		p.RequestDigest != request.Digest || len(p.Entries) != len(request.Locators) {
		return fmt.Errorf("%w: invalid source snapshot physical keyed page fence", ErrIntegrity)
	}
	for index, entry := range p.Entries {
		if entry.Locator != request.Locators[index] {
			return fmt.Errorf("%w: reordered source snapshot physical keyed page", ErrIntegrity)
		}
		if entry.Record == nil {
			continue
		}
		record := entry.Record
		if record.Authority != request.Authority || record.RootID != entry.Locator.RootID ||
			record.Relative != entry.Locator.Relative || len(record.FileIdentity) == 0 ||
			record.Kind < 1 || record.Kind > 3 || len(record.Payload) == 0 {
			return fmt.Errorf("%w: invalid source snapshot physical keyed record", ErrIntegrity)
		}
	}
	expected := p
	expected.Digest = [32]byte{}
	if p.Digest == ([32]byte{}) || p.Digest != sourceKeyedLookupDigest(expected) {
		return fmt.Errorf("%w: invalid source snapshot physical keyed page digest", ErrIntegrity)
	}
	return validateSourceKeyedEncoded(p, ErrIntegrity)
}

// NewSourceAuthorityBindingLookupPage constructs one exact binding response.
func NewSourceAuthorityBindingLookupPage(
	request SourceAuthorityBindingLookupRequest,
	entries []SourceAuthorityBindingLookupEntry,
) (SourceAuthorityBindingLookupPage, error) {
	page := SourceAuthorityBindingLookupPage{
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Logicals)),
		RequestDigest: request.Digest,
		Entries:       append([]SourceAuthorityBindingLookupEntry(nil), entries...),
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourceAuthorityBindingLookupPage{}, err
	}
	return page, nil
}

// NewSourceSnapshotRootLookupRequest constructs one exact staged-root request.
func NewSourceSnapshotRootLookupRequest(
	authority causal.SourceAuthorityID,
	snapshot string,
	cursor uint32,
	tenants []TenantID,
) (SourceSnapshotRootLookupRequest, error) {
	request := SourceSnapshotRootLookupRequest{
		Authority: authority, Snapshot: snapshot, Cursor: cursor,
		Tenants: append([]TenantID(nil), tenants...),
	}
	request.Digest = sourceKeyedLookupDigest(struct {
		Authority causal.SourceAuthorityID
		Snapshot  string
		Cursor    uint32
		Tenants   []TenantID
	}{
		Authority: request.Authority, Snapshot: request.Snapshot,
		Cursor: request.Cursor, Tenants: request.Tenants,
	})
	if err := request.Validate(); err != nil {
		return SourceSnapshotRootLookupRequest{}, err
	}
	return request, nil
}

// Validate verifies one staged-root request without database access.
func (r SourceSnapshotRootLookupRequest) Validate() error {
	if r.Authority == "" || !validSourceLookupText(r.Snapshot, sourceSnapshotCursorLimit) ||
		len(r.Tenants) == 0 || len(r.Tenants) > SourceKeyedLookupLimit ||
		r.Cursor > ^uint32(0)-uint32(len(r.Tenants)) ||
		r.Digest != sourceKeyedLookupDigest(struct {
			Authority causal.SourceAuthorityID
			Snapshot  string
			Cursor    uint32
			Tenants   []TenantID
		}{
			Authority: r.Authority, Snapshot: r.Snapshot,
			Cursor: r.Cursor, Tenants: r.Tenants,
		}) {
		return fmt.Errorf("%w: invalid source snapshot root keyed lookup", ErrInvalidObject)
	}
	for _, tenant := range r.Tenants {
		if !validSourceLookupText(string(tenant), 255) {
			return fmt.Errorf("%w: invalid source snapshot root tenant", ErrInvalidObject)
		}
	}
	return validateSourceKeyedEncoded(r, ErrInvalidObject)
}

// NewSourceSnapshotRootLookupPage constructs one exact staged-root response.
func NewSourceSnapshotRootLookupPage(
	request SourceSnapshotRootLookupRequest,
	entries []SourceSnapshotRootLookupEntry,
) (SourceSnapshotRootLookupPage, error) {
	page := SourceSnapshotRootLookupPage{
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Tenants)),
		RequestDigest: request.Digest,
		Entries:       append([]SourceSnapshotRootLookupEntry(nil), entries...),
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourceSnapshotRootLookupPage{}, err
	}
	return page, nil
}

// Validate verifies one staged-root response against its request.
func (p SourceSnapshotRootLookupPage) Validate(request SourceSnapshotRootLookupRequest) error {
	if request.Validate() != nil ||
		p.Cursor != request.Cursor || p.Next != request.Cursor+uint32(len(request.Tenants)) ||
		p.RequestDigest != request.Digest || len(p.Entries) != len(request.Tenants) {
		return fmt.Errorf("%w: invalid source snapshot root keyed page fence", ErrIntegrity)
	}
	for index, entry := range p.Entries {
		if entry.Tenant != request.Tenants[index] {
			return fmt.Errorf("%w: reordered source snapshot root keyed page", ErrIntegrity)
		}
		if entry.Root == nil {
			continue
		}
		if entry.Root.Tenant != entry.Tenant || entry.Root.Generation == 0 ||
			!validSourceLookupText(entry.Root.LogicalID, 255) || !validSourceKey(entry.Root.RootKey) {
			return fmt.Errorf("%w: invalid source snapshot root keyed record", ErrIntegrity)
		}
	}
	expected := p
	expected.Digest = [32]byte{}
	if p.Digest == ([32]byte{}) || p.Digest != sourceKeyedLookupDigest(expected) {
		return fmt.Errorf("%w: invalid source snapshot root keyed page digest", ErrIntegrity)
	}
	return validateSourceKeyedEncoded(p, ErrIntegrity)
}

// NewSourcePhysicalIndexLookupRequest constructs one exact keyed request.
func NewSourcePhysicalIndexLookupRequest(
	authority causal.SourceAuthorityID,
	cursor uint32,
	locators []SourceIndexLocator,
) (SourcePhysicalIndexLookupRequest, error) {
	request := SourcePhysicalIndexLookupRequest{
		Authority: authority, Cursor: cursor,
		Locators: append([]SourceIndexLocator(nil), locators...),
	}
	request.Digest = sourceKeyedLookupDigest(struct {
		Authority causal.SourceAuthorityID
		Cursor    uint32
		Locators  []SourceIndexLocator
	}{Authority: request.Authority, Cursor: request.Cursor, Locators: request.Locators})
	if err := request.Validate(); err != nil {
		return SourcePhysicalIndexLookupRequest{}, err
	}
	return request, nil
}

// Validate verifies one physical keyed request without database access.
func (r SourcePhysicalIndexLookupRequest) Validate() error {
	if r.Authority == "" || len(r.Locators) == 0 || len(r.Locators) > SourceKeyedLookupLimit ||
		r.Cursor > ^uint32(0)-uint32(len(r.Locators)) ||
		r.Digest != sourceKeyedLookupDigest(struct {
			Authority causal.SourceAuthorityID
			Cursor    uint32
			Locators  []SourceIndexLocator
		}{Authority: r.Authority, Cursor: r.Cursor, Locators: r.Locators}) {
		return fmt.Errorf("%w: invalid source physical keyed lookup", ErrInvalidObject)
	}
	for _, locator := range r.Locators {
		if !validSourceLookupText(locator.RootID, 255) ||
			!validSourceLookupText(locator.Relative, 4096) {
			return fmt.Errorf("%w: invalid source physical keyed locator", ErrInvalidObject)
		}
	}
	return validateSourceKeyedEncoded(r, ErrInvalidObject)
}

// Validate verifies one physical keyed response against its request.
func (p SourcePhysicalIndexLookupPage) Validate(request SourcePhysicalIndexLookupRequest) error {
	if request.Validate() != nil ||
		p.Cursor != request.Cursor || p.Next != request.Cursor+uint32(len(request.Locators)) ||
		p.RequestDigest != request.Digest || len(p.Entries) != len(request.Locators) {
		return fmt.Errorf("%w: invalid source physical keyed page fence", ErrIntegrity)
	}
	for index, entry := range p.Entries {
		if entry.Locator != request.Locators[index] {
			return fmt.Errorf("%w: reordered source physical keyed page", ErrIntegrity)
		}
		if entry.Record == nil {
			continue
		}
		record := entry.Record
		if record.Authority != request.Authority || record.RootID != entry.Locator.RootID ||
			record.Relative != entry.Locator.Relative || len(record.FileIdentity) == 0 ||
			record.Kind < 1 || record.Kind > 3 || len(record.Payload) == 0 {
			return fmt.Errorf("%w: invalid source physical keyed record", ErrIntegrity)
		}
		for logicalIndex, logical := range record.Logical {
			if !validSourceLookupText(logical, 255) ||
				(logicalIndex > 0 && record.Logical[logicalIndex-1] >= logical) {
				return fmt.Errorf("%w: unordered source physical logicals", ErrIntegrity)
			}
		}
	}
	expected := p
	expected.Digest = [32]byte{}
	if p.Digest == ([32]byte{}) || p.Digest != sourceKeyedLookupDigest(expected) {
		return fmt.Errorf("%w: invalid source physical keyed page digest", ErrIntegrity)
	}
	return validateSourceKeyedEncoded(p, ErrIntegrity)
}

// NewSourceAuthorityBindingLookupRequest constructs one exact logical binding request.
func NewSourceAuthorityBindingLookupRequest(
	authority causal.SourceAuthorityID,
	cursor uint32,
	logicals []string,
) (SourceAuthorityBindingLookupRequest, error) {
	request := SourceAuthorityBindingLookupRequest{
		Authority: authority, Cursor: cursor, Logicals: append([]string(nil), logicals...),
	}
	request.Digest = sourceKeyedLookupDigest(struct {
		Authority causal.SourceAuthorityID
		Cursor    uint32
		Logicals  []string
	}{Authority: request.Authority, Cursor: request.Cursor, Logicals: request.Logicals})
	if err := request.Validate(); err != nil {
		return SourceAuthorityBindingLookupRequest{}, err
	}
	return request, nil
}

// Validate verifies one binding keyed request without database access.
func (r SourceAuthorityBindingLookupRequest) Validate() error {
	if r.Authority == "" || len(r.Logicals) == 0 || len(r.Logicals) > SourceKeyedLookupLimit ||
		r.Cursor > ^uint32(0)-uint32(len(r.Logicals)) ||
		r.Digest != sourceKeyedLookupDigest(struct {
			Authority causal.SourceAuthorityID
			Cursor    uint32
			Logicals  []string
		}{Authority: r.Authority, Cursor: r.Cursor, Logicals: r.Logicals}) {
		return fmt.Errorf("%w: invalid source binding keyed lookup", ErrInvalidObject)
	}
	for _, logical := range r.Logicals {
		if !validSourceLookupText(logical, 255) {
			return fmt.Errorf("%w: invalid source binding logical", ErrInvalidObject)
		}
	}
	return validateSourceKeyedEncoded(r, ErrInvalidObject)
}

// Validate verifies one binding keyed response against its request.
func (p SourceAuthorityBindingLookupPage) Validate(request SourceAuthorityBindingLookupRequest) error {
	if request.Validate() != nil ||
		p.Cursor != request.Cursor || p.Next != request.Cursor+uint32(len(request.Logicals)) ||
		p.RequestDigest != request.Digest || len(p.Entries) != len(request.Logicals) {
		return fmt.Errorf("%w: invalid source binding keyed page fence", ErrIntegrity)
	}
	for index, entry := range p.Entries {
		if entry.Logical != request.Logicals[index] {
			return fmt.Errorf("%w: reordered source binding keyed page", ErrIntegrity)
		}
		if entry.Record == nil {
			continue
		}
		record := entry.Record
		if record.Authority != request.Authority || record.LogicalID != entry.Logical ||
			!validSourceKey(record.SourceKey) {
			return fmt.Errorf("%w: invalid source binding keyed record", ErrIntegrity)
		}
	}
	expected := p
	expected.Digest = [32]byte{}
	if p.Digest == ([32]byte{}) || p.Digest != sourceKeyedLookupDigest(expected) {
		return fmt.Errorf("%w: invalid source binding keyed page digest", ErrIntegrity)
	}
	return validateSourceKeyedEncoded(p, ErrIntegrity)
}

// SourcePhysicalIndexLookup returns one setwise, exactly ordered physical batch.
func (c *Catalog) SourcePhysicalIndexLookup(
	ctx context.Context,
	request SourcePhysicalIndexLookupRequest,
) (SourcePhysicalIndexLookupPage, error) {
	if err := request.Validate(); err != nil {
		return SourcePhysicalIndexLookupPage{}, err
	}
	values := make([]string, len(request.Locators))
	args := make([]any, 0, len(request.Locators)*3+1)
	for index, locator := range request.Locators {
		values[index] = "(?, ?, ?)"
		args = append(args, index, locator.RootID, locator.Relative)
	}
	args = append(args, string(request.Authority))
	rows, err := c.readDB.QueryContext(ctx, `
WITH requested(position, root_id, relative_path) AS (VALUES `+strings.Join(values, ",")+`)
SELECT requested.position, physical.file_identity, physical.physical_kind,
       physical.metadata_fingerprint, physical.content_fingerprint, physical.payload,
       logical.logical_id
FROM requested
LEFT JOIN source_physical_index physical
  ON physical.source_authority = ?
 AND physical.root_id = requested.root_id
 AND physical.relative_path = requested.relative_path
LEFT JOIN source_physical_logical logical
  ON logical.source_authority = physical.source_authority
 AND logical.root_id = physical.root_id
 AND logical.relative_path = physical.relative_path
ORDER BY requested.position, logical.logical_id`, args...)
	if err != nil {
		return SourcePhysicalIndexLookupPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourcePhysicalIndexLookupPage{
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Locators)),
		RequestDigest: request.Digest, Entries: make([]SourcePhysicalIndexLookupEntry, len(request.Locators)),
	}
	for index, locator := range request.Locators {
		page.Entries[index].Locator = locator
	}
	for rows.Next() {
		var position int
		var identity, metadata, content, payload []byte
		var kind sql.NullInt64
		var logical sql.NullString
		if err := rows.Scan(&position, &identity, &kind, &metadata, &content, &payload, &logical); err != nil {
			return SourcePhysicalIndexLookupPage{}, err
		}
		if position < 0 || position >= len(page.Entries) {
			return SourcePhysicalIndexLookupPage{}, fmt.Errorf("%w: corrupt keyed lookup position", ErrIntegrity)
		}
		if len(identity) == 0 {
			continue
		}
		entry := &page.Entries[position]
		if entry.Record == nil {
			if !kind.Valid || kind.Int64 < 1 || kind.Int64 > 3 ||
				len(metadata) != sha256.Size || len(content) != sha256.Size || len(payload) == 0 {
				return SourcePhysicalIndexLookupPage{}, fmt.Errorf("%w: corrupt source physical keyed record", ErrIntegrity)
			}
			entry.Record = &SourcePhysicalIndexRecord{
				Authority: request.Authority, RootID: entry.Locator.RootID, Relative: entry.Locator.Relative,
				FileIdentity: append([]byte(nil), identity...), Kind: uint8(kind.Int64),
				MetadataFingerprint: bytesToDigest(metadata), ContentFingerprint: bytesToDigest(content),
				Payload: append([]byte(nil), payload...),
			}
		}
		if logical.Valid {
			entry.Record.Logical = append(entry.Record.Logical, logical.String)
		}
	}
	if err := rows.Err(); err != nil {
		return SourcePhysicalIndexLookupPage{}, err
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourcePhysicalIndexLookupPage{}, err
	}
	return page, nil
}

// SourceSnapshotStageLookup returns one setwise, exactly ordered staged snapshot batch.
func (c *Catalog) SourceSnapshotStageLookup(
	ctx context.Context,
	request SourceSnapshotPhysicalLookupRequest,
) (SourceSnapshotPhysicalLookupPage, error) {
	if err := request.Validate(); err != nil {
		return SourceSnapshotPhysicalLookupPage{}, err
	}
	values := make([]string, len(request.Locators))
	args := make([]any, 0, len(request.Locators)*3+2)
	for index, locator := range request.Locators {
		values[index] = "(?, ?, ?)"
		args = append(args, index, locator.RootID, locator.Relative)
	}
	args = append(args, string(request.Authority), request.Snapshot)
	rows, err := c.readDB.QueryContext(ctx, `
WITH requested(position, root_id, relative_path) AS (VALUES `+strings.Join(values, ",")+`)
SELECT requested.position, physical.file_identity, physical.physical_kind,
       physical.metadata_fingerprint, physical.content_fingerprint, physical.payload
FROM requested
LEFT JOIN source_snapshot_stages physical
  ON physical.source_authority = ?
 AND physical.snapshot_id = ?
 AND physical.root_id = requested.root_id
 AND physical.relative_path = requested.relative_path
ORDER BY requested.position`, args...)
	if err != nil {
		return SourceSnapshotPhysicalLookupPage{}, err
	}
	defer func() { _ = rows.Close() }()
	entries := make([]SourcePhysicalIndexLookupEntry, len(request.Locators))
	for index, locator := range request.Locators {
		entries[index].Locator = locator
	}
	for rows.Next() {
		var position int
		var identity, metadata, content, payload []byte
		var kind sql.NullInt64
		if err := rows.Scan(&position, &identity, &kind, &metadata, &content, &payload); err != nil {
			return SourceSnapshotPhysicalLookupPage{}, err
		}
		if position < 0 || position >= len(entries) {
			return SourceSnapshotPhysicalLookupPage{}, fmt.Errorf("%w: corrupt snapshot keyed lookup position", ErrIntegrity)
		}
		if len(identity) == 0 {
			continue
		}
		if !kind.Valid || kind.Int64 < 1 || kind.Int64 > 3 ||
			len(metadata) != sha256.Size || len(content) != sha256.Size || len(payload) == 0 {
			return SourceSnapshotPhysicalLookupPage{}, fmt.Errorf("%w: corrupt snapshot keyed record", ErrIntegrity)
		}
		locator := entries[position].Locator
		entries[position].Record = &SourcePhysicalIndexRecord{
			Authority: request.Authority, RootID: locator.RootID, Relative: locator.Relative,
			FileIdentity: append([]byte(nil), identity...), Kind: uint8(kind.Int64),
			MetadataFingerprint: bytesToDigest(metadata), ContentFingerprint: bytesToDigest(content),
			Payload: append([]byte(nil), payload...),
		}
	}
	if err := rows.Err(); err != nil {
		return SourceSnapshotPhysicalLookupPage{}, err
	}
	return NewSourceSnapshotPhysicalLookupPage(request, entries)
}

// SourceSnapshotRootLookup returns one setwise, exactly ordered staged-root batch.
func (c *Catalog) SourceSnapshotRootLookup(
	ctx context.Context,
	request SourceSnapshotRootLookupRequest,
) (SourceSnapshotRootLookupPage, error) {
	if err := request.Validate(); err != nil {
		return SourceSnapshotRootLookupPage{}, err
	}
	values := make([]string, len(request.Tenants))
	args := make([]any, 0, len(request.Tenants)*2+2)
	for index, tenant := range request.Tenants {
		values[index] = "(?, ?)"
		args = append(args, index, string(tenant))
	}
	args = append(args, string(request.Authority), request.Snapshot)
	rows, err := c.readDB.QueryContext(ctx, `
WITH requested(position, tenant) AS (VALUES `+strings.Join(values, ",")+`)
SELECT requested.position, root.generation, root.logical_id, root.root_key
FROM requested
LEFT JOIN source_snapshot_roots root
  ON root.source_authority = ?
 AND root.snapshot_id = ?
 AND root.tenant = requested.tenant
ORDER BY requested.position`, args...)
	if err != nil {
		return SourceSnapshotRootLookupPage{}, err
	}
	defer func() { _ = rows.Close() }()
	entries := make([]SourceSnapshotRootLookupEntry, len(request.Tenants))
	for index, tenant := range request.Tenants {
		entries[index].Tenant = tenant
	}
	for rows.Next() {
		var position int
		var generation sql.NullInt64
		var logical, key sql.NullString
		if err := rows.Scan(&position, &generation, &logical, &key); err != nil {
			return SourceSnapshotRootLookupPage{}, err
		}
		if position < 0 || position >= len(entries) {
			return SourceSnapshotRootLookupPage{}, fmt.Errorf("%w: corrupt snapshot root keyed lookup position", ErrIntegrity)
		}
		if !generation.Valid {
			continue
		}
		if generation.Int64 < 1 || !logical.Valid || !key.Valid {
			return SourceSnapshotRootLookupPage{}, fmt.Errorf("%w: corrupt snapshot root keyed record", ErrIntegrity)
		}
		entry := &entries[position]
		entry.Root = &SourceSnapshotRoot{
			Tenant: entry.Tenant, Generation: Generation(generation.Int64),
			LogicalID: logical.String, RootKey: SourceObjectKey(key.String),
		}
	}
	if err := rows.Err(); err != nil {
		return SourceSnapshotRootLookupPage{}, err
	}
	return NewSourceSnapshotRootLookupPage(request, entries)
}

// SourceAuthorityBindingLookup returns one setwise, exactly ordered logical binding batch.
func (c *Catalog) SourceAuthorityBindingLookup(
	ctx context.Context,
	request SourceAuthorityBindingLookupRequest,
) (SourceAuthorityBindingLookupPage, error) {
	if err := request.Validate(); err != nil {
		return SourceAuthorityBindingLookupPage{}, err
	}
	values := make([]string, len(request.Logicals))
	args := make([]any, 0, len(request.Logicals)*2+1)
	for index, logical := range request.Logicals {
		values[index] = "(?, ?)"
		args = append(args, index, logical)
	}
	args = append(args, string(request.Authority))
	rows, err := c.readDB.QueryContext(ctx, `
WITH requested(position, logical_id) AS (VALUES `+strings.Join(values, ",")+`)
SELECT requested.position, binding.source_key, binding.effective_fingerprint
FROM requested
LEFT JOIN source_authority_bindings binding
  ON binding.source_authority = ?
 AND binding.logical_id = requested.logical_id
ORDER BY requested.position`, args...)
	if err != nil {
		return SourceAuthorityBindingLookupPage{}, err
	}
	defer func() { _ = rows.Close() }()
	page := SourceAuthorityBindingLookupPage{
		Cursor: request.Cursor, Next: request.Cursor + uint32(len(request.Logicals)),
		RequestDigest: request.Digest, Entries: make([]SourceAuthorityBindingLookupEntry, len(request.Logicals)),
	}
	for index, logical := range request.Logicals {
		page.Entries[index].Logical = logical
	}
	for rows.Next() {
		var position int
		var key sql.NullString
		var fingerprint []byte
		if err := rows.Scan(&position, &key, &fingerprint); err != nil {
			return SourceAuthorityBindingLookupPage{}, err
		}
		if position < 0 || position >= len(page.Entries) {
			return SourceAuthorityBindingLookupPage{}, fmt.Errorf("%w: corrupt binding keyed lookup position", ErrIntegrity)
		}
		if !key.Valid {
			continue
		}
		if len(fingerprint) != sha256.Size {
			return SourceAuthorityBindingLookupPage{}, fmt.Errorf("%w: corrupt source binding keyed record", ErrIntegrity)
		}
		entry := &page.Entries[position]
		entry.Record = &SourceAuthorityBindingRecord{
			Authority: request.Authority, LogicalID: entry.Logical,
			SourceKey: SourceObjectKey(key.String), Fingerprint: bytesToDigest(fingerprint),
		}
	}
	if err := rows.Err(); err != nil {
		return SourceAuthorityBindingLookupPage{}, err
	}
	page.Digest = sourceKeyedLookupDigest(page)
	if err := page.Validate(request); err != nil {
		return SourceAuthorityBindingLookupPage{}, err
	}
	return page, nil
}

func sourceKeyedLookupDigest(value any) [32]byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(encoded)
}

func validateSourceKeyedEncoded(value any, sentinel error) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%w: encode source keyed lookup: %v", sentinel, err)
	}
	if len(encoded) == 0 || len(encoded) > SourceKeyedLookupByteLimit {
		return fmt.Errorf("%w: source keyed lookup exceeds encoded byte limit", sentinel)
	}
	return nil
}

func validSourceLookupText(value string, limit int) bool {
	return value != "" && len(value) <= limit && !strings.ContainsRune(value, 0)
}
