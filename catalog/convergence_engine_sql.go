package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

func applyConvergenceEngineDelta(ctx context.Context, tx *sql.Tx, page ConvergenceEngineDeltaPage) error {
	for _, authority := range page.DeleteHeads {
		if _, err := tx.ExecContext(ctx, `DELETE FROM convergence_engine_heads WHERE source_authority = ?`, string(authority)); err != nil {
			return fmt.Errorf("catalog: delete convergence engine head: %w", err)
		}
	}
	for _, head := range page.UpsertHeads {
		if head.Authority == "" || head.Head == 0 || head.DedupFloor > head.Head {
			return fmt.Errorf("%w: invalid convergence engine head", ErrInvalidObject)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_heads(source_authority, head, dedup_floor) VALUES (?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET head = excluded.head, dedup_floor = excluded.dedup_floor`,
			string(head.Authority), uint64(head.Head), uint64(head.DedupFloor)); err != nil {
			return fmt.Errorf("catalog: upsert convergence engine head: %w", mapConstraint(err))
		}
	}
	for _, domain := range page.DeleteDomains {
		if _, err := tx.ExecContext(ctx, `DELETE FROM convergence_engine_domains WHERE domain_id = ?`, string(domain)); err != nil {
			return fmt.Errorf("catalog: delete convergence engine domain: %w", err)
		}
	}
	for _, domain := range page.UpsertDomains {
		if err := upsertConvergenceEngineDomain(ctx, tx, domain); err != nil {
			return err
		}
	}
	for _, change := range page.DeleteChanges {
		if _, err := tx.ExecContext(ctx, `DELETE FROM convergence_engine_changes WHERE change_id = ?`, change[:]); err != nil {
			return fmt.Errorf("catalog: delete convergence engine change: %w", err)
		}
	}
	for _, change := range page.UpsertChanges {
		if err := upsertConvergenceEngineChange(ctx, tx, change); err != nil {
			return err
		}
	}
	if page.ResetOutbox {
		if _, err := tx.ExecContext(ctx, `DELETE FROM convergence_engine_outbox WHERE singleton = 1`); err != nil {
			return fmt.Errorf("catalog: reset convergence engine outbox: %w", err)
		}
		if page.Outbox != nil {
			if err := insertConvergenceEngineOutbox(ctx, tx, *page.Outbox); err != nil {
				return err
			}
		}
	} else if page.Outbox != nil {
		return fmt.Errorf("%w: outbox metadata requires reset", ErrInvalidObject)
	}
	return nil
}

func upsertConvergenceEngineDomain(ctx context.Context, tx *sql.Tx, domain ConvergenceEngineDomain) error {
	if domain.Tenant == "" || domain.Domain == "" || domain.Generation == 0 || domain.CatalogRevision == 0 {
		return fmt.Errorf("%w: invalid convergence engine domain", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_domains(
    domain_id, tenant, generation, fingerprint, catalog_revision,
    notified_catalog_revision, observed_catalog_revision,
    desired, notified, observed, demanded, forced,
    pending_sent_unix_nano, quarantine_since_unix_nano, quarantine_until_unix_nano
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(domain_id) DO UPDATE SET
    tenant = excluded.tenant, generation = excluded.generation, fingerprint = excluded.fingerprint,
    catalog_revision = excluded.catalog_revision,
    notified_catalog_revision = excluded.notified_catalog_revision,
    observed_catalog_revision = excluded.observed_catalog_revision,
    desired = excluded.desired, notified = excluded.notified, observed = excluded.observed,
    demanded = excluded.demanded, forced = excluded.forced,
    pending_sent_unix_nano = excluded.pending_sent_unix_nano,
    quarantine_since_unix_nano = excluded.quarantine_since_unix_nano,
    quarantine_until_unix_nano = excluded.quarantine_until_unix_nano`,
		string(domain.Domain), string(domain.Tenant), uint64(domain.Generation), domain.Fingerprint[:],
		uint64(domain.CatalogRevision), uint64(domain.NotifiedCatalogRevision), uint64(domain.ObservedCatalogRevision),
		uint64(domain.Desired), uint64(domain.Notified), uint64(domain.Observed),
		convergenceBoolInt(domain.Demanded), convergenceBoolInt(domain.Forced), unixNano(domain.PendingSent),
		unixNano(domain.QuarantineSince), unixNano(domain.QuarantineUntil),
	); err != nil {
		return fmt.Errorf("catalog: upsert convergence engine domain: %w", mapConstraint(err))
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM convergence_engine_domain_changes WHERE domain_id = ?`, string(domain.Domain)); err != nil {
		return fmt.Errorf("catalog: reset convergence engine domain changes: %w", err)
	}
	changes := [4]causal.ChangeSet{domain.Applicable, domain.DesiredChange, domain.NotifiedChange, domain.ObservedChange}
	for index, change := range changes {
		if change.ChangeID == (causal.ChangeID{}) {
			continue
		}
		if err := insertConvergenceEngineDomainChange(ctx, tx, domain.Domain, uint8(index+1), change); err != nil {
			return err
		}
	}
	return nil
}

func insertConvergenceEngineDomainChange(
	ctx context.Context,
	tx *sql.Tx,
	domain causal.DomainID,
	slot uint8,
	change causal.ChangeSet,
) error {
	change = causalHeader(change)
	if change.SourceAuthority == "" || change.SourceRevision == 0 || change.ChangeID == (causal.ChangeID{}) ||
		change.OperationID == (causal.OperationID{}) {
		return fmt.Errorf("%w: invalid convergence engine domain change", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_domain_changes(
    domain_id, slot, source_authority, source_revision, change_id, operation_id,
    cause, origin_domain, origin_generation
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(domain), slot, string(change.SourceAuthority), uint64(change.SourceRevision),
		change.ChangeID[:], change.OperationID[:], string(change.Cause),
		string(change.Origin), uint64(change.OriginGeneration),
	); err != nil {
		return fmt.Errorf("catalog: insert convergence engine domain change: %w", mapConstraint(err))
	}
	return nil
}

func upsertConvergenceEngineChange(ctx context.Context, tx *sql.Tx, record ConvergenceEngineChange) error {
	change := causalHeader(record.Change)
	if change.SourceAuthority == "" || change.SourceRevision == 0 || change.ChangeID == (causal.ChangeID{}) ||
		change.OperationID == (causal.OperationID{}) || record.AffectedCount == 0 ||
		record.AffectedDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid convergence engine change", ErrInvalidObject)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_changes(
    change_id, source_authority, source_revision, operation_id,
    cause, origin_domain, origin_generation, engine_revision,
    affected_count, affected_digest
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(change_id) DO UPDATE SET
    source_authority = excluded.source_authority, source_revision = excluded.source_revision,
    operation_id = excluded.operation_id, cause = excluded.cause,
    origin_domain = excluded.origin_domain, origin_generation = excluded.origin_generation,
    engine_revision = excluded.engine_revision,
    affected_count = excluded.affected_count,
    affected_digest = excluded.affected_digest`,
		change.ChangeID[:], string(change.SourceAuthority), uint64(change.SourceRevision),
		change.OperationID[:], string(change.Cause), string(change.Origin),
		uint64(change.OriginGeneration), uint64(record.EngineRevision),
		record.AffectedCount, record.AffectedDigest[:],
	); err != nil {
		return fmt.Errorf("catalog: upsert convergence engine change: %w", mapConstraint(err))
	}
	return nil
}

func insertConvergenceEngineOutbox(ctx context.Context, tx *sql.Tx, outbox ConvergenceEngineOutbox) error {
	change := causalHeader(outbox.Change)
	if outbox.EngineRevision == 0 || outbox.AffectedDigest == ([32]byte{}) {
		return fmt.Errorf("%w: invalid convergence engine outbox progress", ErrInvalidObject)
	}
	switch change.Cause {
	case causal.CauseProviderMutation, causal.CauseDaemonWrite, causal.CauseExternalUnattributed, causal.CauseMigration, causal.CauseOnDemand:
	default:
		return fmt.Errorf("%w: invalid convergence engine outbox cause %q", ErrInvalidObject, change.Cause)
	}
	var settlement []byte
	if outbox.Settlement != nil {
		if outbox.Settlement.ChangeID != change.ChangeID {
			return fmt.Errorf("%w: convergence engine settlement changed identity", ErrInvalidObject)
		}
		settlement = outbox.Settlement.Digest[:]
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_engine_outbox(
    singleton, source_authority, source_revision, change_id, operation_id,
    cause, origin_domain, origin_generation,
    cursor_sequence, cursor_after_key, cursor_after_tenant, settlement_digest,
    engine_revision, commit_count, affected_count, affected_digest
) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(change.SourceAuthority), uint64(change.SourceRevision), change.ChangeID[:],
		change.OperationID[:], string(change.Cause), string(change.Origin),
		uint64(change.OriginGeneration), outbox.Cursor.Sequence,
		string(outbox.Cursor.AfterKey), string(outbox.Cursor.AfterTenant), settlement,
		uint64(outbox.EngineRevision), outbox.CommitCount, outbox.AffectedCount, outbox.AffectedDigest[:],
	); err != nil {
		return fmt.Errorf("catalog: insert convergence engine outbox: %w", mapConstraint(err))
	}
	return nil
}

// PageConvergenceEngine returns one fixed-size page from every normalized relation.
func (c *Catalog) PageConvergenceEngine(
	ctx context.Context,
	cursor ConvergenceEngineCursor,
) (ConvergenceEnginePage, error) {
	header, err := c.ConvergenceEngineHead(ctx)
	if err != nil {
		return ConvergenceEnginePage{}, err
	}
	page := ConvergenceEnginePage{Header: header}
	more := false
	if page.Heads, more, err = pageConvergenceHeads(ctx, c.readDB, cursor.AfterHead); err != nil {
		return ConvergenceEnginePage{}, err
	}
	next := cursor
	if len(page.Heads) != 0 {
		next.AfterHead = page.Heads[len(page.Heads)-1].Authority
	}
	var domainsMore bool
	if page.Domains, domainsMore, err = pageConvergenceDomains(ctx, c.readDB, cursor.AfterDomain); err != nil {
		return ConvergenceEnginePage{}, err
	}
	more = more || domainsMore
	if len(page.Domains) != 0 {
		next.AfterDomain = page.Domains[len(page.Domains)-1].Domain
	}
	var changesMore bool
	if page.Changes, changesMore, err = pageConvergenceChanges(ctx, c.readDB, cursor.AfterChange); err != nil {
		return ConvergenceEnginePage{}, err
	}
	more = more || changesMore
	if len(page.Changes) != 0 {
		next.AfterChange = page.Changes[len(page.Changes)-1].Change.ChangeID
	}
	if cursor == (ConvergenceEngineCursor{}) {
		page.Outbox, err = readConvergenceEngineOutbox(ctx, c.readDB)
		if err != nil {
			return ConvergenceEnginePage{}, err
		}
	}
	if more {
		page.Next = &next
	}
	return page, nil
}

func pageConvergenceHeads(
	ctx context.Context,
	query *sql.DB,
	after causal.SourceAuthorityID,
) ([]ConvergenceEngineHead, bool, error) {
	rows, err := query.QueryContext(ctx, `
SELECT source_authority, head, dedup_floor FROM convergence_engine_heads
WHERE source_authority > ? ORDER BY source_authority LIMIT ?`,
		string(after), ConvergenceEnginePageLimit+1)
	if err != nil {
		return nil, false, fmt.Errorf("catalog: query convergence engine heads: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []ConvergenceEngineHead
	for rows.Next() {
		var record ConvergenceEngineHead
		if err := rows.Scan(&record.Authority, &record.Head, &record.DedupFloor); err != nil {
			return nil, false, fmt.Errorf("catalog: scan convergence engine head: %w", err)
		}
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("catalog: read convergence engine heads: %w", err)
	}
	more := len(result) > ConvergenceEnginePageLimit
	if more {
		result = result[:ConvergenceEnginePageLimit]
	}
	return result, more, nil
}

func pageConvergenceDomains(
	ctx context.Context,
	query *sql.DB,
	after causal.DomainID,
) ([]ConvergenceEngineDomain, bool, error) {
	rows, err := query.QueryContext(ctx, `
SELECT domain_id, tenant, generation, fingerprint, catalog_revision,
       notified_catalog_revision, observed_catalog_revision,
       desired, notified, observed, demanded, forced,
       pending_sent_unix_nano, quarantine_since_unix_nano, quarantine_until_unix_nano
FROM convergence_engine_domains WHERE domain_id > ? ORDER BY domain_id LIMIT ?`,
		string(after), ConvergenceEnginePageLimit+1)
	if err != nil {
		return nil, false, fmt.Errorf("catalog: query convergence engine domains: %w", err)
	}
	var result []ConvergenceEngineDomain
	for rows.Next() {
		var record ConvergenceEngineDomain
		var fingerprint []byte
		var demanded, forced bool
		var pending, since, until int64
		if err := rows.Scan(
			&record.Domain, &record.Tenant, &record.Generation, &fingerprint,
			&record.CatalogRevision, &record.NotifiedCatalogRevision, &record.ObservedCatalogRevision,
			&record.Desired, &record.Notified, &record.Observed, &demanded, &forced,
			&pending, &since, &until,
		); err != nil {
			return nil, false, fmt.Errorf("catalog: scan convergence engine domain: %w", err)
		}
		if len(fingerprint) != len(record.Fingerprint) {
			return nil, false, fmt.Errorf("%w: corrupt convergence engine fingerprint", ErrIntegrity)
		}
		copy(record.Fingerprint[:], fingerprint)
		record.Demanded, record.Forced = demanded, forced
		record.PendingSent = timeFromUnixNano(pending)
		record.QuarantineSince = timeFromUnixNano(since)
		record.QuarantineUntil = timeFromUnixNano(until)
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, fmt.Errorf("catalog: read convergence engine domains: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, false, fmt.Errorf("catalog: close convergence engine domains: %w", err)
	}
	more := len(result) > ConvergenceEnginePageLimit
	if more {
		result = result[:ConvergenceEnginePageLimit]
	}
	if len(result) != 0 {
		if err := readConvergenceDomainChangesPage(ctx, query, result); err != nil {
			return nil, false, err
		}
	}
	return result, more, nil
}

func readConvergenceDomainChangesPage(
	ctx context.Context,
	query *sql.DB,
	domains []ConvergenceEngineDomain,
) error {
	rows, err := query.QueryContext(ctx, `
SELECT domain_id, slot, source_authority, source_revision, change_id, operation_id,
       cause, origin_domain, origin_generation
FROM convergence_engine_domain_changes
WHERE domain_id >= ? AND domain_id <= ?
ORDER BY domain_id, slot`, string(domains[0].Domain), string(domains[len(domains)-1].Domain))
	if err != nil {
		return fmt.Errorf("catalog: query convergence engine domain changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	byDomain := make(map[causal.DomainID]int, len(domains))
	for index := range domains {
		byDomain[domains[index].Domain] = index
	}
	for rows.Next() {
		var domainID causal.DomainID
		var slot uint8
		change, err := scanConvergenceEngineDomainChange(rows, &domainID, &slot)
		if err != nil {
			return err
		}
		index, ok := byDomain[domainID]
		if !ok {
			return fmt.Errorf("%w: convergence engine domain change escaped its page", ErrIntegrity)
		}
		domain := &domains[index]
		switch slot {
		case 1:
			domain.Applicable = change
		case 2:
			domain.DesiredChange = change
		case 3:
			domain.NotifiedChange = change
		case 4:
			domain.ObservedChange = change
		default:
			return fmt.Errorf("%w: invalid convergence engine domain change slot", ErrIntegrity)
		}
	}
	return rows.Err()
}

func scanConvergenceEngineDomainChange(
	scanner convergenceScanner,
	domain *causal.DomainID,
	slot *uint8,
) (causal.ChangeSet, error) {
	var authority, cause, origin string
	var revision, originGeneration uint64
	var changeID, operationID []byte
	if err := scanner.Scan(
		domain, slot, &authority, &revision, &changeID, &operationID,
		&cause, &origin, &originGeneration,
	); err != nil {
		return causal.ChangeSet{}, fmt.Errorf("catalog: scan convergence engine domain change: %w", err)
	}
	if len(changeID) != len(causal.ChangeID{}) || len(operationID) != len(causal.OperationID{}) {
		return causal.ChangeSet{}, fmt.Errorf("%w: corrupt convergence engine domain change identity", ErrIntegrity)
	}
	change := causal.ChangeSet{
		SourceAuthority: causal.SourceAuthorityID(authority), SourceRevision: causal.Revision(revision),
		Cause: causal.Cause(cause), Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(originGeneration),
	}
	copy(change.ChangeID[:], changeID)
	copy(change.OperationID[:], operationID)
	return change, nil
}

func pageConvergenceChanges(
	ctx context.Context,
	query *sql.DB,
	after causal.ChangeID,
) ([]ConvergenceEngineChange, bool, error) {
	rows, err := query.QueryContext(ctx, `
SELECT source_authority, source_revision, change_id, operation_id,
       cause, origin_domain, origin_generation, engine_revision,
       affected_count, affected_digest
FROM convergence_engine_changes WHERE change_id > ? ORDER BY change_id LIMIT ?`,
		after[:], ConvergenceEnginePageLimit+1)
	if err != nil {
		return nil, false, fmt.Errorf("catalog: query convergence engine changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []ConvergenceEngineChange
	for rows.Next() {
		var engineRevision causal.Revision
		var affectedCount uint64
		var affectedDigest []byte
		change, err := scanConvergenceEngineChange(rows, nil, &engineRevision, &affectedCount, &affectedDigest)
		if err != nil {
			return nil, false, err
		}
		if len(affectedDigest) != len(ConvergenceEngineChange{}.AffectedDigest) {
			return nil, false, fmt.Errorf("%w: corrupt convergence engine affected digest", ErrIntegrity)
		}
		record := ConvergenceEngineChange{
			Change: change, EngineRevision: engineRevision, AffectedCount: affectedCount,
		}
		copy(record.AffectedDigest[:], affectedDigest)
		result = append(result, record)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("catalog: read convergence engine changes: %w", err)
	}
	more := len(result) > ConvergenceEnginePageLimit
	if more {
		result = result[:ConvergenceEnginePageLimit]
	}
	return result, more, nil
}

type convergenceScanner interface {
	Scan(...any) error
}

func scanConvergenceEngineChange(
	scanner convergenceScanner,
	slot *uint8,
	engineRevision *causal.Revision,
	affectedCount *uint64,
	affectedDigest *[]byte,
) (causal.ChangeSet, error) {
	var authority, cause, origin string
	var revision, originGeneration uint64
	var changeID, operationID []byte
	destinations := []any{&authority, &revision, &changeID, &operationID, &cause, &origin, &originGeneration}
	if slot != nil {
		destinations = append([]any{slot}, destinations...)
	}
	if engineRevision != nil {
		destinations = append(destinations, engineRevision)
	}
	if affectedCount != nil {
		destinations = append(destinations, affectedCount, affectedDigest)
	}
	if err := scanner.Scan(destinations...); err != nil {
		return causal.ChangeSet{}, fmt.Errorf("catalog: scan convergence engine causal change: %w", err)
	}
	if len(changeID) != len(causal.ChangeID{}) || len(operationID) != len(causal.OperationID{}) {
		return causal.ChangeSet{}, fmt.Errorf("%w: corrupt convergence engine causal identity", ErrIntegrity)
	}
	change := causal.ChangeSet{
		SourceAuthority: causal.SourceAuthorityID(authority), SourceRevision: causal.Revision(revision),
		Cause: causal.Cause(cause), Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(originGeneration),
	}
	copy(change.ChangeID[:], changeID)
	copy(change.OperationID[:], operationID)
	return change, nil
}

func readConvergenceEngineOutbox(ctx context.Context, query *sql.DB) (*ConvergenceEngineOutbox, error) {
	var authority, cause, origin, afterKey, afterTenant string
	var revision, originGeneration, sequence, engineRevision, commitCount, affectedCount uint64
	var changeID, operationID, settlement, affectedDigest []byte
	err := query.QueryRowContext(ctx, `
SELECT source_authority, source_revision, change_id, operation_id,
       cause, origin_domain, origin_generation,
       cursor_sequence, cursor_after_key, cursor_after_tenant, settlement_digest,
       engine_revision, commit_count, affected_count, affected_digest
FROM convergence_engine_outbox WHERE singleton = 1`).Scan(
		&authority, &revision, &changeID, &operationID, &cause, &origin, &originGeneration,
		&sequence, &afterKey, &afterTenant, &settlement,
		&engineRevision, &commitCount, &affectedCount, &affectedDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: read convergence engine outbox: %w", err)
	}
	if len(changeID) != len(causal.ChangeID{}) || len(operationID) != len(causal.OperationID{}) ||
		(len(settlement) != 0 && len(settlement) != len(causal.OutboxSettlement{}.Digest)) ||
		len(affectedDigest) != len(ConvergenceEngineOutbox{}.AffectedDigest) {
		return nil, fmt.Errorf("%w: corrupt convergence engine outbox", ErrIntegrity)
	}
	result := &ConvergenceEngineOutbox{
		Change: causal.ChangeSet{
			SourceAuthority: causal.SourceAuthorityID(authority), SourceRevision: causal.Revision(revision),
			Cause: causal.Cause(cause), Origin: causal.DomainID(origin), OriginGeneration: causal.Generation(originGeneration),
		},
		Cursor: causal.OutboxCursor{
			Sequence: sequence, AfterKey: causal.LogicalKey(afterKey), AfterTenant: causal.TenantID(afterTenant),
		},
		EngineRevision: causal.Revision(engineRevision), CommitCount: commitCount, AffectedCount: affectedCount,
	}
	copy(result.AffectedDigest[:], affectedDigest)
	copy(result.Change.ChangeID[:], changeID)
	copy(result.Change.OperationID[:], operationID)
	if len(settlement) != 0 {
		proof := causal.OutboxSettlement{ChangeID: result.Change.ChangeID}
		copy(proof.Digest[:], settlement)
		result.Settlement = &proof
	}
	return result, nil
}
