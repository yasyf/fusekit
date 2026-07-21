package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE fusekit_schema (
    component TEXT PRIMARY KEY,
    digest TEXT NOT NULL
);

CREATE TABLE convergence_engine (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL CHECK (version >= 0),
    revision INTEGER NOT NULL CHECK (revision >= 0)
) STRICT;
INSERT INTO convergence_engine(singleton, version, revision) VALUES (1, 0, 0);

CREATE TABLE convergence_engine_heads (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    head INTEGER NOT NULL CHECK (head > 0),
    dedup_floor INTEGER NOT NULL CHECK (dedup_floor >= 0 AND dedup_floor <= head)
) STRICT;

CREATE TABLE convergence_engine_domains (
    domain_id TEXT PRIMARY KEY CHECK (length(domain_id) > 0),
    tenant TEXT NOT NULL CHECK (length(tenant) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    fingerprint BLOB NOT NULL CHECK (length(fingerprint) = 32),
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    notified_catalog_revision INTEGER NOT NULL CHECK (notified_catalog_revision >= 0),
    observed_catalog_revision INTEGER NOT NULL CHECK (observed_catalog_revision >= 0),
    desired INTEGER NOT NULL CHECK (desired >= 0),
    notified INTEGER NOT NULL CHECK (notified >= 0 AND notified <= desired),
    observed INTEGER NOT NULL CHECK (observed >= 0 AND observed <= desired),
    demanded INTEGER NOT NULL CHECK (demanded IN (0, 1)),
    forced INTEGER NOT NULL CHECK (forced IN (0, 1)),
    pending_sent_unix_nano INTEGER NOT NULL CHECK (pending_sent_unix_nano >= 0),
    quarantine_since_unix_nano INTEGER NOT NULL CHECK (quarantine_since_unix_nano >= 0),
    quarantine_until_unix_nano INTEGER NOT NULL CHECK (quarantine_until_unix_nano >= 0),
    CHECK ((quarantine_since_unix_nano = 0 AND quarantine_until_unix_nano = 0)
        OR quarantine_until_unix_nano > quarantine_since_unix_nano)
) STRICT;
CREATE INDEX convergence_engine_domains_demanded_observed
    ON convergence_engine_domains(tenant, observed_catalog_revision)
    WHERE demanded = 1;

CREATE TABLE convergence_engine_domain_changes (
    domain_id TEXT NOT NULL REFERENCES convergence_engine_domains(domain_id) ON DELETE CASCADE,
    slot INTEGER NOT NULL CHECK (slot BETWEEN 1 AND 4),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision >= 0),
    change_id BLOB NOT NULL CHECK (length(change_id) = 16),
    operation_id BLOB NOT NULL CHECK (length(operation_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap', 'on_demand')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    PRIMARY KEY (domain_id, slot)
) STRICT;

CREATE TABLE convergence_engine_changes (
    change_id BLOB PRIMARY KEY CHECK (length(change_id) = 16),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    operation_id BLOB NOT NULL CHECK (length(operation_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap', 'on_demand')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    engine_revision INTEGER NOT NULL CHECK (engine_revision >= 0),
    affected_count INTEGER NOT NULL CHECK (affected_count > 0),
    affected_digest BLOB NOT NULL CHECK (length(affected_digest) = 32)
) STRICT;

CREATE TABLE convergence_engine_outbox (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    change_id BLOB NOT NULL CHECK (length(change_id) = 16),
    operation_id BLOB NOT NULL CHECK (length(operation_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap', 'on_demand')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    cursor_sequence INTEGER NOT NULL CHECK (cursor_sequence >= 0),
    cursor_after_key TEXT NOT NULL,
    cursor_after_tenant TEXT NOT NULL,
    settlement_digest BLOB NOT NULL CHECK (length(settlement_digest) IN (0, 32)),
    engine_revision INTEGER NOT NULL CHECK (engine_revision > 0),
    commit_count INTEGER NOT NULL CHECK (commit_count >= 0),
    affected_count INTEGER NOT NULL CHECK (affected_count >= 0),
    affected_digest BLOB NOT NULL CHECK (length(affected_digest) = 32)
) STRICT;

CREATE TABLE convergence_engine_mutations (
    operation_id BLOB PRIMARY KEY CHECK (length(operation_id) = 16),
    expected_version INTEGER NOT NULL CHECK (expected_version >= 0),
    target_revision INTEGER NOT NULL CHECK (target_revision >= 0),
    page_count INTEGER NOT NULL CHECK (page_count BETWEEN 1 AND 65536),
    state INTEGER NOT NULL CHECK (state IN (1, 2))
) STRICT;

CREATE TABLE convergence_engine_mutation_pages (
    operation_id BLOB NOT NULL REFERENCES convergence_engine_mutations(operation_id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    digest BLOB NOT NULL CHECK (length(digest) = 32),
    payload BLOB NOT NULL,
    PRIMARY KEY (operation_id, sequence)
) STRICT;

CREATE TABLE convergence_source (
	source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    head INTEGER NOT NULL CHECK (head >= 0)
);

CREATE TABLE convergence_changes (
    change_id BLOB PRIMARY KEY CHECK (length(change_id) = 16),
    source_operation_id BLOB NOT NULL UNIQUE CHECK (length(source_operation_id) = 16),
	source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap', 'on_demand')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    outbox_state INTEGER NOT NULL DEFAULT 1 CHECK (outbox_state BETWEEN 1 AND 3),
    CHECK ((cause IN ('provider_mutation', 'on_demand') AND length(origin_domain) > 0 AND origin_generation > 0)
        OR (cause NOT IN ('provider_mutation', 'on_demand') AND origin_domain = '' AND origin_generation = 0))
	, UNIQUE (source_authority, source_revision)
);
CREATE INDEX convergence_changes_unsettled
    ON convergence_changes(outbox_state);

CREATE TABLE convergence_change_affected (
    change_id BLOB NOT NULL REFERENCES convergence_changes(change_id) ON DELETE CASCADE,
    affected_key TEXT NOT NULL CHECK (length(affected_key) > 0),
    PRIMARY KEY (change_id, affected_key)
);

CREATE TABLE convergence_change_targets (
    change_id BLOB NOT NULL REFERENCES convergence_changes(change_id) ON DELETE CASCADE,
    tenant TEXT NOT NULL,
    PRIMARY KEY (change_id, tenant)
);

CREATE TABLE convergence_outbox (
    catalog_operation_id BLOB PRIMARY KEY CHECK (length(catalog_operation_id) = 32),
    change_id BLOB NOT NULL REFERENCES convergence_changes(change_id),
    tenant TEXT NOT NULL,
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    file_provider_fingerprint BLOB NOT NULL CHECK (length(file_provider_fingerprint) = 32),
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 3),
    UNIQUE (change_id, tenant),
    UNIQUE (tenant, catalog_revision)
);
CREATE TABLE convergence_outbox_claims (
    change_id BLOB PRIMARY KEY REFERENCES convergence_changes(change_id) ON DELETE CASCADE,
    state INTEGER NOT NULL CHECK (state IN (1, 2)),
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
    after_key TEXT NOT NULL,
    after_tenant TEXT NOT NULL,
    last_valid INTEGER NOT NULL CHECK (last_valid IN (0, 1)),
    last_before_key TEXT NOT NULL,
    last_before_tenant TEXT NOT NULL,
    settlement_digest BLOB NOT NULL CHECK (length(settlement_digest) IN (0, 32)),
    CHECK ((state = 1 AND length(settlement_digest) IN (0, 32))
        OR (state = 2 AND length(settlement_digest) = 32)),
    CHECK (last_valid = 1 OR (
        next_sequence = 0 AND after_key = '' AND after_tenant = ''
        AND last_before_key = '' AND last_before_tenant = ''
    ))
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant TEXT PRIMARY KEY,
    root_id BLOB NOT NULL UNIQUE CHECK (length(root_id) = 16),
    case_policy INTEGER NOT NULL CHECK (case_policy IN (1, 2)),
	 presentation_set INTEGER NOT NULL CHECK (presentation_set BETWEEN 1 AND 3),
    head INTEGER NOT NULL CHECK (head > 0),
    floor INTEGER NOT NULL CHECK (floor >= 0 AND floor <= head)
);

CREATE TABLE catalog_maintenance_sequence (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_ticket INTEGER NOT NULL CHECK (next_ticket >= 0)
) STRICT;
INSERT INTO catalog_maintenance_sequence(singleton, next_ticket) VALUES (1, 0);

CREATE TABLE catalog_global_maintenance (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_phase INTEGER NOT NULL CHECK (next_phase BETWEEN 1 AND 7)
) STRICT;
INSERT INTO catalog_global_maintenance(singleton, next_phase) VALUES (1, 1);

CREATE TABLE catalog_maintenance (
    tenant TEXT PRIMARY KEY REFERENCES tenants(tenant) ON DELETE CASCADE,
    dirty_revision INTEGER NOT NULL CHECK (dirty_revision > 0),
    running_revision INTEGER NOT NULL DEFAULT 0
        CHECK (running_revision >= 0 AND running_revision <= dirty_revision),
    ticket INTEGER NOT NULL UNIQUE CHECK (ticket > 0),
    next_phase INTEGER NOT NULL DEFAULT 1 CHECK (next_phase BETWEEN 1 AND 4)
) STRICT;
CREATE INDEX catalog_maintenance_queue
    ON catalog_maintenance(ticket, tenant);
CREATE INDEX catalog_maintenance_running
    ON catalog_maintenance(ticket, tenant)
    WHERE running_revision <> 0;

CREATE TABLE catalog_generations (
    owner_id BLOB PRIMARY KEY CHECK (length(owner_id) = 16),
    retired INTEGER NOT NULL CHECK (retired IN (0, 1))
) STRICT;
CREATE INDEX catalog_generations_retired
    ON catalog_generations(retired, owner_id);

CREATE TABLE storage_accounting (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    temporary_bytes INTEGER NOT NULL CHECK (temporary_bytes >= 0),
    published_bytes INTEGER NOT NULL CHECK (published_bytes >= 0),
    version INTEGER NOT NULL CHECK (version >= 0)
) STRICT;
INSERT INTO storage_accounting(
    singleton, temporary_bytes, published_bytes, version
) VALUES (1, 0, 0, 0);

CREATE TABLE storage_entries (
    name TEXT PRIMARY KEY CHECK (length(name) > 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    state INTEGER NOT NULL CHECK (state IN (1, 2)),
    size INTEGER NOT NULL CHECK (size >= 0),
    stage_id BLOB UNIQUE CHECK (stage_id IS NULL OR length(stage_id) = 16),
    hash BLOB UNIQUE CHECK (hash IS NULL OR length(hash) = 32),
    owner_id BLOB CHECK (owner_id IS NULL OR length(owner_id) = 16),
    CHECK (
        (kind = 1 AND stage_id IS NOT NULL AND owner_id IS NOT NULL AND hash IS NULL)
        OR
        (kind = 2 AND state = 2 AND stage_id IS NULL AND owner_id IS NULL AND hash IS NOT NULL)
    ),
    FOREIGN KEY (owner_id) REFERENCES catalog_generations(owner_id)
) STRICT;
CREATE INDEX storage_entries_owner_state
    ON storage_entries(kind, state, owner_id, name);
CREATE INDEX storage_entries_generation
    ON storage_entries(owner_id, name)
    WHERE owner_id IS NOT NULL;

CREATE TABLE storage_transitions (
    transition_id BLOB PRIMARY KEY CHECK (length(transition_id) = 16),
    kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 4),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    stage_id BLOB CHECK (stage_id IS NULL OR length(stage_id) = 16),
    source_name TEXT NOT NULL CHECK (length(source_name) > 0),
    target_name TEXT NOT NULL DEFAULT '',
    hash BLOB CHECK (hash IS NULL OR length(hash) = 32),
    size INTEGER NOT NULL CHECK (size >= 0),
    new_blob INTEGER NOT NULL CHECK (new_blob IN (0, 1)),
    quarantined INTEGER NOT NULL DEFAULT 0 CHECK (quarantined IN (0, 1)),
    reason TEXT NOT NULL DEFAULT '' CHECK (length(reason) <= 4096),
    CHECK (
        (quarantined = 0 AND reason = '')
        OR (quarantined = 1 AND length(reason) > 0)
    ),
    CHECK (
        (kind = 1
         AND stage_id IS NOT NULL
         AND target_name = ''
         AND hash IS NULL
         AND new_blob = 0)
        OR
        (kind = 2
         AND stage_id IS NOT NULL
         AND length(target_name) > 0
         AND hash IS NOT NULL)
        OR
        (kind = 3
         AND stage_id IS NOT NULL
         AND target_name = ''
         AND hash IS NULL
         AND new_blob = 0)
        OR
        (kind = 4
         AND stage_id IS NULL
         AND target_name = ''
         AND hash IS NOT NULL
         AND new_blob = 0)
    ),
    FOREIGN KEY (owner_id) REFERENCES catalog_generations(owner_id)
) STRICT;
CREATE INDEX storage_transitions_pending
    ON storage_transitions(quarantined, transition_id);
CREATE INDEX storage_transitions_owner
    ON storage_transitions(owner_id, quarantined, transition_id);
CREATE UNIQUE INDEX storage_transitions_stage
    ON storage_transitions(stage_id)
    WHERE stage_id IS NOT NULL;
CREATE UNIQUE INDEX storage_transitions_published_delete
    ON storage_transitions(hash)
    WHERE kind = 4;
CREATE UNIQUE INDEX storage_transitions_new_blob_target
    ON storage_transitions(target_name)
    WHERE kind = 2 AND new_blob = 1;
CREATE TRIGGER storage_transition_identity_immutable
BEFORE UPDATE OF
    transition_id, kind, owner_id, stage_id,
    source_name, target_name, hash, new_blob
ON storage_transitions
WHEN NEW.transition_id IS NOT OLD.transition_id
  OR NEW.kind IS NOT OLD.kind
  OR NEW.owner_id IS NOT OLD.owner_id
  OR NEW.stage_id IS NOT OLD.stage_id
  OR NEW.source_name IS NOT OLD.source_name
  OR NEW.target_name IS NOT OLD.target_name
  OR NEW.hash IS NOT OLD.hash
  OR NEW.new_blob IS NOT OLD.new_blob
BEGIN
    SELECT RAISE(ABORT, 'storage transition identity is immutable');
END;
CREATE TRIGGER storage_transition_size_monotonic
BEFORE UPDATE OF size ON storage_transitions
WHEN NEW.size IS NOT OLD.size
 AND NOT (
     OLD.kind = 1
     AND OLD.quarantined = 0
     AND NEW.quarantined = 0
     AND NEW.size > OLD.size
 )
BEGIN
    SELECT RAISE(
        ABORT, 'storage transition size may only grow while creating temporary content'
    );
END;

CREATE TABLE storage_quarantine_resolutions (
    transition_id BLOB PRIMARY KEY CHECK (length(transition_id) = 16),
    token BLOB NOT NULL CHECK (length(token) = 32),
    resolution INTEGER NOT NULL CHECK (resolution IN (1, 2)),
    state INTEGER NOT NULL CHECK (state IN (1, 2)),
    outcome_digest BLOB NOT NULL CHECK (length(outcome_digest) = 32)
) STRICT;
CREATE INDEX storage_quarantine_resolutions_state
    ON storage_quarantine_resolutions(state, transition_id);
CREATE TRIGGER storage_quarantine_resolution_identity_immutable
BEFORE UPDATE OF transition_id, token, resolution, outcome_digest
ON storage_quarantine_resolutions
WHEN NEW.transition_id IS NOT OLD.transition_id
  OR NEW.token IS NOT OLD.token
  OR NEW.resolution IS NOT OLD.resolution
  OR NEW.outcome_digest IS NOT OLD.outcome_digest
BEGIN
    SELECT RAISE(ABORT, 'storage quarantine resolution is immutable');
END;
CREATE TRIGGER storage_quarantine_resolution_state_monotonic
BEFORE UPDATE OF state ON storage_quarantine_resolutions
WHEN NEW.state IS NOT OLD.state
 AND NOT (OLD.state = 1 AND NEW.state = 2)
BEGIN
    SELECT RAISE(
        ABORT, 'storage quarantine resolution state must advance once'
    );
END;

CREATE TABLE retention_owners (
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    session_owner TEXT NOT NULL CHECK (length(session_owner) BETWEEN 1 AND 256),
    retired INTEGER NOT NULL CHECK (retired IN (0, 1)),
    PRIMARY KEY (owner_id, session_owner),
    FOREIGN KEY (owner_id) REFERENCES catalog_generations(owner_id)
) STRICT;
CREATE INDEX retention_owners_retired
    ON retention_owners(retired, owner_id, session_owner);

CREATE TRIGGER catalog_maintenance_tenant_insert
AFTER INSERT ON tenants
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    VALUES (
        NEW.tenant, NEW.head, 0,
        COALESCE(
            (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
            (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
        )
    );
END;

CREATE TRIGGER catalog_maintenance_tenant_advance
AFTER UPDATE OF head ON tenants
WHEN NEW.head > OLD.head
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    VALUES (
        NEW.tenant, NEW.head, 0,
        COALESCE(
            (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
            (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
        )
    )
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TABLE desired_tenants (
    tenant TEXT PRIMARY KEY REFERENCES tenants(tenant),
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    presentation_root TEXT NOT NULL CHECK (length(presentation_root) > 0),
    backing_root TEXT NOT NULL CHECK (length(backing_root) > 0),
    content_source_id TEXT NOT NULL CHECK (length(content_source_id) > 0),
	file_provider_account_id TEXT NOT NULL,
	file_provider_display_name TEXT NOT NULL,
    access_mode INTEGER NOT NULL CHECK (access_mode IN (1, 2)),
	generation INTEGER NOT NULL CHECK (generation > 0),
	CHECK ((file_provider_account_id = '' AND file_provider_display_name = '')
	    OR (length(file_provider_account_id) > 0 AND length(file_provider_display_name) > 0))
);

CREATE TABLE desired_topology_heads (
    owner_id TEXT PRIMARY KEY CHECK (length(owner_id) > 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    floor INTEGER NOT NULL CHECK (floor > 0 AND floor <= revision),
    tenant_count INTEGER NOT NULL CHECK (tenant_count BETWEEN 0 AND 1000000)
) STRICT;
CREATE TRIGGER desired_topology_heads_monotonic
BEFORE UPDATE OF owner_id, revision, floor, tenant_count ON desired_topology_heads
WHEN NEW.owner_id <> OLD.owner_id
  OR NEW.revision <> OLD.revision + 1
  OR NEW.floor < OLD.floor
  OR NEW.floor > NEW.revision
BEGIN
    SELECT RAISE(ABORT, 'desired topology head must advance exactly once');
END;

CREATE TABLE desired_topology_changes (
    owner_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    tenant TEXT NOT NULL,
    fleet_generation INTEGER NOT NULL CHECK (fleet_generation >= 0),
    PRIMARY KEY (owner_id, revision),
    FOREIGN KEY (owner_id) REFERENCES desired_topology_heads(owner_id) ON DELETE CASCADE,
    CHECK ((kind = 1 AND length(tenant) > 0 AND fleet_generation = 0)
        OR (kind = 2 AND tenant = '' AND fleet_generation > 0))
) STRICT;
CREATE INDEX desired_topology_changes_page
    ON desired_topology_changes(owner_id, revision);

CREATE TABLE source_driver_target_epochs (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    target_epoch INTEGER NOT NULL CHECK (target_epoch > 0)
) STRICT;

CREATE TRIGGER source_driver_target_epoch_insert
AFTER INSERT ON desired_tenants
BEGIN
    INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
    VALUES (NEW.content_source_id, 1)
    ON CONFLICT(source_authority) DO UPDATE SET target_epoch = target_epoch + 1;
END;

CREATE TRIGGER source_driver_target_epoch_delete
AFTER DELETE ON desired_tenants
BEGIN
    INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
    VALUES (OLD.content_source_id, 1)
    ON CONFLICT(source_authority) DO UPDATE SET target_epoch = target_epoch + 1;
END;

CREATE TRIGGER source_driver_target_epoch_update
AFTER UPDATE OF content_source_id, generation ON desired_tenants
WHEN OLD.content_source_id <> NEW.content_source_id OR OLD.generation <> NEW.generation
BEGIN
    INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
    VALUES (OLD.content_source_id, 1)
    ON CONFLICT(source_authority) DO UPDATE SET target_epoch = target_epoch + 1;
    INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
    SELECT NEW.content_source_id, 1
    WHERE NEW.content_source_id <> OLD.content_source_id
    ON CONFLICT(source_authority) DO UPDATE SET target_epoch = target_epoch + 1;
END;

CREATE TABLE tenant_provision_removals (
    tenant TEXT PRIMARY KEY,
    generation INTEGER NOT NULL CHECK (generation > 0)
) STRICT;

CREATE TABLE file_provider_domains (
	domain_id TEXT PRIMARY KEY CHECK (length(domain_id) > 0),
	tenant TEXT NOT NULL UNIQUE REFERENCES tenants(tenant),
	owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
	generation INTEGER NOT NULL CHECK (generation > 0),
	root_id BLOB NOT NULL CHECK (length(root_id) = 16),
	access_mode INTEGER NOT NULL CHECK (access_mode IN (1, 2)),
	account_instance_id TEXT NOT NULL CHECK (length(account_instance_id) > 0),
	display_name TEXT NOT NULL CHECK (length(display_name) > 0),
	public_path TEXT NOT NULL,
	registered INTEGER NOT NULL CHECK (registered IN (0, 1))
);

CREATE TABLE file_provider_domain_removals (
	domain_id TEXT PRIMARY KEY CHECK (length(domain_id) > 0),
	tenant TEXT NOT NULL UNIQUE REFERENCES tenants(tenant),
	owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
	generation INTEGER NOT NULL CHECK (generation > 0),
	root_id BLOB NOT NULL CHECK (length(root_id) = 16),
	access_mode INTEGER NOT NULL CHECK (access_mode IN (1, 2)),
	account_instance_id TEXT NOT NULL CHECK (length(account_instance_id) > 0),
	display_name TEXT NOT NULL CHECK (length(display_name) > 0),
	confirmed_absent INTEGER NOT NULL CHECK (confirmed_absent IN (0, 1))
);

CREATE TABLE file_provider_leases (
	lease_id TEXT PRIMARY KEY CHECK (length(lease_id) > 0),
	tenant TEXT NOT NULL REFERENCES tenants(tenant),
	domain_id TEXT NOT NULL,
	generation INTEGER NOT NULL CHECK (generation > 0),
	expires_unix_nano INTEGER NOT NULL CHECK (expires_unix_nano > 0)
);
CREATE INDEX file_provider_leases_live
	ON file_provider_leases(tenant, domain_id, generation, expires_unix_nano);
CREATE INDEX file_provider_leases_expired
    ON file_provider_leases(expires_unix_nano, lease_id);
CREATE INDEX file_provider_leases_tenant_expiry
    ON file_provider_leases(tenant, expires_unix_nano, domain_id, generation);

CREATE TABLE broker_sequence (
	singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
	last_command_id INTEGER NOT NULL CHECK (last_command_id >= 0)
);
INSERT INTO broker_sequence(singleton, last_command_id) VALUES (1, 0);

CREATE TABLE broker_command_attempts (
	attempt_id BLOB PRIMARY KEY CHECK (length(attempt_id) = 16),
	command_id INTEGER NOT NULL UNIQUE CHECK (command_id > 0),
	process_pid INTEGER NOT NULL CHECK (process_pid > 1),
	process_start_time TEXT NOT NULL CHECK (length(process_start_time) > 0),
	process_boot TEXT NOT NULL CHECK (length(process_boot) > 0),
	process_generation TEXT NOT NULL CHECK (length(process_generation) > 0),
	command_kind TEXT NOT NULL CHECK (length(command_kind) > 0),
	payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
	domain_id TEXT NOT NULL,
	revision INTEGER NOT NULL CHECK (revision >= 0),
	state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 4),
	created_unix_nano INTEGER NOT NULL CHECK (created_unix_nano > 0),
	settled_unix_nano INTEGER NOT NULL CHECK (settled_unix_nano >= 0),
	CHECK ((state IN (1, 2) AND settled_unix_nano = 0)
	    OR (state IN (3, 4) AND settled_unix_nano > 0)),
	CHECK ((command_kind = 'signal_domain' AND length(domain_id) > 0 AND revision > 0)
	    OR (command_kind <> 'signal_domain' AND domain_id = '' AND revision = 0))
) STRICT;
CREATE UNIQUE INDEX broker_signal_attempt_once
	ON broker_command_attempts(domain_id, revision)
	WHERE command_kind = 'signal_domain';
CREATE INDEX broker_command_attempts_process
	ON broker_command_attempts(process_pid, process_start_time, process_boot, process_generation);
CREATE INDEX broker_command_attempts_state
    ON broker_command_attempts(state, command_id);
CREATE INDEX broker_command_attempts_terminal_cleanup
    ON broker_command_attempts(
        process_pid, process_start_time, process_boot, process_generation,
        command_id, attempt_id
    )
    WHERE command_kind <> 'signal_domain' AND state = 3;
CREATE TABLE broker_signal_watermarks (
	domain_id TEXT PRIMARY KEY CHECK (length(domain_id) > 0),
	revision INTEGER NOT NULL CHECK (revision > 0),
	attempt_id BLOB NOT NULL UNIQUE CHECK (length(attempt_id) = 16)
) STRICT;

CREATE TABLE source_watermarks (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16)
);

CREATE TABLE source_operations (
    operation_id BLOB PRIMARY KEY CHECK (length(operation_id) = 16),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    mode INTEGER NOT NULL CHECK (mode IN (1, 2)),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    result_json BLOB NOT NULL,
    UNIQUE (source_authority, source_revision)
);

CREATE TABLE source_object_ids (
    source_authority TEXT NOT NULL,
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
	PRIMARY KEY (source_authority, source_key),
	UNIQUE (source_authority, object_id)
);
CREATE INDEX source_object_ids_object_gc
    ON source_object_ids(object_id, source_authority, source_key);

CREATE TABLE source_object_bindings (
	source_authority TEXT NOT NULL,
	tenant TEXT NOT NULL REFERENCES tenants(tenant),
	source_key TEXT NOT NULL,
	PRIMARY KEY (source_authority, tenant, source_key),
	FOREIGN KEY (source_authority, source_key) REFERENCES source_object_ids(source_authority, source_key)
);

CREATE TABLE source_tenant_roots (
	source_authority TEXT NOT NULL,
	tenant TEXT NOT NULL REFERENCES tenants(tenant),
	root_key TEXT NOT NULL CHECK (length(root_key) > 0),
	PRIMARY KEY (source_authority, tenant),
	UNIQUE (source_authority, root_key)
);

CREATE TABLE source_key_reservations (
	source_authority TEXT NOT NULL,
	source_key TEXT NOT NULL CHECK (length(source_key) > 0),
	mutation_id BLOB NOT NULL UNIQUE CHECK (length(mutation_id) = 32),
	PRIMARY KEY (source_authority, source_key)
);

CREATE TABLE source_commits (
    catalog_operation_id BLOB PRIMARY KEY CHECK (length(catalog_operation_id) = 32),
    source_operation_id BLOB NOT NULL REFERENCES source_operations(operation_id),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    catalog_fingerprint BLOB NOT NULL CHECK (length(catalog_fingerprint) = 32),
    file_provider_fingerprint BLOB NOT NULL CHECK (length(file_provider_fingerprint) = 32),
    UNIQUE (source_operation_id, tenant),
    UNIQUE (tenant, catalog_revision)
);

CREATE TABLE source_tenant_targets (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    change_id BLOB NOT NULL REFERENCES convergence_changes(change_id),
    source_operation_id BLOB NOT NULL REFERENCES source_operations(operation_id),
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    catalog_fingerprint BLOB NOT NULL CHECK (length(catalog_fingerprint) = 32),
    file_provider_fingerprint BLOB NOT NULL CHECK (length(file_provider_fingerprint) = 32),
    PRIMARY KEY (source_authority, tenant)
);

CREATE TABLE source_observer_roots (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    root_id TEXT NOT NULL CHECK (length(root_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    path TEXT NOT NULL CHECK (length(path) > 0),
    volume_uuid TEXT NOT NULL CHECK (length(volume_uuid) > 0),
    root_inode INTEGER NOT NULL CHECK (root_inode > 0),
    root_birthtime_sec INTEGER NOT NULL,
    root_birthtime_nsec INTEGER NOT NULL CHECK (root_birthtime_nsec BETWEEN 0 AND 999999999),
    root_kind INTEGER NOT NULL CHECK (root_kind IN (1, 2)),
    root_set_digest BLOB NOT NULL CHECK (length(root_set_digest) = 32),
    PRIMARY KEY (source_authority, root_id),
    UNIQUE (source_authority, path)
);

CREATE TABLE source_authority_fleets (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    authority_count INTEGER NOT NULL CHECK (authority_count BETWEEN 0 AND 10000),
    authorities_digest BLOB NOT NULL CHECK (length(authorities_digest) = 32),
    declarations_digest BLOB NOT NULL CHECK (length(declarations_digest) = 32),
    acknowledgement_digest BLOB NOT NULL CHECK (length(acknowledgement_digest) = 32),
    PRIMARY KEY (owner_id, generation)
) STRICT;

CREATE TABLE source_authority_desired_fleets (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    authority_count INTEGER NOT NULL CHECK (authority_count BETWEEN 0 AND 10000),
    authorities_digest BLOB NOT NULL CHECK (length(authorities_digest) = 32),
    declarations_digest BLOB NOT NULL CHECK (length(declarations_digest) = 32),
    PRIMARY KEY (owner_id, generation)
) STRICT;
CREATE TRIGGER source_authority_desired_fleets_immutable
BEFORE UPDATE ON source_authority_desired_fleets
BEGIN
    SELECT RAISE(ABORT, 'desired source authority fleet generations are immutable');
END;

CREATE TABLE source_authority_desired_fleet_heads (
    owner_id TEXT PRIMARY KEY CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_desired_fleets(owner_id, generation)
) STRICT;
CREATE TRIGGER source_authority_desired_fleet_heads_monotonic
BEFORE UPDATE OF owner_id, generation ON source_authority_desired_fleet_heads
WHEN NEW.owner_id <> OLD.owner_id OR NEW.generation <= OLD.generation
BEGIN
    SELECT RAISE(ABORT, 'desired source authority fleet head must advance for one owner');
END;

CREATE TABLE source_authority_desired_fleet_members (
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    driver_config BLOB NOT NULL CHECK (length(driver_config) <= 65536),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    PRIMARY KEY (owner_id, generation, source_authority),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_desired_fleets(owner_id, generation)
        ON DELETE CASCADE
) STRICT;
CREATE INDEX source_authority_desired_fleet_members_page
    ON source_authority_desired_fleet_members(
        owner_id, generation, source_authority, declaration_digest
    );
CREATE TRIGGER source_authority_desired_fleet_members_immutable
BEFORE UPDATE ON source_authority_desired_fleet_members
BEGIN
    SELECT RAISE(ABORT, 'desired source authority fleet members are immutable');
END;

CREATE TRIGGER source_authority_fleets_immutable
BEFORE UPDATE ON source_authority_fleets
BEGIN
    SELECT RAISE(ABORT, 'source authority fleet generations are immutable');
END;

CREATE TABLE source_authority_fleet_heads (
    owner_id TEXT PRIMARY KEY CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_fleets(owner_id, generation)
) STRICT;
CREATE TRIGGER source_authority_fleet_heads_monotonic
BEFORE UPDATE OF owner_id, generation ON source_authority_fleet_heads
WHEN NEW.owner_id <> OLD.owner_id OR NEW.generation <= OLD.generation
BEGIN
    SELECT RAISE(
        ABORT, 'source authority fleet head must advance for one owner'
    );
END;

CREATE TABLE source_authority_fleet_members (
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    driver_config BLOB NOT NULL CHECK (length(driver_config) <= 65536),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    runtime_epoch BLOB NOT NULL DEFAULT X''
        CHECK (length(runtime_epoch) IN (0, 16)),
    runtime_owner_json BLOB CHECK (
        runtime_owner_json IS NULL
        OR length(runtime_owner_json) BETWEEN 1 AND 16384
    ),
    runtime_owner_digest BLOB CHECK (
        runtime_owner_digest IS NULL
        OR length(runtime_owner_digest) = 32
    ),
    runtime_closed INTEGER NOT NULL DEFAULT 1 CHECK (runtime_closed IN (0, 1)),
    CHECK (
        (length(runtime_epoch) = 0
         AND runtime_owner_json IS NULL
         AND runtime_owner_digest IS NULL
         AND runtime_closed = 1)
        OR
        (length(runtime_epoch) = 16
         AND runtime_owner_json IS NOT NULL
         AND runtime_owner_digest IS NOT NULL)
    ),
    PRIMARY KEY (owner_id, generation, source_authority),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_fleets(owner_id, generation)
        ON DELETE CASCADE
) STRICT;
CREATE INDEX source_authority_fleet_members_page
    ON source_authority_fleet_members(
        owner_id, generation, source_authority,
        declaration_digest, runtime_epoch, runtime_closed
    );
CREATE UNIQUE INDEX source_authority_fleet_members_declaration
    ON source_authority_fleet_members(
        owner_id, generation, source_authority, declaration_digest
    );
CREATE INDEX source_authority_fleet_members_authority
    ON source_authority_fleet_members(source_authority, owner_id, generation);
CREATE INDEX source_authority_fleet_members_runtime_owner
    ON source_authority_fleet_members(
        runtime_owner_digest, owner_id, generation,
        source_authority, runtime_closed
    )
    WHERE runtime_owner_digest IS NOT NULL;
CREATE TRIGGER source_authority_fleet_member_declaration_immutable
BEFORE UPDATE OF owner_id, generation, source_authority, driver_id, driver_config, declaration_digest
ON source_authority_fleet_members
BEGIN
    SELECT RAISE(ABORT, 'source authority fleet member declaration is immutable');
END;
CREATE TRIGGER source_authority_runtime_owner_fenced
BEFORE UPDATE OF runtime_epoch, runtime_owner_json, runtime_owner_digest
ON source_authority_fleet_members
WHEN NEW.runtime_epoch IS NOT OLD.runtime_epoch
  OR NEW.runtime_owner_json IS NOT OLD.runtime_owner_json
  OR NEW.runtime_owner_digest IS NOT OLD.runtime_owner_digest
BEGIN
    SELECT CASE WHEN NOT (
        OLD.runtime_closed = 1
        AND NEW.runtime_closed = 0
        AND length(NEW.runtime_epoch) = 16
        AND NEW.runtime_epoch IS NOT OLD.runtime_epoch
        AND NEW.runtime_owner_json IS NOT NULL
        AND NEW.runtime_owner_digest IS NOT NULL
    ) THEN RAISE(
        ABORT, 'source authority runtime owner transition is not fenced'
    ) END;
END;
CREATE TRIGGER source_authority_runtime_state_fenced
BEFORE UPDATE OF runtime_closed ON source_authority_fleet_members
WHEN NEW.runtime_closed IS NOT OLD.runtime_closed
BEGIN
    SELECT CASE WHEN NOT (
        (OLD.runtime_closed = 0 AND NEW.runtime_closed = 1)
        OR
        (
            OLD.runtime_closed = 1
            AND NEW.runtime_closed = 0
            AND length(NEW.runtime_epoch) = 16
            AND NEW.runtime_epoch IS NOT OLD.runtime_epoch
            AND NEW.runtime_owner_json IS NOT NULL
            AND NEW.runtime_owner_digest IS NOT NULL
        )
    ) THEN RAISE(
        ABORT, 'source authority runtime state transition is not fenced'
    ) END;
END;

CREATE TABLE source_authority_runtime_recovery_receipts (
    receipt_digest BLOB PRIMARY KEY CHECK (length(receipt_digest) = 32),
    ledger_id BLOB NOT NULL CHECK (length(ledger_id) = 16),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    receipt_json BLOB NOT NULL CHECK (length(receipt_json) BETWEEN 1 AND 32768),
    runtime_owner_json BLOB NOT NULL CHECK (length(runtime_owner_json) BETWEEN 1 AND 16384),
    runtime_owner_digest BLOB NOT NULL CHECK (length(runtime_owner_digest) = 32),
    closed_count INTEGER NOT NULL CHECK (closed_count BETWEEN 0 AND 10000),
    closed_digest BLOB NOT NULL CHECK (length(closed_digest) = 32),
    UNIQUE (ledger_id, sequence)
) STRICT;
CREATE INDEX source_authority_runtime_recovery_receipts_owner
    ON source_authority_runtime_recovery_receipts(
        runtime_owner_digest, receipt_digest
    );
CREATE TRIGGER source_authority_runtime_recovery_receipts_immutable
BEFORE UPDATE ON source_authority_runtime_recovery_receipts
BEGIN
    SELECT RAISE(ABORT, 'source authority runtime recovery receipts are immutable');
END;

CREATE TABLE source_authority_runtime_recovery_floors (
    ledger_id BLOB PRIMARY KEY CHECK (length(ledger_id) = 16),
    singleton INTEGER NOT NULL UNIQUE CHECK (singleton = 1),
    processed_sequence INTEGER NOT NULL CHECK (processed_sequence >= 0),
    acknowledged_sequence INTEGER NOT NULL CHECK (
        acknowledged_sequence >= 0 AND acknowledged_sequence <= processed_sequence
    )
) STRICT;
CREATE TRIGGER source_authority_runtime_recovery_floors_monotonic
BEFORE UPDATE ON source_authority_runtime_recovery_floors
BEGIN
    SELECT CASE WHEN
        NEW.ledger_id IS NOT OLD.ledger_id
        OR NEW.singleton != OLD.singleton
        OR NEW.processed_sequence < OLD.processed_sequence
        OR NEW.acknowledged_sequence < OLD.acknowledged_sequence
    THEN RAISE(ABORT, 'source authority runtime recovery floor is not monotonic') END;
END;

CREATE TABLE source_authority_runtime_recovery_members (
    receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) = 32),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    runtime_epoch BLOB NOT NULL CHECK (length(runtime_epoch) = 16),
    PRIMARY KEY (receipt_digest, ordinal),
    UNIQUE (receipt_digest, owner_id, generation, source_authority),
    FOREIGN KEY (receipt_digest)
        REFERENCES source_authority_runtime_recovery_receipts(receipt_digest)
        ON DELETE CASCADE
) STRICT;
CREATE INDEX source_authority_runtime_recovery_members_reference
    ON source_authority_runtime_recovery_members(
        owner_id, generation, source_authority, runtime_epoch, receipt_digest
    );
CREATE TRIGGER source_authority_runtime_recovery_members_immutable
BEFORE UPDATE ON source_authority_runtime_recovery_members
BEGIN
    SELECT RAISE(ABORT, 'source authority runtime recovery members are immutable');
END;

CREATE TABLE source_authority_fleet_stages (
    owner_id TEXT PRIMARY KEY CHECK (length(owner_id) > 0),
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
    received_count INTEGER NOT NULL CHECK (received_count BETWEEN 0 AND 10000),
    authority_count INTEGER NOT NULL CHECK (authority_count BETWEEN 0 AND 10000),
    byte_count INTEGER NOT NULL CHECK (byte_count BETWEEN 0 AND 67108864),
    authorities_digest BLOB NOT NULL CHECK (length(authorities_digest) = 32),
    declarations_digest BLOB NOT NULL CHECK (length(declarations_digest) = 32),
    stage_seed BLOB NOT NULL CHECK (length(stage_seed) = 32),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    CHECK (received_count <= authority_count),
    CHECK (complete = 0 OR received_count = authority_count),
    UNIQUE (owner_id, generation),
    UNIQUE (owner_id, generation, stage_seed)
) STRICT;
CREATE TRIGGER source_authority_fleet_stage_identity_immutable
BEFORE UPDATE OF
    owner_id, expected_generation, generation, authority_count,
    authorities_digest, declarations_digest, stage_seed
ON source_authority_fleet_stages
WHEN NEW.owner_id <> OLD.owner_id
  OR NEW.expected_generation <> OLD.expected_generation
  OR NEW.generation <> OLD.generation
  OR NEW.authority_count <> OLD.authority_count
  OR NEW.authorities_digest <> OLD.authorities_digest
  OR NEW.declarations_digest <> OLD.declarations_digest
  OR NEW.stage_seed <> OLD.stage_seed
BEGIN
    SELECT RAISE(ABORT, 'source authority fleet stage identity is immutable');
END;

CREATE TABLE source_authority_fleet_stage_members (
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    driver_config BLOB NOT NULL CHECK (length(driver_config) <= 65536),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    PRIMARY KEY (owner_id, generation, source_authority),
	UNIQUE (owner_id, generation, source_authority, declaration_digest),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_fleet_stages(owner_id, generation) ON DELETE CASCADE
) STRICT;
CREATE INDEX source_authority_fleet_stage_members_page
    ON source_authority_fleet_stage_members(
        owner_id, generation, source_authority, driver_id, declaration_digest
    );
CREATE TRIGGER source_authority_fleet_stage_members_immutable
BEFORE UPDATE ON source_authority_fleet_stage_members
WHEN NEW.owner_id <> OLD.owner_id
  OR NEW.generation <> OLD.generation
  OR NEW.source_authority <> OLD.source_authority
  OR NEW.driver_id <> OLD.driver_id
  OR NEW.driver_config <> OLD.driver_config
  OR NEW.declaration_digest <> OLD.declaration_digest
BEGIN
    SELECT RAISE(ABORT, 'source authority fleet stage member is immutable');
END;
CREATE TRIGGER source_authority_fleets_insert_fenced
BEFORE INSERT ON source_authority_fleets
BEGIN
    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM source_authority_fleet_heads head
        WHERE head.owner_id = NEW.owner_id
          AND head.generation >= NEW.generation
    ) THEN RAISE(
        ABORT, 'source authority fleet generation does not advance its head'
    ) END;
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM source_authority_fleet_stages stage
        WHERE stage.owner_id = NEW.owner_id
          AND stage.generation = NEW.generation
          AND stage.complete = 1
          AND stage.authority_count = NEW.authority_count
          AND stage.authorities_digest = NEW.authorities_digest
          AND stage.declarations_digest = NEW.declarations_digest
    ) THEN RAISE(
        ABORT, 'source authority fleet has no exact complete stage'
    ) END;
END;
CREATE TRIGGER source_authority_fleet_members_insert_fenced
BEFORE INSERT ON source_authority_fleet_members
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM source_authority_fleet_stages stage
        JOIN source_authority_fleet_stage_members member
          ON member.owner_id = stage.owner_id
         AND member.generation = stage.generation
        WHERE stage.owner_id = NEW.owner_id
          AND stage.generation = NEW.generation
          AND stage.complete = 1
          AND member.source_authority = NEW.source_authority
          AND member.declaration_digest = NEW.declaration_digest
    ) THEN RAISE(
        ABORT, 'source authority fleet member has no exact complete stage'
    ) END;
END;
CREATE TRIGGER source_authority_fleet_members_delete_fenced
BEFORE DELETE ON source_authority_fleet_members
WHEN EXISTS (
    SELECT 1
    FROM source_authority_fleet_heads head
    WHERE head.owner_id = OLD.owner_id
      AND head.generation = OLD.generation
)
BEGIN
    SELECT RAISE(
        ABORT, 'current source authority fleet members are immutable'
    );
END;

CREATE TABLE source_authority_claims (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    current_generation INTEGER CHECK (
        current_generation IS NULL OR current_generation > 0
    ),
    current_declaration_digest BLOB CHECK (
        current_declaration_digest IS NULL
        OR length(current_declaration_digest) = 32
    ),
    pending_generation INTEGER CHECK (
        pending_generation IS NULL OR pending_generation > 0
    ),
    pending_stage_seed BLOB CHECK (
        pending_stage_seed IS NULL OR length(pending_stage_seed) = 32
    ),
    pending_declaration_digest BLOB CHECK (
        pending_declaration_digest IS NULL
        OR length(pending_declaration_digest) = 32
    ),
    CHECK (
        (current_generation IS NULL AND current_declaration_digest IS NULL)
        OR
        (current_generation IS NOT NULL
         AND current_declaration_digest IS NOT NULL)
    ),
    CHECK (
        (pending_generation IS NULL
         AND pending_stage_seed IS NULL
         AND pending_declaration_digest IS NULL)
        OR
        (pending_generation IS NOT NULL
         AND pending_stage_seed IS NOT NULL
         AND pending_declaration_digest IS NOT NULL)
    ),
    CHECK (current_generation IS NOT NULL OR pending_generation IS NOT NULL),
    CHECK (
        current_generation IS NULL
        OR pending_generation IS NULL
        OR pending_generation > current_generation
    ),
    FOREIGN KEY (
        owner_id, current_generation,
        source_authority, current_declaration_digest
    ) REFERENCES source_authority_fleet_members(
        owner_id, generation, source_authority, declaration_digest
    ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        owner_id, pending_generation,
        source_authority, pending_declaration_digest
    ) REFERENCES source_authority_fleet_stage_members(
        owner_id, generation, source_authority, declaration_digest
    ) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (
        owner_id, pending_generation, pending_stage_seed
    ) REFERENCES source_authority_fleet_stages(
        owner_id, generation, stage_seed
    ) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE INDEX source_authority_claims_current
    ON source_authority_claims(
        owner_id, current_generation, source_authority
    ) WHERE current_generation IS NOT NULL;
CREATE INDEX source_authority_claims_pending
    ON source_authority_claims(
        owner_id, pending_generation, source_authority
    ) WHERE pending_generation IS NOT NULL;
CREATE TRIGGER source_authority_claims_identity_immutable
BEFORE UPDATE OF source_authority, owner_id ON source_authority_claims
BEGIN
    SELECT RAISE(ABORT, 'source authority claim identity is immutable');
END;
CREATE TRIGGER source_authority_claims_pending_stable
BEFORE UPDATE OF
    pending_generation, pending_stage_seed, pending_declaration_digest
ON source_authority_claims
WHEN OLD.pending_generation IS NOT NULL
 AND NEW.pending_generation IS NOT NULL
 AND (
     NEW.pending_generation <> OLD.pending_generation
     OR NEW.pending_stage_seed <> OLD.pending_stage_seed
     OR NEW.pending_declaration_digest <> OLD.pending_declaration_digest
 )
BEGIN
    SELECT RAISE(ABORT, 'source authority pending claim is immutable');
END;
CREATE TRIGGER source_authority_claims_current_promoted
BEFORE UPDATE OF current_generation, current_declaration_digest
ON source_authority_claims
WHEN (
    NEW.current_generation IS NOT OLD.current_generation
    OR NEW.current_declaration_digest IS NOT OLD.current_declaration_digest
)
AND NOT (
    OLD.pending_generation IS NOT NULL
    AND NEW.current_generation = OLD.pending_generation
    AND NEW.current_declaration_digest = OLD.pending_declaration_digest
    AND NEW.pending_generation IS NULL
    AND NEW.pending_stage_seed IS NULL
    AND NEW.pending_declaration_digest IS NULL
)
BEGIN
    SELECT RAISE(
        ABORT, 'source authority current claim must promote its pending claim'
    );
END;

CREATE TABLE source_authority_fleet_stage_pages (
    owner_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    request_digest BLOB CHECK (request_digest IS NULL OR length(request_digest) = 32),
    request_bound INTEGER NOT NULL DEFAULT 0 CHECK (request_bound IN (0, 1)),
    response_json BLOB NOT NULL CHECK (length(response_json) > 0),
    PRIMARY KEY (owner_id, generation, sequence),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_fleet_stages(owner_id, generation) ON DELETE CASCADE
) STRICT;

CREATE TABLE source_authority_retirement_receipts (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) = 32),
    result_json BLOB NOT NULL CHECK (length(result_json) > 0),
    PRIMARY KEY (owner_id, generation, source_authority)
) STRICT;
CREATE INDEX source_authority_retirement_receipts_compaction
    ON source_authority_retirement_receipts(owner_id, generation, source_authority);
CREATE INDEX source_authority_retirement_receipts_expected
    ON source_authority_retirement_receipts(
        owner_id, expected_generation, generation, source_authority
    );

CREATE TABLE source_authority_fleet_ack_receipts (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    authority_count INTEGER NOT NULL CHECK (authority_count BETWEEN 0 AND 10000),
    authorities_digest BLOB NOT NULL CHECK (length(authorities_digest) = 32),
    declarations_digest BLOB NOT NULL CHECK (length(declarations_digest) = 32),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    acknowledgement_digest BLOB NOT NULL CHECK (length(acknowledgement_digest) = 32),
    result_json BLOB NOT NULL CHECK (length(result_json) > 0),
    PRIMARY KEY (owner_id, generation),
    FOREIGN KEY (owner_id, generation)
        REFERENCES source_authority_fleets(owner_id, generation)
) STRICT;
CREATE INDEX source_authority_fleet_ack_receipts_compaction
    ON source_authority_fleet_ack_receipts(owner_id, generation);

CREATE TABLE source_authority_fleet_abort_receipts (
    owner_id TEXT NOT NULL CHECK (length(owner_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    expected_generation INTEGER NOT NULL CHECK (expected_generation >= 0),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) = 32),
    result_json BLOB NOT NULL CHECK (length(result_json) > 0),
    PRIMARY KEY (owner_id, generation)
) STRICT;
CREATE INDEX source_authority_fleet_abort_receipts_compaction
    ON source_authority_fleet_abort_receipts(owner_id, generation);
CREATE INDEX source_authority_fleet_abort_receipts_expected
    ON source_authority_fleet_abort_receipts(
        owner_id, expected_generation, generation
    );

CREATE TABLE source_observer_streams (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    fleet_generation INTEGER NOT NULL CHECK (fleet_generation > 0),
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    root_set_digest BLOB NOT NULL CHECK (length(root_set_digest) = 32),
    fleet_digest BLOB NOT NULL CHECK (length(fleet_digest) = 32),
    last_received_sequence INTEGER NOT NULL CHECK (last_received_sequence >= 0),
    last_applied_sequence INTEGER NOT NULL CHECK (last_applied_sequence >= 0 AND last_applied_sequence <= last_received_sequence),
    applied_snapshot_id TEXT NOT NULL DEFAULT '',
    applied_snapshot_operation BLOB NOT NULL DEFAULT X'' CHECK (length(applied_snapshot_operation) IN (0, 16)),
    applied_snapshot_digest BLOB NOT NULL DEFAULT X'' CHECK (length(applied_snapshot_digest) IN (0, 32)),
    applied_snapshot_fence BLOB NOT NULL DEFAULT X'' CHECK (length(applied_snapshot_fence) IN (0, 32)),
    state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4)),
    quarantine_detail TEXT NOT NULL,
    CHECK ((state = 3 AND length(quarantine_detail) > 0) OR (state <> 3 AND quarantine_detail = '')),
    CHECK ((applied_snapshot_id = '' AND length(applied_snapshot_operation) = 0 AND
            length(applied_snapshot_digest) = 0 AND length(applied_snapshot_fence) = 0)
        OR (length(applied_snapshot_id) > 0 AND length(applied_snapshot_operation) = 16 AND
            length(applied_snapshot_digest) = 32 AND length(applied_snapshot_fence) = 32)),
    FOREIGN KEY (fleet_owner_id, fleet_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX source_observer_streams_fleet
    ON source_observer_streams(
        fleet_owner_id, fleet_generation, source_authority
    );

CREATE TABLE source_observer_configuration_stages (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    fleet_generation INTEGER NOT NULL CHECK (fleet_generation > 0),
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16),
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    root_set_digest BLOB NOT NULL CHECK (length(root_set_digest) = 32),
    fleet_digest BLOB NOT NULL CHECK (length(fleet_digest) = 32),
    reset INTEGER NOT NULL CHECK (reset IN (0, 1)),
    expected_root_count INTEGER NOT NULL CHECK (expected_root_count > 0),
    expected_checkpoint_count INTEGER NOT NULL CHECK (expected_checkpoint_count > 0),
    expected_roots_digest BLOB NOT NULL CHECK (length(expected_roots_digest) = 32),
    expected_checkpoints_digest BLOB NOT NULL CHECK (length(expected_checkpoints_digest) = 32),
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
    root_count INTEGER NOT NULL CHECK (root_count >= 0),
    checkpoint_count INTEGER NOT NULL CHECK (checkpoint_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count BETWEEN 0 AND 2147483648),
    phase INTEGER NOT NULL CHECK (phase IN (1, 2)),
    last_root_id TEXT NOT NULL,
    last_checkpoint_stream TEXT NOT NULL,
    identity_digest BLOB NOT NULL CHECK (length(identity_digest) = 32),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    roots_digest BLOB NOT NULL CHECK (length(roots_digest) = 32),
    checkpoints_digest BLOB NOT NULL CHECK (length(checkpoints_digest) = 32),
    UNIQUE (source_authority, operation_id),
    FOREIGN KEY (fleet_owner_id, fleet_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX source_observer_configuration_stages_fleet
    ON source_observer_configuration_stages(
        fleet_owner_id, fleet_generation, source_authority
    );

CREATE TABLE source_observer_configuration_pages (
    source_authority TEXT NOT NULL,
    operation_id BLOB NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    page_digest BLOB NOT NULL CHECK (length(page_digest) = 32),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    cumulative_root_count INTEGER NOT NULL CHECK (cumulative_root_count >= 0),
    cumulative_checkpoint_count INTEGER NOT NULL CHECK (cumulative_checkpoint_count >= 0),
    cumulative_byte_count INTEGER NOT NULL CHECK (cumulative_byte_count > 0),
    PRIMARY KEY (source_authority, operation_id, sequence),
    FOREIGN KEY (source_authority, operation_id)
        REFERENCES source_observer_configuration_stages(source_authority, operation_id) ON DELETE CASCADE
);

CREATE TABLE source_observer_configuration_roots (
    source_authority TEXT NOT NULL,
    operation_id BLOB NOT NULL,
    root_id TEXT NOT NULL CHECK (length(root_id) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    path TEXT NOT NULL CHECK (length(path) > 0),
    volume_uuid TEXT NOT NULL CHECK (length(volume_uuid) > 0),
    root_inode INTEGER NOT NULL CHECK (root_inode > 0),
    root_birthtime_sec INTEGER NOT NULL,
    root_birthtime_nsec INTEGER NOT NULL CHECK (root_birthtime_nsec BETWEEN 0 AND 999999999),
    root_kind INTEGER NOT NULL CHECK (root_kind IN (1, 2)),
    PRIMARY KEY (source_authority, operation_id, root_id),
    UNIQUE (source_authority, operation_id, path),
    FOREIGN KEY (source_authority, operation_id)
        REFERENCES source_observer_configuration_stages(source_authority, operation_id) ON DELETE CASCADE
);

CREATE TABLE source_observer_configuration_checkpoints (
    source_authority TEXT NOT NULL,
    operation_id BLOB NOT NULL,
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    native_event_id INTEGER NOT NULL CHECK (native_event_id >= 0),
    PRIMARY KEY (source_authority, operation_id, stream_identity),
    FOREIGN KEY (source_authority, operation_id)
        REFERENCES source_observer_configuration_stages(source_authority, operation_id) ON DELETE CASCADE
);

CREATE TABLE source_observer_configuration_receipts (
    source_authority TEXT NOT NULL,
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    fleet_generation INTEGER NOT NULL CHECK (fleet_generation > 0),
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    root_count INTEGER NOT NULL CHECK (root_count > 0),
    checkpoint_count INTEGER NOT NULL CHECK (checkpoint_count > 0),
    byte_count INTEGER NOT NULL CHECK (byte_count > 0),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    stream_identity TEXT NOT NULL,
    root_epoch TEXT NOT NULL,
    root_set_digest BLOB NOT NULL CHECK (length(root_set_digest) = 32),
    fleet_digest BLOB NOT NULL CHECK (length(fleet_digest) = 32),
    last_received_sequence INTEGER NOT NULL,
    last_applied_sequence INTEGER NOT NULL,
    state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4)),
    quarantine_detail TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    forgotten INTEGER NOT NULL DEFAULT 0 CHECK (forgotten IN (0, 1)),
    CHECK (forgotten = 0 OR acknowledged = 1),
    PRIMARY KEY (source_authority, operation_id),
    FOREIGN KEY (fleet_owner_id, fleet_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE INDEX source_observer_configuration_receipts_ack_gc
    ON source_observer_configuration_receipts(
        acknowledged, source_authority, sequence, operation_id
    );
CREATE INDEX source_observer_configuration_receipts_ack_order
    ON source_observer_configuration_receipts(
        acknowledged, source_authority
    );
CREATE INDEX source_observer_configuration_receipts_authority_rowid
    ON source_observer_configuration_receipts(source_authority);
CREATE INDEX source_observer_configuration_receipts_fleet
    ON source_observer_configuration_receipts(
        fleet_owner_id, fleet_generation, source_authority,
        operation_id, acknowledged
    );

CREATE TABLE source_observer_checkpoints (
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    native_event_id INTEGER NOT NULL CHECK (native_event_id >= 0),
    PRIMARY KEY (source_authority, stream_identity)
);

CREATE TABLE source_observer_inbox (
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    predecessor_sequence INTEGER NOT NULL CHECK (predecessor_sequence + 1 = sequence),
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    predecessor_event INTEGER NOT NULL CHECK (predecessor_event >= 0),
    through_event INTEGER NOT NULL CHECK (through_event >= predecessor_event),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    event_count INTEGER NOT NULL CHECK (event_count > 0),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    payload BLOB NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (source_authority, sequence),
    UNIQUE (source_authority, stream_identity, through_event)
);

CREATE TABLE source_physical_index (
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    file_identity BLOB NOT NULL,
    physical_kind INTEGER NOT NULL CHECK (physical_kind BETWEEN 1 AND 3),
    metadata_fingerprint BLOB NOT NULL CHECK (length(metadata_fingerprint) = 32),
    content_fingerprint BLOB NOT NULL CHECK (length(content_fingerprint) = 32),
    payload BLOB NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (source_authority, root_id, relative_path),
    FOREIGN KEY (source_authority, root_id) REFERENCES source_observer_roots(source_authority, root_id)
);
CREATE UNIQUE INDEX source_physical_index_identity
    ON source_physical_index(source_authority, file_identity);

CREATE TABLE source_physical_logical (
    source_authority TEXT NOT NULL,
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    PRIMARY KEY (source_authority, logical_id, root_id, relative_path),
    FOREIGN KEY (source_authority, root_id, relative_path)
        REFERENCES source_physical_index(source_authority, root_id, relative_path) ON DELETE CASCADE
);
CREATE INDEX source_physical_logical_lookup
    ON source_physical_logical(source_authority, logical_id, root_id, relative_path);
CREATE INDEX source_physical_logical_physical
    ON source_physical_logical(source_authority, root_id, relative_path);

CREATE TABLE source_snapshot_stages (
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    snapshot_id TEXT NOT NULL CHECK (length(snapshot_id) > 0),
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    file_identity BLOB NOT NULL,
    physical_kind INTEGER NOT NULL CHECK (physical_kind BETWEEN 1 AND 3),
    metadata_fingerprint BLOB NOT NULL CHECK (length(metadata_fingerprint) = 32),
    content_fingerprint BLOB NOT NULL CHECK (length(content_fingerprint) = 32),
    payload BLOB NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (source_authority, snapshot_id, root_id, relative_path),
    FOREIGN KEY (source_authority, root_id) REFERENCES source_observer_roots(source_authority, root_id)
);
CREATE INDEX source_snapshot_stages_page
    ON source_snapshot_stages(source_authority, snapshot_id, root_id, relative_path);
CREATE UNIQUE INDEX source_snapshot_stages_identity
    ON source_snapshot_stages(source_authority, snapshot_id, file_identity);

CREATE TABLE source_snapshot_logical (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    PRIMARY KEY (source_authority, snapshot_id, logical_id, root_id, relative_path),
    FOREIGN KEY (source_authority, snapshot_id, root_id, relative_path)
        REFERENCES source_snapshot_stages(source_authority, snapshot_id, root_id, relative_path) ON DELETE CASCADE
);
CREATE INDEX source_snapshot_logical_lookup
    ON source_snapshot_logical(source_authority, snapshot_id, logical_id, root_id, relative_path);
CREATE INDEX source_snapshot_logical_stage
    ON source_snapshot_logical(source_authority, snapshot_id, root_id, relative_path);

CREATE TABLE source_snapshot_sessions (
    source_authority TEXT PRIMARY KEY REFERENCES source_observer_streams(source_authority),
    snapshot_id TEXT NOT NULL CHECK (length(snapshot_id) > 0),
    physical_count INTEGER NOT NULL DEFAULT 0 CHECK (physical_count >= 0 AND physical_count <= 10000000),
    physical_bytes INTEGER NOT NULL DEFAULT 0 CHECK (physical_bytes >= 0 AND physical_bytes <= 2147483648)
);

CREATE TABLE source_snapshot_publications (
    source_authority TEXT PRIMARY KEY REFERENCES source_snapshot_sessions(source_authority) ON DELETE CASCADE,
    snapshot_id TEXT NOT NULL CHECK (length(snapshot_id) > 0),
    operation_id BLOB NOT NULL UNIQUE CHECK (length(operation_id) = 16),
	change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
	source_revision INTEGER NOT NULL CHECK (source_revision > 0),
	cause TEXT NOT NULL CHECK (cause IN ('external_unattributed', 'bootstrap')),
	fence_digest BLOB NOT NULL CHECK (length(fence_digest) = 32),
	fence_authority_generation INTEGER NOT NULL CHECK (fence_authority_generation > 0),
	fence_inbox INTEGER NOT NULL CHECK (fence_inbox >= 0),
    fence_root_digest BLOB NOT NULL CHECK (length(fence_root_digest) = 32),
    fence_fleet_digest BLOB NOT NULL CHECK (length(fence_fleet_digest) = 32),
    next_cursor TEXT NOT NULL CHECK (length(next_cursor) <= 512),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    page_count INTEGER NOT NULL CHECK (page_count >= 0),
    last_affected_key TEXT NOT NULL,
    last_tenant TEXT NOT NULL,
    last_logical TEXT NOT NULL,
    affected_count INTEGER NOT NULL CHECK (affected_count >= 0),
    root_count INTEGER NOT NULL CHECK (root_count >= 0),
    binding_count INTEGER NOT NULL CHECK (binding_count >= 0),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    metadata_bytes INTEGER NOT NULL CHECK (metadata_bytes >= 0 AND metadata_bytes <= 2147483648),
    content_bytes INTEGER NOT NULL CHECK (content_bytes >= 0 AND content_bytes <= 68719476736),
    UNIQUE (source_authority, snapshot_id)
);

CREATE TABLE source_snapshot_fence_checkpoints (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    stream_identity TEXT NOT NULL CHECK (length(stream_identity) > 0),
    native_event_id INTEGER NOT NULL CHECK (native_event_id >= 0),
    root_epoch TEXT NOT NULL CHECK (length(root_epoch) > 0),
    PRIMARY KEY (source_authority, snapshot_id, stream_identity),
    FOREIGN KEY (source_authority, snapshot_id)
        REFERENCES source_snapshot_publications(source_authority, snapshot_id) ON DELETE CASCADE
);

CREATE TABLE source_snapshot_pages (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    cursor TEXT NOT NULL CHECK (length(cursor) <= 512),
    next_cursor TEXT NOT NULL CHECK (length(next_cursor) <= 512),
    page_digest BLOB NOT NULL CHECK (length(page_digest) = 32),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    page_bytes INTEGER NOT NULL CHECK (page_bytes >= 0 AND page_bytes <= 2097152),
    PRIMARY KEY (source_authority, snapshot_id, cursor),
    FOREIGN KEY (source_authority, snapshot_id)
        REFERENCES source_snapshot_publications(source_authority, snapshot_id) ON DELETE CASCADE
);

CREATE TABLE source_snapshot_affected (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    affected_key TEXT NOT NULL CHECK (length(affected_key) > 0),
    PRIMARY KEY (source_authority, snapshot_id, affected_key),
    FOREIGN KEY (source_authority, snapshot_id)
        REFERENCES source_snapshot_publications(source_authority, snapshot_id) ON DELETE CASCADE
);

CREATE TABLE source_snapshot_roots (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    root_key TEXT NOT NULL CHECK (length(root_key) > 0),
    catalog_operation_id BLOB NOT NULL CHECK (length(catalog_operation_id) = 32),
    catalog_revision INTEGER NOT NULL DEFAULT 0 CHECK (catalog_revision >= 0),
    catalog_fingerprint BLOB CHECK (catalog_fingerprint IS NULL OR length(catalog_fingerprint) = 32),
    file_provider_fingerprint BLOB CHECK (file_provider_fingerprint IS NULL OR length(file_provider_fingerprint) = 32),
    PRIMARY KEY (source_authority, snapshot_id, tenant),
    UNIQUE (source_authority, snapshot_id, logical_id),
    UNIQUE (source_authority, snapshot_id, root_key),
    UNIQUE (source_authority, snapshot_id, tenant, generation),
    FOREIGN KEY (source_authority, snapshot_id)
        REFERENCES source_snapshot_publications(source_authority, snapshot_id) ON DELETE CASCADE
);

CREATE TABLE source_snapshot_bindings (
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    effective_fingerprint BLOB NOT NULL CHECK (length(effective_fingerprint) = 32),
    PRIMARY KEY (source_authority, snapshot_id, logical_id),
    UNIQUE (source_authority, snapshot_id, source_key),
    FOREIGN KEY (source_authority, snapshot_id)
        REFERENCES source_snapshot_publications(source_authority, snapshot_id) ON DELETE CASCADE
);
CREATE INDEX source_snapshot_bindings_object_gc
    ON source_snapshot_bindings(object_id, source_authority, snapshot_id);

CREATE TABLE source_snapshot_objects (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source_authority TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    parent_key TEXT NOT NULL,
    object_name TEXT NOT NULL CHECK (length(object_name) > 0),
    name_key TEXT NOT NULL CHECK (length(name_key) > 0),
    object_kind INTEGER NOT NULL CHECK (object_kind BETWEEN 1 AND 3),
    object_mode INTEGER NOT NULL CHECK (object_mode >= 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    content_stage BLOB,
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    content_size INTEGER NOT NULL CHECK (content_size >= 0),
    link_target TEXT NOT NULL,
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    UNIQUE (source_authority, snapshot_id, tenant, source_key),
    FOREIGN KEY (source_authority, snapshot_id, logical_id)
        REFERENCES source_snapshot_bindings(source_authority, snapshot_id, logical_id) ON DELETE CASCADE,
    FOREIGN KEY (source_authority, snapshot_id, tenant, generation)
        REFERENCES source_snapshot_roots(source_authority, snapshot_id, tenant, generation) ON DELETE CASCADE,
    CHECK ((object_kind = 2 AND content_revision > 0 AND length(content_stage) = 16
               AND link_target = '')
        OR (object_kind = 3 AND content_revision > 0 AND content_stage IS NULL
               AND length(link_target) > 0)
        OR (object_kind = 1 AND content_revision = 0 AND content_stage IS NULL
               AND link_target = ''))
);
CREATE INDEX source_snapshot_objects_page
    ON source_snapshot_objects(source_authority, snapshot_id, tenant, sequence);
CREATE INDEX source_snapshot_objects_parent
    ON source_snapshot_objects(source_authority, snapshot_id, tenant, parent_key, source_key);
CREATE INDEX source_snapshot_objects_binding
    ON source_snapshot_objects(source_authority, snapshot_id, logical_id);
CREATE UNIQUE INDEX source_snapshot_objects_mount_name
    ON source_snapshot_objects(source_authority, snapshot_id, tenant, parent_key, name_key)
    WHERE mount_visible = 1;
CREATE UNIQUE INDEX source_snapshot_objects_file_provider_name
    ON source_snapshot_objects(source_authority, snapshot_id, tenant, parent_key, name_key)
    WHERE file_provider_visible = 1;

CREATE TABLE source_authority_bindings (
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    effective_fingerprint BLOB NOT NULL CHECK (length(effective_fingerprint) = 32),
    PRIMARY KEY (source_authority, logical_id),
    UNIQUE (source_authority, source_key)
);

CREATE TABLE source_publication_stages (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    stage_kind INTEGER NOT NULL DEFAULT 1 CHECK (stage_kind IN (1, 2)),
    stream_identity TEXT NOT NULL,
    root_epoch TEXT NOT NULL,
    through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    last_revision INTEGER NOT NULL CHECK (last_revision >= predecessor_revision),
    next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
    item_count INTEGER NOT NULL CHECK (item_count BETWEEN 0 AND 10000000),
    byte_count INTEGER NOT NULL CHECK (byte_count BETWEEN 0 AND 2147483648),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    aborting INTEGER NOT NULL CHECK (aborting IN (0, 1)),
    identity_digest BLOB NOT NULL CHECK (length(identity_digest) = 32),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    CHECK ((stage_kind = 1 AND length(stream_identity) > 0 AND length(root_epoch) > 0)
        OR (stage_kind = 2 AND stream_identity = '' AND root_epoch = '' AND through_sequence = 0)),
    PRIMARY KEY (source_authority, stage_operation_id)
);

CREATE TABLE source_driver_checkpoints (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    target_epoch INTEGER NOT NULL CHECK (target_epoch > 0),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 10000),
    targets_digest BLOB NOT NULL CHECK (length(targets_digest) = 32),
    source_operation_id BLOB NOT NULL UNIQUE CHECK (length(source_operation_id) = 16),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    applied_token TEXT NOT NULL CHECK (length(applied_token) BETWEEN 1 AND 255),
    token_digest BLOB NOT NULL CHECK (length(token_digest) = 32),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    snapshot_required INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_required IN (0, 2, 3)),
    CHECK ((cause = 'provider_mutation' AND length(origin_domain) > 0 AND origin_generation > 0)
        OR (cause <> 'provider_mutation' AND origin_domain = '' AND origin_generation = 0)),
    FOREIGN KEY (fleet_owner_id, authority_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE source_driver_checkpoint_targets (
    source_authority TEXT NOT NULL REFERENCES source_driver_checkpoints(source_authority) ON DELETE CASCADE,
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
    root_key TEXT NOT NULL CHECK (length(root_key) > 0),
    source_revision INTEGER NOT NULL CHECK (source_revision >= 0),
    catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
    PRIMARY KEY (source_authority, tenant),
    UNIQUE (tenant, generation)
) STRICT;

CREATE TABLE source_driver_mutation_reservations (
    mutation_id BLOB PRIMARY KEY REFERENCES prepared_mutations(mutation_id) ON DELETE CASCADE
        CHECK (length(mutation_id) = 32),
    claim_owner BLOB NOT NULL CHECK (length(claim_owner) = 16),
    claim_epoch INTEGER NOT NULL CHECK (claim_epoch > 0),
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 10000),
    targets_digest BLOB NOT NULL CHECK (length(targets_digest) = 32),
    mutation_tenant TEXT NOT NULL CHECK (length(mutation_tenant) > 0),
    mutation_generation INTEGER NOT NULL CHECK (mutation_generation > 0),
    from_token TEXT NOT NULL CHECK (length(from_token) BETWEEN 1 AND 255),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision > 0),
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    source_operation_id BLOB NOT NULL UNIQUE CHECK (length(source_operation_id) = 16),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    request_digest BLOB CHECK (request_digest IS NULL OR length(request_digest) = 32),
    request_bound INTEGER NOT NULL DEFAULT 0 CHECK (request_bound IN (0, 1)),
    committed INTEGER NOT NULL DEFAULT 0 CHECK (committed IN (0, 1)),
    target_epoch INTEGER NOT NULL CHECK (target_epoch > 0),
    declared_target_count INTEGER NOT NULL DEFAULT 0 CHECK (declared_target_count BETWEEN 0 AND target_count),
    target_cursor TEXT NOT NULL DEFAULT '',
    target_digest_state BLOB NOT NULL CHECK (length(target_digest_state) = 32),
    targets_prepared INTEGER NOT NULL DEFAULT 0 CHECK (targets_prepared IN (0, 1)),
    receipt_to_token TEXT,
    receipt_result_key TEXT,
    receipt_digest BLOB CHECK (receipt_digest IS NULL OR length(receipt_digest) = 32),
    CHECK ((request_bound = 0 AND request_digest IS NULL)
        OR (request_bound = 1 AND request_digest IS NOT NULL)),
    CHECK ((receipt_to_token IS NULL AND receipt_result_key IS NULL AND receipt_digest IS NULL)
        OR (length(receipt_to_token) BETWEEN 1 AND 255 AND receipt_result_key IS NOT NULL
            AND receipt_digest IS NOT NULL)),
    FOREIGN KEY (fleet_owner_id, authority_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE UNIQUE INDEX source_driver_mutation_reservations_active_authority
    ON source_driver_mutation_reservations(source_authority) WHERE committed = 0;
CREATE UNIQUE INDEX source_driver_mutation_reservations_active_tenant
    ON source_driver_mutation_reservations(mutation_tenant) WHERE committed = 0;

CREATE TABLE source_driver_mutation_reservation_targets (
    mutation_id BLOB NOT NULL REFERENCES source_driver_mutation_reservations(mutation_id) ON DELETE CASCADE
        CHECK (length(mutation_id) = 32),
    tenant TEXT NOT NULL CHECK (length(tenant) > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    PRIMARY KEY (mutation_id, tenant)
) STRICT;

CREATE TABLE source_driver_stages (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 10000),
    targets_digest BLOB NOT NULL CHECK (length(targets_digest) = 32),
    source_operation_id BLOB NOT NULL UNIQUE CHECK (length(source_operation_id) = 16),
    change_id BLOB NOT NULL UNIQUE CHECK (length(change_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    mode INTEGER NOT NULL CHECK (mode IN (1, 2, 3)),
    snapshot_reason INTEGER NOT NULL CHECK (snapshot_reason IN (0, 1, 2, 3)),
    from_token TEXT NOT NULL CHECK (length(from_token) <= 255),
    from_token_digest BLOB NOT NULL CHECK (length(from_token_digest) = 32),
    to_token TEXT NOT NULL CHECK (length(to_token) BETWEEN 1 AND 255),
    to_token_digest BLOB NOT NULL CHECK (length(to_token_digest) = 32),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    target_epoch INTEGER NOT NULL CHECK (target_epoch > 0),
    target_cursor TEXT NOT NULL,
    declared_target_count INTEGER NOT NULL CHECK (declared_target_count BETWEEN 0 AND target_count),
    target_digest_state BLOB NOT NULL CHECK (length(target_digest_state) = 32),
    targets_prepared INTEGER NOT NULL CHECK (targets_prepared IN (0, 1)),
    driver_cursor BLOB NOT NULL CHECK (length(driver_cursor) <= 4096),
    driver_page_digest BLOB NOT NULL CHECK (length(driver_page_digest) = 32),
    mutation_id BLOB CHECK (mutation_id IS NULL OR length(mutation_id) = 32),
    mutation_tenant TEXT NOT NULL,
    mutation_generation INTEGER NOT NULL CHECK (mutation_generation >= 0),
    mutation_result_key TEXT NOT NULL,
    mutation_request_digest BLOB CHECK (mutation_request_digest IS NULL OR length(mutation_request_digest) = 32),
    mutation_receipt_digest BLOB CHECK (mutation_receipt_digest IS NULL OR length(mutation_receipt_digest) = 32),
    claim_owner BLOB CHECK (claim_owner IS NULL OR length(claim_owner) = 16),
    claim_epoch INTEGER CHECK (claim_epoch IS NULL OR claim_epoch > 0),
    identity_digest BLOB NOT NULL CHECK (length(identity_digest) = 32),
    CHECK ((cause = 'provider_mutation' AND length(origin_domain) > 0 AND origin_generation > 0)
        OR (cause <> 'provider_mutation' AND origin_domain = '' AND origin_generation = 0)),
    CHECK ((mode = 1 AND snapshot_reason IN (1, 2, 3) AND from_token = '')
        OR (mode = 2 AND snapshot_reason = 0 AND length(from_token) > 0)
        OR (mode = 3 AND snapshot_reason IN (0, 2) AND length(from_token) > 0)),
    CHECK ((mode = 3 AND mutation_id IS NOT NULL AND length(mutation_tenant) > 0
            AND mutation_generation > 0
            AND mutation_request_digest IS NOT NULL AND mutation_receipt_digest IS NOT NULL
            AND claim_owner IS NOT NULL AND claim_epoch IS NOT NULL)
        OR (mode <> 3 AND mutation_id IS NULL AND mutation_tenant = ''
            AND mutation_generation = 0 AND mutation_result_key = ''
            AND mutation_request_digest IS NULL AND mutation_receipt_digest IS NULL
            AND claim_owner IS NULL AND claim_epoch IS NULL)),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE,
    FOREIGN KEY (fleet_owner_id, authority_generation, source_authority)
        REFERENCES source_authority_fleet_members(owner_id, generation, source_authority)
        DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (source_authority, stage_operation_id)
) STRICT;

CREATE TABLE source_driver_stage_targets (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
    root_key TEXT NOT NULL CHECK (length(root_key) > 0),
    expected_catalog_revision INTEGER NOT NULL CHECK (expected_catalog_revision > 0),
    PRIMARY KEY (source_authority, stage_operation_id, tenant),
    UNIQUE (source_authority, stage_operation_id, tenant, generation),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_driver_stages(source_authority, stage_operation_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE source_driver_stage_entries (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    change_sequence INTEGER NOT NULL CHECK (change_sequence >= 0),
    action INTEGER NOT NULL CHECK (action IN (1, 2)),
    parent_key TEXT NOT NULL,
    object_name TEXT NOT NULL,
    object_kind INTEGER NOT NULL CHECK (object_kind BETWEEN 0 AND 3),
    object_mode INTEGER NOT NULL CHECK (object_mode >= 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    content_stage BLOB,
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    content_size INTEGER NOT NULL CHECK (content_size >= 0),
    link_target TEXT NOT NULL,
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    PRIMARY KEY (source_authority, stage_operation_id, tenant, change_sequence, source_key),
    FOREIGN KEY (source_authority, stage_operation_id, tenant, generation)
        REFERENCES source_driver_stage_targets(source_authority, stage_operation_id, tenant, generation) ON DELETE CASCADE,
    CHECK ((action = 1 AND parent_key = '' AND object_name = '' AND object_kind = 0
            AND object_mode = 0 AND content_revision = 0 AND content_stage IS NULL
            AND content_hash = zeroblob(32) AND content_size = 0 AND link_target = ''
            AND mount_visible = 0 AND file_provider_visible = 0)
        OR (action = 2 AND length(object_name) > 0 AND object_kind BETWEEN 1 AND 3))
) STRICT;
CREATE INDEX source_driver_stage_entries_latest
    ON source_driver_stage_entries(
        source_authority, stage_operation_id, tenant, source_key, change_sequence DESC
    );
CREATE INDEX source_driver_stage_entries_affected
    ON source_driver_stage_entries(source_authority, stage_operation_id, source_key);

CREATE TABLE source_driver_stage_receipts (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    mode INTEGER NOT NULL CHECK (mode IN (1, 2, 3)),
    from_token TEXT NOT NULL CHECK (length(from_token) <= 255),
    to_token TEXT NOT NULL CHECK (length(to_token) BETWEEN 1 AND 255),
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 10000),
    targets_digest BLOB NOT NULL CHECK (length(targets_digest) = 32),
    stage_sequence INTEGER NOT NULL CHECK (stage_sequence > 0),
    stage_item_count INTEGER NOT NULL CHECK (stage_item_count >= 0),
    stage_byte_count INTEGER NOT NULL CHECK (stage_byte_count >= 0),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    identity_digest BLOB NOT NULL CHECK (length(identity_digest) = 32),
    result_json BLOB NOT NULL CHECK (length(result_json) > 0),
    result_digest BLOB NOT NULL CHECK (length(result_digest) = 32),
    mutation_id BLOB CHECK (mutation_id IS NULL OR length(mutation_id) = 32),
    mutation_request_digest BLOB CHECK (mutation_request_digest IS NULL OR length(mutation_request_digest) = 32),
    mutation_receipt_digest BLOB CHECK (mutation_receipt_digest IS NULL OR length(mutation_receipt_digest) = 32),
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    forgotten INTEGER NOT NULL DEFAULT 0 CHECK (forgotten IN (0, 1)),
    CHECK (forgotten = 0 OR acknowledged = 1),
    CHECK ((mode = 3 AND mutation_id IS NOT NULL AND mutation_request_digest IS NOT NULL
            AND mutation_receipt_digest IS NOT NULL)
        OR (mode <> 3 AND mutation_id IS NULL AND mutation_request_digest IS NULL
            AND mutation_receipt_digest IS NULL)),
    PRIMARY KEY (source_authority, stage_operation_id)
) STRICT;

CREATE UNIQUE INDEX source_driver_stage_receipts_mutation
ON source_driver_stage_receipts(source_authority, mutation_id)
WHERE mutation_id IS NOT NULL;

CREATE INDEX source_driver_stage_receipts_pending
ON source_driver_stage_receipts(forgotten, source_authority, source_revision);

CREATE TABLE source_publication_stage_pages (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    page_digest BLOB NOT NULL CHECK (length(page_digest) = 32),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    page_item_count INTEGER NOT NULL CHECK (page_item_count BETWEEN 1 AND 256),
    page_byte_count INTEGER NOT NULL CHECK (page_byte_count BETWEEN 1 AND 1048576),
    driver_cursor BLOB CHECK (driver_cursor IS NULL OR length(driver_cursor) <= 4096),
    driver_page_digest BLOB CHECK (driver_page_digest IS NULL OR length(driver_page_digest) = 32),
    cumulative_revision INTEGER NOT NULL CHECK (cumulative_revision >= 0),
    cumulative_item_count INTEGER NOT NULL CHECK (cumulative_item_count BETWEEN 1 AND 10000000),
    cumulative_byte_count INTEGER NOT NULL CHECK (cumulative_byte_count BETWEEN 1 AND 2147483648),
    PRIMARY KEY (source_authority, stage_operation_id, sequence),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_receipts (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
    first_revision INTEGER NOT NULL CHECK (first_revision > 0),
    last_revision INTEGER NOT NULL CHECK (last_revision >= first_revision),
    revision_count INTEGER NOT NULL CHECK (revision_count > 0),
    stage_sequence INTEGER NOT NULL CHECK (stage_sequence > 0),
    stage_item_count INTEGER NOT NULL CHECK (stage_item_count BETWEEN 1 AND 10000000),
    stage_byte_count INTEGER NOT NULL CHECK (stage_byte_count BETWEEN 1 AND 2147483648),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) = 32),
    PRIMARY KEY (source_authority, stage_operation_id)
) STRICT;
CREATE INDEX source_publication_stage_receipts_revision
    ON source_publication_stage_receipts(source_authority, last_revision, first_revision);

CREATE TABLE source_observer_settlement_receipts (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL UNIQUE CHECK (length(stage_operation_id) = 16),
    fleet_owner_id TEXT NOT NULL CHECK (length(fleet_owner_id) > 0),
    authority_generation INTEGER NOT NULL CHECK (authority_generation > 0),
    driver_id TEXT NOT NULL CHECK (length(driver_id) BETWEEN 1 AND 128),
    declaration_digest BLOB NOT NULL CHECK (length(declaration_digest) = 32),
    through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
    source_revision INTEGER NOT NULL CHECK (source_revision >= 0),
    stage_sequence INTEGER NOT NULL CHECK (stage_sequence > 0),
    stage_item_count INTEGER NOT NULL CHECK (stage_item_count BETWEEN 1 AND 10000000),
    stage_byte_count INTEGER NOT NULL CHECK (stage_byte_count BETWEEN 1 AND 2147483648),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) = 32),
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    PRIMARY KEY (source_authority, stage_operation_id)
) STRICT;
CREATE INDEX source_observer_settlement_receipts_sequence
    ON source_observer_settlement_receipts(source_authority, through_sequence);
CREATE INDEX source_observer_settlement_receipts_ack_gc
    ON source_observer_settlement_receipts(
        acknowledged, source_authority, through_sequence, stage_operation_id
    );
CREATE INDEX source_observer_settlement_receipts_ack_order
    ON source_observer_settlement_receipts(
        acknowledged, source_authority
    );
CREATE INDEX source_observer_settlement_receipts_authority_rowid
    ON source_observer_settlement_receipts(source_authority);

CREATE TABLE source_publication_stage_revisions (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    source_revision INTEGER NOT NULL CHECK (source_revision > 0),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    mode INTEGER NOT NULL CHECK (mode IN (1, 2)),
    operation_id BLOB NOT NULL CHECK (length(operation_id) = 16),
    change_id BLOB NOT NULL CHECK (length(change_id) = 16),
    cause TEXT NOT NULL CHECK (cause IN ('provider_mutation', 'daemon_write', 'external_unattributed', 'bootstrap')),
    origin_domain TEXT NOT NULL,
    origin_generation INTEGER NOT NULL CHECK (origin_generation >= 0),
    last_affected_key TEXT NOT NULL,
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    PRIMARY KEY (source_authority, stage_operation_id, source_revision),
    UNIQUE (source_authority, stage_operation_id, operation_id),
    CHECK ((cause = 'provider_mutation' AND length(origin_domain) > 0 AND origin_generation > 0)
        OR (cause <> 'provider_mutation' AND origin_domain = '' AND origin_generation = 0)),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_affected (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    source_revision INTEGER NOT NULL,
    affected_key TEXT NOT NULL CHECK (length(affected_key) > 0),
    PRIMARY KEY (source_authority, stage_operation_id, source_revision, affected_key),
    FOREIGN KEY (source_authority, stage_operation_id, source_revision)
        REFERENCES source_publication_stage_revisions(
            source_authority, stage_operation_id, source_revision
        ) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_index (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    file_identity BLOB NOT NULL,
    object_kind INTEGER NOT NULL CHECK (object_kind BETWEEN 1 AND 3),
    metadata_fingerprint BLOB NOT NULL CHECK (length(metadata_fingerprint) = 32),
    content_fingerprint BLOB NOT NULL CHECK (length(content_fingerprint) = 32),
    payload BLOB NOT NULL CHECK (length(payload) > 0),
    PRIMARY KEY (source_authority, stage_operation_id, root_id, relative_path),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_index_logical (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    PRIMARY KEY (source_authority, stage_operation_id, root_id, relative_path, logical_id),
    FOREIGN KEY (source_authority, stage_operation_id, root_id, relative_path)
        REFERENCES source_publication_stage_index(
            source_authority, stage_operation_id, root_id, relative_path
        ) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_index_deletes (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    root_id TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    PRIMARY KEY (source_authority, stage_operation_id, root_id, relative_path),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_bindings (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    logical_id TEXT NOT NULL CHECK (length(logical_id) > 0),
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    effective_fingerprint BLOB NOT NULL CHECK (length(effective_fingerprint) = 32),
    PRIMARY KEY (source_authority, stage_operation_id, logical_id),
    UNIQUE (source_authority, stage_operation_id, source_key),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_publication_stage_mutations (
    source_authority TEXT NOT NULL,
    stage_operation_id BLOB NOT NULL,
    mutation_id BLOB NOT NULL CHECK (length(mutation_id) = 32),
    matched INTEGER NOT NULL CHECK (matched IN (0, 1)),
    PRIMARY KEY (source_authority, stage_operation_id, mutation_id),
    FOREIGN KEY (source_authority, stage_operation_id)
        REFERENCES source_publication_stages(source_authority, stage_operation_id) ON DELETE CASCADE
);

CREATE TABLE source_driver_visibility (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    active_publication_id BLOB NOT NULL CHECK (length(active_publication_id) IN (0, 16)),
    active_source_revision INTEGER NOT NULL CHECK (active_source_revision >= 0),
    visibility_epoch INTEGER NOT NULL CHECK (visibility_epoch >= 0),
    CHECK ((length(active_publication_id) = 0 AND active_source_revision = 0)
        OR (length(active_publication_id) = 16 AND active_source_revision > 0))
) STRICT;

CREATE TABLE source_driver_publications (
    source_authority TEXT NOT NULL CHECK (length(source_authority) > 0),
    publication_id BLOB NOT NULL CHECK (length(publication_id) = 16),
    publication_kind INTEGER NOT NULL DEFAULT 1 CHECK (publication_kind IN (1, 2, 3)),
    identity_digest BLOB NOT NULL CHECK (length(identity_digest) = 32),
    target_count INTEGER NOT NULL CHECK (target_count BETWEEN 1 AND 10000),
    targets_digest BLOB NOT NULL CHECK (length(targets_digest) = 32),
    stage_sequence INTEGER NOT NULL CHECK (stage_sequence > 0),
    stage_item_count INTEGER NOT NULL CHECK (stage_item_count > 0),
    stage_byte_count INTEGER NOT NULL CHECK (stage_byte_count > 0),
    stage_digest BLOB NOT NULL CHECK (length(stage_digest) = 32),
    predecessor_publication_id BLOB NOT NULL
        CHECK (length(predecessor_publication_id) IN (0, 16)),
    predecessor_revision INTEGER NOT NULL CHECK (predecessor_revision >= 0),
    source_revision INTEGER NOT NULL CHECK (source_revision > predecessor_revision),
    expected_visibility_epoch INTEGER NOT NULL CHECK (expected_visibility_epoch >= 0),
    target_epoch INTEGER NOT NULL CHECK (target_epoch > 0),
    phase INTEGER NOT NULL CHECK (phase BETWEEN 1 AND 16),
    cursor_tenant TEXT NOT NULL,
    cursor_key TEXT NOT NULL,
    initialized_target_count INTEGER NOT NULL
        CHECK (initialized_target_count BETWEEN 0 AND target_count),
    prepared_target_count INTEGER NOT NULL
        CHECK (prepared_target_count BETWEEN 0 AND target_count),
    item_count INTEGER NOT NULL CHECK (item_count BETWEEN 0 AND 100000000),
    byte_count INTEGER NOT NULL CHECK (byte_count BETWEEN 0 AND 1099511627776),
    rolling_digest BLOB NOT NULL CHECK (length(rolling_digest) = 32),
    prepared INTEGER NOT NULL CHECK (prepared IN (0, 1)),
    PRIMARY KEY (source_authority, publication_id),
    UNIQUE (publication_id),
    CHECK ((length(predecessor_publication_id) = 0 AND predecessor_revision = 0)
        OR (length(predecessor_publication_id) = 16 AND predecessor_revision > 0))
) STRICT;
CREATE UNIQUE INDEX source_driver_publications_semantic_revision
    ON source_driver_publications(source_authority, source_revision)
    WHERE publication_kind IN (1, 3);
CREATE INDEX source_driver_publications_predecessor
    ON source_driver_publications(source_authority, predecessor_publication_id);

CREATE TABLE source_driver_publication_compactions (
    source_authority TEXT PRIMARY KEY CHECK (length(source_authority) > 0),
    source_publication_id BLOB NOT NULL CHECK (length(source_publication_id) = 16),
    compaction_publication_id BLOB NOT NULL UNIQUE CHECK (length(compaction_publication_id) = 16),
    expected_visibility_epoch INTEGER NOT NULL CHECK (expected_visibility_epoch >= 0),
    phase INTEGER NOT NULL CHECK (phase BETWEEN 1 AND 5),
    FOREIGN KEY (source_authority, source_publication_id)
        REFERENCES source_driver_publications(source_authority, publication_id),
    FOREIGN KEY (source_authority, compaction_publication_id)
        REFERENCES source_driver_publications(source_authority, publication_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE source_driver_publication_targets (
    source_authority TEXT NOT NULL,
    publication_id BLOB NOT NULL,
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
    root_key TEXT NOT NULL CHECK (length(root_key) > 0),
    catalog_operation_id BLOB NOT NULL CHECK (length(catalog_operation_id) = 32),
    predecessor_head INTEGER NOT NULL CHECK (predecessor_head > 0),
    catalog_head INTEGER NOT NULL CHECK (catalog_head >= predecessor_head),
    catalog_fingerprint BLOB NOT NULL CHECK (length(catalog_fingerprint) = 32),
    file_provider_fingerprint BLOB NOT NULL CHECK (length(file_provider_fingerprint) = 32),
    changed INTEGER NOT NULL CHECK (changed IN (0, 1)),
    provider_changed INTEGER NOT NULL CHECK (provider_changed IN (0, 1)),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    phase INTEGER NOT NULL CHECK (phase BETWEEN 1 AND 16),
    cursor_key TEXT NOT NULL,
    cursor_object_id BLOB NOT NULL CHECK (length(cursor_object_id) IN (0, 16)),
    cursor_revision INTEGER NOT NULL CHECK (cursor_revision >= 0),
    catalog_state BLOB NOT NULL CHECK (length(catalog_state) <= 4096),
    provider_state BLOB NOT NULL CHECK (length(provider_state) <= 4096),
    next_change_sequence INTEGER NOT NULL CHECK (next_change_sequence >= 0),
    prepared INTEGER NOT NULL CHECK (prepared IN (0, 1)),
    PRIMARY KEY (source_authority, publication_id, tenant),
    FOREIGN KEY (source_authority, publication_id)
        REFERENCES source_driver_publications(source_authority, publication_id) ON DELETE CASCADE
) STRICT;
CREATE INDEX source_driver_publication_targets_phase
    ON source_driver_publication_targets(source_authority, publication_id, prepared, tenant);

CREATE TABLE source_driver_publication_objects (
    source_authority TEXT NOT NULL,
    publication_id BLOB NOT NULL,
    tenant TEXT NOT NULL,
    source_key TEXT NOT NULL CHECK (length(source_key) > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (source_authority, publication_id, tenant, source_key),
    UNIQUE (source_authority, publication_id, tenant, object_id),
    FOREIGN KEY (source_authority, publication_id, tenant)
        REFERENCES source_driver_publication_targets(
            source_authority, publication_id, tenant
        ) ON DELETE CASCADE
) STRICT;
CREATE UNIQUE INDEX source_driver_publication_objects_mount_live_name
    ON source_driver_publication_objects(
        source_authority, publication_id, tenant, parent_id, name_key
    ) WHERE tombstone = 0 AND mount_visible = 1;
CREATE UNIQUE INDEX source_driver_publication_objects_provider_live_name
    ON source_driver_publication_objects(
        source_authority, publication_id, tenant, parent_id, name_key
    ) WHERE tombstone = 0 AND file_provider_visible = 1;
CREATE INDEX source_driver_publication_objects_mount_parent
    ON source_driver_publication_objects(
        source_authority, publication_id, tenant, parent_id, name_key, object_id
    ) WHERE tombstone = 0 AND mount_visible = 1;
CREATE INDEX source_driver_publication_objects_provider_parent
    ON source_driver_publication_objects(
        source_authority, publication_id, tenant, parent_id, name_key, object_id
    ) WHERE tombstone = 0 AND file_provider_visible = 1;
CREATE INDEX source_driver_publication_objects_live_blob
    ON source_driver_publication_objects(hash) WHERE kind = 2 AND tombstone = 0;

CREATE TABLE source_driver_publication_versions (
    source_authority TEXT NOT NULL,
    publication_id BLOB NOT NULL,
    tenant TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (source_authority, publication_id, tenant, object_id, revision),
    FOREIGN KEY (source_authority, publication_id, tenant)
        REFERENCES source_driver_publication_targets(
            source_authority, publication_id, tenant
        ) ON DELETE CASCADE
) STRICT;
CREATE INDEX source_driver_publication_versions_snapshot
    ON source_driver_publication_versions(
        source_authority, publication_id, tenant, object_id, revision DESC
    );
CREATE INDEX source_driver_publication_versions_container_snapshot
    ON source_driver_publication_versions(
        source_authority, publication_id, tenant, parent_id, object_id, revision DESC
    );
CREATE INDEX source_driver_publication_versions_live_blob
    ON source_driver_publication_versions(hash) WHERE kind = 2 AND tombstone = 0;
CREATE TRIGGER source_driver_publication_versions_immutable
    BEFORE UPDATE ON source_driver_publication_versions
    BEGIN SELECT RAISE(ABORT, 'source publication object revisions are immutable'); END;

CREATE TABLE source_driver_publication_changes (
    source_authority TEXT NOT NULL,
    publication_id BLOB NOT NULL,
    tenant TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    scope_kind INTEGER NOT NULL CHECK (scope_kind IN (1, 2)),
    presentation INTEGER NOT NULL CHECK (presentation IN (1, 2)),
    scope_parent BLOB NOT NULL CHECK (length(scope_parent) = 16),
    scope_domain TEXT NOT NULL,
    scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    object_revision INTEGER NOT NULL CHECK (object_revision > 0),
    PRIMARY KEY (
        source_authority, publication_id, tenant, revision, scope_kind,
        presentation, scope_parent, scope_domain, scope_generation, sequence
    ),
    FOREIGN KEY (source_authority, publication_id, tenant)
        REFERENCES source_driver_publication_targets(
            source_authority, publication_id, tenant
        ) ON DELETE CASCADE
) STRICT;
CREATE INDEX source_driver_publication_changes_range
    ON source_driver_publication_changes(
        source_authority, publication_id, tenant, revision,
        scope_kind, presentation, scope_parent, scope_domain, scope_generation, sequence
    );

CREATE TABLE source_mutation_expectations (
    operation_id BLOB PRIMARY KEY CHECK (length(operation_id) = 32),
    source_authority TEXT NOT NULL REFERENCES source_observer_streams(source_authority),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    causal_origin BLOB NOT NULL CHECK (length(causal_origin) > 0),
    payload_digest BLOB NOT NULL CHECK (length(payload_digest) = 32),
    payload BLOB NOT NULL CHECK (length(payload) > 0),
	receipt_digest BLOB NOT NULL CHECK (length(receipt_digest) IN (0, 32)),
	receipt BLOB NOT NULL,
    state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4, 5))
);
CREATE INDEX source_mutation_expectations_authority_state
    ON source_mutation_expectations(source_authority, state);

CREATE TABLE IF NOT EXISTS tenant_state (
    tenant TEXT PRIMARY KEY REFERENCES tenants(tenant),
    generation INTEGER NOT NULL CHECK (generation > 0),
	 activated_generation INTEGER NOT NULL CHECK (activated_generation = 0 OR activated_generation = generation),
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    version INTEGER NOT NULL CHECK (version > 0),
    quarantine_lane INTEGER CHECK (quarantine_lane BETWEEN 1 AND 4),
    quarantine_revision INTEGER CHECK (quarantine_revision >= 0),
    quarantine_cause INTEGER CHECK (quarantine_cause BETWEEN 1 AND 4),
    quarantine_detail TEXT,
    quarantine_since INTEGER,
    CHECK (
        (quarantine_lane IS NULL AND quarantine_revision IS NULL AND
         quarantine_cause IS NULL AND quarantine_detail IS NULL AND quarantine_since IS NULL)
        OR
        (quarantine_lane IS NOT NULL AND quarantine_revision IS NOT NULL AND
         quarantine_cause IS NOT NULL AND quarantine_detail IS NOT NULL AND quarantine_since IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS objects (
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (tenant, object_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS objects_mount_live_name
    ON objects(tenant, parent_id, name_key) WHERE tombstone = 0 AND mount_visible = 1;
CREATE UNIQUE INDEX IF NOT EXISTS objects_file_provider_live_name
    ON objects(tenant, parent_id, name_key) WHERE tombstone = 0 AND file_provider_visible = 1;
CREATE INDEX IF NOT EXISTS objects_mount_live_parent
    ON objects(tenant, parent_id, name_key, object_id) WHERE tombstone = 0 AND mount_visible = 1;
CREATE INDEX IF NOT EXISTS objects_file_provider_live_parent
    ON objects(tenant, parent_id, name_key, object_id) WHERE tombstone = 0 AND file_provider_visible = 1;
CREATE INDEX objects_tombstone_gc
    ON objects(tenant, revision, object_id) WHERE tombstone = 1;

CREATE TABLE IF NOT EXISTS object_versions (
    tenant TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    parent_id BLOB NOT NULL CHECK (length(parent_id) = 16),
    revision INTEGER NOT NULL CHECK (revision > 0),
    metadata_revision INTEGER NOT NULL CHECK (metadata_revision > 0),
    content_revision INTEGER NOT NULL CHECK (content_revision >= 0),
    name TEXT NOT NULL,
    name_key TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3)),
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    hash BLOB NOT NULL CHECK (length(hash) = 32),
    link_target TEXT NOT NULL,
    desired_revision INTEGER NOT NULL CHECK (desired_revision >= 0),
    observed_revision INTEGER NOT NULL CHECK (observed_revision >= 0),
    verified_revision INTEGER NOT NULL CHECK (verified_revision >= 0),
    applied_revision INTEGER NOT NULL CHECK (applied_revision >= 0),
    mount_visible INTEGER NOT NULL CHECK (mount_visible IN (0, 1)),
    file_provider_visible INTEGER NOT NULL CHECK (file_provider_visible IN (0, 1)),
    tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
    PRIMARY KEY (tenant, object_id, revision)
);
CREATE INDEX IF NOT EXISTS object_versions_snapshot
    ON object_versions(tenant, object_id, revision DESC);
CREATE INDEX IF NOT EXISTS object_versions_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC);
CREATE INDEX IF NOT EXISTS object_versions_mount_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC) WHERE mount_visible = 1;
CREATE INDEX IF NOT EXISTS object_versions_file_provider_container_snapshot
    ON object_versions(tenant, parent_id, object_id, revision DESC) WHERE file_provider_visible = 1;
CREATE INDEX object_versions_compaction
    ON object_versions(tenant, revision, object_id);
CREATE INDEX object_versions_live_blob
    ON object_versions(hash) WHERE kind = 2 AND tombstone = 0;
CREATE TRIGGER IF NOT EXISTS object_versions_immutable
    BEFORE UPDATE ON object_versions
    BEGIN SELECT RAISE(ABORT, 'object revisions are immutable'); END;

CREATE TABLE IF NOT EXISTS content_stages (
    stage_id BLOB PRIMARY KEY CHECK (length(stage_id) = 16),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    mutation_id BLOB CHECK (mutation_id IS NULL OR length(mutation_id) = 32),
    source_operation_id BLOB CHECK (source_operation_id IS NULL OR length(source_operation_id) = 16),
    temp_name TEXT NOT NULL,
    hash BLOB CHECK (hash IS NULL OR length(hash) = 32),
    size INTEGER CHECK (size IS NULL OR size >= 0),
    published INTEGER NOT NULL CHECK (published IN (0, 1)),
    CHECK ((published = 0 AND hash IS NULL AND size IS NULL) OR
           (published = 1 AND hash IS NOT NULL AND size IS NOT NULL)),
    CHECK (mutation_id IS NULL OR source_operation_id IS NULL),
    FOREIGN KEY (owner_id) REFERENCES catalog_generations(owner_id)
);
CREATE INDEX content_stages_temp_name
    ON content_stages(temp_name);
CREATE INDEX content_stages_owner
    ON content_stages(owner_id, stage_id);
CREATE INDEX content_stages_published_hash
    ON content_stages(hash) WHERE published = 1;
CREATE INDEX content_stages_pending_owner
    ON content_stages(owner_id) WHERE published = 0;
CREATE INDEX content_stages_orphan_owner
    ON content_stages(owner_id)
    WHERE mutation_id IS NULL AND source_operation_id IS NULL;
CREATE INDEX content_stages_mutation
    ON content_stages(mutation_id) WHERE mutation_id IS NOT NULL;
CREATE INDEX content_stages_source_operation
    ON content_stages(source_operation_id) WHERE source_operation_id IS NOT NULL;

CREATE TABLE blob_gc_candidates (
    hash BLOB PRIMARY KEY CHECK (length(hash) = 32)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS changes (
    tenant TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
	 scope_kind INTEGER NOT NULL CHECK (scope_kind IN (1, 2)),
	 presentation INTEGER NOT NULL CHECK (presentation IN (1, 2)),
	 scope_parent BLOB NOT NULL CHECK (length(scope_parent) = 16),
	 scope_domain TEXT NOT NULL,
	 scope_generation INTEGER NOT NULL CHECK (scope_generation >= 0),
    sequence INTEGER NOT NULL CHECK (sequence >= 0),
    kind INTEGER NOT NULL CHECK (kind IN (1, 2)),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
	 object_revision INTEGER NOT NULL CHECK (object_revision > 0),
	 PRIMARY KEY (tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation, sequence),
	 CHECK ((scope_kind = 1 AND presentation = 2 AND length(scope_domain) > 0 AND scope_generation > 0)
	     OR (scope_kind = 2 AND scope_domain = '' AND scope_generation = 0))
);
CREATE INDEX IF NOT EXISTS changes_scope_since
	 ON changes(tenant, scope_kind, presentation, scope_parent, scope_domain, scope_generation, revision, sequence);
CREATE INDEX changes_compaction
    ON changes(tenant, revision, sequence);

CREATE TABLE IF NOT EXISTS prepared_mutations (
    mutation_id BLOB PRIMARY KEY CHECK (length(mutation_id) = 32),
    tenant TEXT NOT NULL REFERENCES tenants(tenant),
    kind INTEGER NOT NULL CHECK (kind BETWEEN 2 AND 5),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    intent_json BLOB NOT NULL,
    source_id TEXT NOT NULL CHECK (length(source_id) > 0),
	source_context_json BLOB,
	source_result_json BLOB,
    expected_head INTEGER NOT NULL CHECK (expected_head > 0),
    state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 5),
    claim_owner BLOB CHECK (claim_owner IS NULL OR length(claim_owner) = 16),
    claim_epoch INTEGER CHECK (claim_epoch IS NULL OR claim_epoch > 0),
    CHECK ((claim_owner IS NULL AND claim_epoch IS NULL) OR
           (claim_owner IS NOT NULL AND claim_epoch IS NOT NULL)),
    CHECK (state = 1 OR (claim_owner IS NOT NULL AND claim_epoch IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS prepared_mutations_active_tenant
    ON prepared_mutations(tenant) WHERE state IN (1, 2, 3, 5);

CREATE TABLE IF NOT EXISTS mutation_journal (
    mutation_id BLOB PRIMARY KEY CHECK (length(mutation_id) = 32),
    tenant TEXT NOT NULL,
    kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 7),
    request_hash BLOB NOT NULL CHECK (length(request_hash) = 32),
    revision INTEGER NOT NULL CHECK (revision > 0),
    primary_object BLOB NOT NULL CHECK (length(primary_object) = 16),
    secondary_object BLOB CHECK (secondary_object IS NULL OR length(secondary_object) = 16)
);
CREATE INDEX IF NOT EXISTS mutation_journal_tenant_revision
    ON mutation_journal(tenant, revision, mutation_id);
CREATE INDEX mutation_journal_primary_gc
    ON mutation_journal(tenant, primary_object, revision);
CREATE INDEX mutation_journal_secondary_gc
    ON mutation_journal(tenant, secondary_object, revision)
    WHERE secondary_object IS NOT NULL;

CREATE TABLE mutation_pins (
    pin_id BLOB PRIMARY KEY CHECK (length(pin_id) = 16),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    session_owner TEXT NOT NULL CHECK (length(session_owner) BETWEEN 1 AND 256),
    tenant TEXT NOT NULL,
    mutation_id BLOB NOT NULL CHECK (length(mutation_id) = 32),
    target_revision INTEGER NOT NULL CHECK (target_revision > 0),
    closed INTEGER NOT NULL CHECK (closed IN (0, 1)),
    UNIQUE (owner_id, session_owner, mutation_id),
    FOREIGN KEY (owner_id, session_owner)
        REFERENCES retention_owners(owner_id, session_owner)
) STRICT;
CREATE INDEX mutation_pins_live
    ON mutation_pins(tenant, target_revision, mutation_id) WHERE closed = 0;
CREATE INDEX mutation_pins_compaction
    ON mutation_pins(tenant, mutation_id) WHERE closed = 0;
CREATE INDEX mutation_pins_owner_state
    ON mutation_pins(owner_id, session_owner, closed, tenant, pin_id);

CREATE TABLE IF NOT EXISTS handles (
    handle_id BLOB PRIMARY KEY CHECK (length(handle_id) = 16),
    owner_id BLOB NOT NULL CHECK (length(owner_id) = 16),
    session_owner TEXT NOT NULL CHECK (length(session_owner) BETWEEN 1 AND 256),
    tenant TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    object_revision INTEGER NOT NULL CHECK (object_revision > 0),
    opened_head INTEGER NOT NULL CHECK (opened_head >= object_revision),
    closed INTEGER NOT NULL CHECK (closed IN (0, 1)),
    FOREIGN KEY (owner_id, session_owner)
        REFERENCES retention_owners(owner_id, session_owner)
);
CREATE INDEX IF NOT EXISTS handles_object
    ON handles(tenant, object_id, object_revision) WHERE closed = 0;
CREATE INDEX handles_compaction
    ON handles(tenant, opened_head) WHERE closed = 0;
CREATE INDEX handles_owner_state
    ON handles(owner_id, session_owner, closed, tenant, handle_id);

CREATE TABLE IF NOT EXISTS materialization_interests (
    interest_id BLOB PRIMARY KEY CHECK (length(interest_id) = 16),
    tenant TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 16),
    owner_presentation INTEGER NOT NULL CHECK (owner_presentation IN (1, 2)),
    owner_domain TEXT NOT NULL CHECK (length(owner_domain) > 0),
    owner_generation INTEGER NOT NULL CHECK (owner_generation > 0),
    desired_revision INTEGER NOT NULL CHECK (desired_revision > 0),
    created_revision INTEGER NOT NULL CHECK (created_revision > 0),
    removed_revision INTEGER CHECK (removed_revision IS NULL OR removed_revision >= created_revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS materialization_interests_live
    ON materialization_interests(tenant, object_id, owner_presentation, owner_domain, owner_generation)
    WHERE removed_revision IS NULL;
CREATE INDEX IF NOT EXISTS materialization_interests_snapshot
	 ON materialization_interests(tenant, object_id, created_revision, removed_revision);
CREATE INDEX materialization_interests_removed_gc
    ON materialization_interests(tenant, removed_revision, object_id, interest_id)
    WHERE removed_revision IS NOT NULL;
CREATE INDEX materialization_interests_live_desired
    ON materialization_interests(tenant, desired_revision)
    WHERE removed_revision IS NULL;

CREATE INDEX changes_object_gc
    ON changes(tenant, object_id, revision);

CREATE TRIGGER catalog_maintenance_handle_closed
AFTER UPDATE OF closed ON handles
WHEN OLD.closed = 0 AND NEW.closed = 1
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT NEW.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = NEW.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_mutation_pin_closed
AFTER UPDATE OF closed ON mutation_pins
WHEN OLD.closed = 0 AND NEW.closed = 1
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT NEW.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = NEW.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_interest_removed
AFTER UPDATE OF removed_revision ON materialization_interests
WHEN OLD.removed_revision IS NULL AND NEW.removed_revision IS NOT NULL
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT NEW.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = NEW.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_domain_progress
AFTER UPDATE OF demanded, observed_catalog_revision ON convergence_engine_domains
WHEN NEW.demanded <> OLD.demanded
  OR NEW.observed_catalog_revision <> OLD.observed_catalog_revision
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT NEW.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = NEW.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_domain_deleted
AFTER DELETE ON convergence_engine_domains
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = OLD.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT OLD.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = OLD.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = OLD.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_lease_updated
AFTER UPDATE OF expires_unix_nano, tenant, domain_id, generation ON file_provider_leases
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = OLD.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT OLD.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = OLD.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = OLD.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_lease_deleted
AFTER DELETE ON file_provider_leases
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = OLD.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT OLD.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = OLD.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = OLD.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;

CREATE TRIGGER catalog_maintenance_applied_revision
AFTER UPDATE OF applied_revision ON tenant_state
WHEN NEW.applied_revision > OLD.applied_revision
BEGIN
    UPDATE catalog_maintenance_sequence
    SET next_ticket = next_ticket + 1
    WHERE singleton = 1
      AND NOT EXISTS (
          SELECT 1 FROM catalog_maintenance WHERE tenant = NEW.tenant
      );
    INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
    SELECT NEW.tenant, head, 0,
           COALESCE(
               (SELECT ticket FROM catalog_maintenance WHERE tenant = NEW.tenant),
               (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
           )
    FROM tenants WHERE tenant = NEW.tenant
    ON CONFLICT(tenant) DO UPDATE SET
        dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision);
END;
`

type failpoint func(string) error

// Catalog is the durable filesystem catalog.
type Catalog struct {
	db        *sql.DB
	readDB    *sql.DB
	blobDir   string
	owner     HandleOwnerID
	failpoint failpoint
	blobGates keyedGate
	storage   *storageState
	topology  *topologyNotifier
}

// Open opens or creates a durable WAL catalog at path.
func Open(ctx context.Context, path string) (*Catalog, error) {
	return OpenWithStorageLimits(ctx, path, DefaultStorageLimits())
}

func open(ctx context.Context, path string, fp failpoint) (*Catalog, error) {
	return openWithStorageLimits(ctx, path, fp, DefaultStorageLimits())
}

// OpenWithStorageLimits opens a catalog with explicit hard storage ceilings.
func OpenWithStorageLimits(ctx context.Context, path string, limits StorageLimits) (*Catalog, error) {
	return openWithStorageLimits(ctx, path, nil, limits)
}

func openWithStorageLimits(ctx context.Context, path string, fp failpoint, limits StorageLimits) (*Catalog, error) {
	if path == "" {
		return nil, errors.New("catalog: database path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("catalog: create database directory: %w", err)
	}
	blobDir := path + ".blobs"
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		return nil, fmt.Errorf("catalog: create blob directory: %w", err)
	}
	storage, err := newStorageState(blobDir, path, limits)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, true))
	if err != nil {
		return nil, fmt.Errorf("catalog: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	owner, err := newHandleOwnerID()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c := &Catalog{
		db: db, blobDir: blobDir, owner: owner, failpoint: fp, storage: storage,
		topology: newTopologyNotifier(),
	}
	if err := c.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := c.loadStorageAccounting(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := c.registerCatalogGeneration(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	readDB, err := sql.Open("sqlite", sqliteDSN(path, false))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("catalog: open read-only sqlite: %w", err)
	}
	readDB.SetMaxOpenConns(8)
	readDB.SetMaxIdleConns(8)
	if err := readDB.PingContext(ctx); err != nil {
		_ = readDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("catalog: connect read-only sqlite: %w", err)
	}
	c.readDB = readDB
	return c, nil
}

func sqliteDSN(path string, writer bool) string {
	location := &url.URL{Scheme: "file", Path: path}
	query := location.Query()
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	if writer {
		query.Add("_pragma", "synchronous(FULL)")
		query.Set("_txlock", "immediate")
	} else {
		query.Set("mode", "ro")
	}
	location.RawQuery = query.Encode()
	return location.String()
}

func (c *Catalog) initialize(ctx context.Context) error {
	var mode string
	if err := c.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("catalog: enable WAL: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("catalog: journal mode %q, want wal", mode)
	}
	if err := c.configureSQLiteStorage(ctx); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin schema validation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	digest := schemaDigest()
	var identityTable int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_schema
WHERE type = 'table' AND name = 'fusekit_schema'`).Scan(&identityTable); err != nil {
		return fmt.Errorf("catalog: inspect schema identity table: %w", err)
	}
	if identityTable == 1 {
		var stored string
		err = tx.QueryRowContext(ctx,
			"SELECT digest FROM fusekit_schema WHERE component = 'catalog'").Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: catalog schema identity is missing", ErrSchemaMismatch)
		}
		if err != nil {
			return fmt.Errorf("catalog: read schema identity: %w", err)
		}
		if stored != digest {
			return fmt.Errorf("%w: catalog digest %q, want %q", ErrSchemaMismatch, stored, digest)
		}
	} else {
		var objects int
		if countErr := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_schema
WHERE type IN ('table', 'index', 'trigger', 'view') AND name NOT LIKE 'sqlite_%'`).Scan(&objects); countErr != nil {
			return fmt.Errorf("catalog: inspect database schema: %w", countErr)
		}
		if objects != 0 {
			return fmt.Errorf("%w: database has %d unrecognized schema objects", ErrSchemaMismatch, objects)
		}
		if _, createErr := tx.ExecContext(ctx, schema); createErr != nil {
			return fmt.Errorf("catalog: initialize schema: %w", createErr)
		}
		if _, insertErr := tx.ExecContext(ctx,
			"INSERT INTO fusekit_schema(component, digest) VALUES ('catalog', ?)", digest); insertErr != nil {
			return fmt.Errorf("catalog: record schema identity: %w", insertErr)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit schema validation: %w", err)
	}
	if err := c.checkpointSQLiteStorage(ctx); err != nil {
		return err
	}
	return nil
}

func schemaDigest() string {
	digest := sha256.Sum256([]byte(schema))
	return hex.EncodeToString(digest[:])
}

// Close closes the catalog after all callers have stopped.
func (c *Catalog) Close() error {
	c.topology.close()
	var readErr error
	if c.readDB != nil {
		if err := c.readDB.Close(); err != nil {
			readErr = fmt.Errorf("catalog: close read-only sqlite: %w", err)
		}
	}
	var writeErr error
	if err := c.db.Close(); err != nil {
		writeErr = fmt.Errorf("catalog: close sqlite: %w", err)
	}
	return errors.Join(readErr, writeErr)
}

func (c *Catalog) trip(point string) error {
	if c.failpoint == nil {
		return nil
	}
	return c.failpoint(point)
}
