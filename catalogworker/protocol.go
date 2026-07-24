package catalogworker

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/yasyf/fusekit/catalog"
	sqlite3 "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

const maxRemoteErrorBytes = 4 << 10

const protocolVersion = Version

// WorkerIdentity fences every request to one exact child process generation.
type WorkerIdentity struct {
	PID        int    `json:"pid"`
	StartTime  string `json:"start_time"`
	Boot       string `json:"boot"`
	Generation string `json:"generation"`
}

func (i WorkerIdentity) validate() error {
	if i.PID <= 1 || i.StartTime == "" || i.Boot == "" || i.Generation == "" {
		return errors.New("catalog worker: incomplete process identity")
	}
	return nil
}

type requestID [16]byte

type requestHeader struct {
	Protocol    uint16         `json:"protocol"`
	OperationID requestID      `json:"operation_id"`
	Worker      WorkerIdentity `json:"worker"`
}

func (h requestHeader) validate(identity WorkerIdentity) error {
	if h.Protocol != protocolVersion || h.OperationID == (requestID{}) {
		return errors.New("catalog worker: invalid request header")
	}
	if err := h.Worker.validate(); err != nil {
		return err
	}
	if h.Worker != identity {
		return errors.New("catalog worker: request addressed a different process generation")
	}
	return nil
}

type responseHeader struct {
	Protocol    uint16       `json:"protocol"`
	OperationID requestID    `json:"operation_id"`
	Error       *remoteError `json:"error,omitempty"`
}

type remoteError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message"`
}

func encodeRemoteError(err error) *remoteError {
	if err == nil {
		return nil
	}
	message := boundedRemoteErrorMessage(err.Error())
	for code, sentinel := range catalogErrors {
		if errors.Is(err, sentinel) {
			return &remoteError{Code: code, Message: message}
		}
	}
	var sqliteErr *sqlite3.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code() == sqlite3lib.SQLITE_FULL {
		return &remoteError{Code: "storage_quota", Message: message}
	}
	return &remoteError{Message: message}
}

func boundedRemoteErrorMessage(message string) string {
	if len(message) <= maxRemoteErrorBytes {
		return message
	}
	message = message[:maxRemoteErrorBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

func decodeRemoteError(encoded *remoteError) error {
	if encoded == nil {
		return nil
	}
	if sentinel := catalogErrors[encoded.Code]; sentinel != nil {
		return fmt.Errorf("%w: %s", sentinel, encoded.Message)
	}
	return errors.New(encoded.Message)
}

var catalogErrors = map[string]error{
	"not_found":                         catalog.ErrNotFound,
	"conflict":                          catalog.ErrConflict,
	"invalid_object":                    catalog.ErrInvalidObject,
	"invalid_transition":                catalog.ErrInvalidTransition,
	"integrity":                         catalog.ErrIntegrity,
	"mutation_conflict":                 catalog.ErrMutationConflict,
	"mutation_expired":                  catalog.ErrMutationExpired,
	"handle_closed":                     catalog.ErrHandleClosed,
	"state_not_found":                   catalog.ErrStateNotFound,
	"state_conflict":                    catalog.ErrStateConflict,
	"generation_mismatch":               catalog.ErrGenerationMismatch,
	"mutation_active":                   catalog.ErrMutationActive,
	"mutation_claimed":                  catalog.ErrMutationClaimed,
	"schema_mismatch":                   catalog.ErrSchemaMismatch,
	"storage_quota":                     catalog.ErrStorageQuota,
	"tenant_provision_conflict":         catalog.ErrTenantProvisionConflict,
	"tenant_owner_mismatch":             catalog.ErrTenantOwnerMismatch,
	"tenant_lifecycle_stale":            catalog.ErrTenantLifecycleStale,
	"tenant_lifecycle_retry_deferred":   catalog.ErrTenantLifecycleRetryDeferred,
	"tenant_mutation_conflict":          catalog.ErrTenantMutationConflict,
	"tenant_targeting_changed":          catalog.ErrTenantTargetingChanged,
	"tenant_preparation_owner_conflict": catalog.ErrTenantPreparationOwnershipConflict,
	"source_predecessor":                catalog.ErrSourcePredecessor,
	"source_requires_snapshot":          catalog.ErrSourceRequiresSnapshot,
	"source_locator_missing":            catalog.ErrSourceLocatorMissing,
	"source_locator_stale":              catalog.ErrSourceLocatorStale,
	"source_observer_snapshot_required": catalog.ErrSourceObserverSnapshotRequired,
	"source_observer_conflict":          catalog.ErrSourceObserverConflict,
	"source_observer_fence_changed":     catalog.ErrSourceObserverFenceChanged,
	"source_observer_inbox_coalesced":   catalog.ErrSourceObserverInboxCoalesced,
	"topology_revision_stale":           catalog.ErrTopologyRevisionStale,
}
