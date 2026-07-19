package catalog

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	defaultDatabaseBytes         int64 = 16 << 30
	defaultWALBytes              int64 = 256 << 20
	defaultWALCheckpointBytes    int64 = 16 << 20
	defaultObjectContentBytes    int64 = 64 << 30
	defaultTemporaryContentBytes int64 = 64 << 30
	defaultPublishedContentBytes int64 = 128 << 30
	defaultTotalContentBytes     int64 = 192 << 30
)

// StorageLimits are hard per-catalog storage ceilings.
type StorageLimits struct {
	DatabaseBytes         int64
	WALBytes              int64
	WALCheckpointBytes    int64
	ObjectContentBytes    int64
	TemporaryContentBytes int64
	PublishedContentBytes int64
	TotalContentBytes     int64
}

// DefaultStorageLimits returns the production catalog storage ceilings.
func DefaultStorageLimits() StorageLimits {
	return StorageLimits{
		DatabaseBytes:         defaultDatabaseBytes,
		WALBytes:              defaultWALBytes,
		WALCheckpointBytes:    defaultWALCheckpointBytes,
		ObjectContentBytes:    defaultObjectContentBytes,
		TemporaryContentBytes: defaultTemporaryContentBytes,
		PublishedContentBytes: defaultPublishedContentBytes,
		TotalContentBytes:     defaultTotalContentBytes,
	}
}

// StorageQuotaError describes the exact storage ceiling that rejected growth.
type StorageQuotaError struct {
	Resource  string
	Limit     int64
	Used      int64
	Requested int64
}

func (e *StorageQuotaError) Error() string {
	return fmt.Sprintf("%v: %s limit=%d used=%d requested=%d", ErrStorageQuota, e.Resource, e.Limit, e.Used, e.Requested)
}

// Unwrap makes StorageQuotaError match ErrStorageQuota.
func (e *StorageQuotaError) Unwrap() error { return ErrStorageQuota }

func (l StorageLimits) validate() error {
	values := []struct {
		name  string
		value int64
	}{
		{name: "database", value: l.DatabaseBytes},
		{name: "WAL", value: l.WALBytes},
		{name: "WAL checkpoint", value: l.WALCheckpointBytes},
		{name: "object content", value: l.ObjectContentBytes},
		{name: "temporary content", value: l.TemporaryContentBytes},
		{name: "published content", value: l.PublishedContentBytes},
		{name: "total content", value: l.TotalContentBytes},
	}
	for _, value := range values {
		if value.value <= 0 {
			return fmt.Errorf("catalog: %s storage limit must be positive", value.name)
		}
	}
	if l.WALCheckpointBytes > l.WALBytes {
		return errors.New("catalog: WAL checkpoint threshold exceeds WAL limit")
	}
	if l.ObjectContentBytes > l.TemporaryContentBytes ||
		l.TemporaryContentBytes > l.TotalContentBytes ||
		l.PublishedContentBytes > l.TotalContentBytes {
		return errors.New("catalog: inconsistent content storage limits")
	}
	return nil
}

type storageUsage struct {
	published int64
	temporary int64
}

type storageState struct {
	mu     sync.Mutex
	dir    string
	dbPath string
	limits StorageLimits
	usage  storageUsage
}

func newStorageState(dir, dbPath string, limits StorageLimits) (*storageState, error) {
	if err := limits.validate(); err != nil {
		return nil, err
	}
	return &storageState{dir: dir, dbPath: dbPath, limits: limits}, nil
}

func (c *Catalog) configureSQLiteStorage(ctx context.Context) error {
	var pageSize, pageCount int64
	if err := c.db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return fmt.Errorf("catalog: read SQLite page size: %w", err)
	}
	if err := c.db.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount); err != nil {
		return fmt.Errorf("catalog: read SQLite page count: %w", err)
	}
	if pageSize <= 0 {
		return fmt.Errorf("%w: invalid SQLite page size %d", ErrIntegrity, pageSize)
	}
	maxPages := c.storage.limits.DatabaseBytes / pageSize
	if maxPages <= 0 {
		return &StorageQuotaError{
			Resource: "database", Limit: c.storage.limits.DatabaseBytes,
			Used: pageCount * pageSize, Requested: pageSize,
		}
	}
	if pageCount > maxPages {
		return &StorageQuotaError{
			Resource: "database", Limit: c.storage.limits.DatabaseBytes,
			Used: pageCount * pageSize, Requested: 0,
		}
	}
	var configuredMax int64
	if err := c.db.QueryRowContext(ctx, fmt.Sprintf("PRAGMA max_page_count=%d", maxPages)).Scan(&configuredMax); err != nil {
		return fmt.Errorf("catalog: set SQLite page limit: %w", err)
	}
	if configuredMax != maxPages {
		return fmt.Errorf("%w: SQLite max pages %d, want %d", ErrIntegrity, configuredMax, maxPages)
	}
	checkpointPages := c.storage.limits.WALCheckpointBytes / pageSize
	if checkpointPages < 1 {
		checkpointPages = 1
	}
	var configuredCheckpoint int64
	if err := c.db.QueryRowContext(ctx,
		fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", checkpointPages)).Scan(&configuredCheckpoint); err != nil {
		return fmt.Errorf("catalog: set SQLite WAL checkpoint threshold: %w", err)
	}
	if configuredCheckpoint != checkpointPages {
		return fmt.Errorf("%w: SQLite WAL checkpoint pages %d, want %d", ErrIntegrity, configuredCheckpoint, checkpointPages)
	}
	var configuredWAL int64
	if err := c.db.QueryRowContext(ctx,
		fmt.Sprintf("PRAGMA journal_size_limit=%d", c.storage.limits.WALBytes)).Scan(&configuredWAL); err != nil {
		return fmt.Errorf("catalog: set SQLite WAL size limit: %w", err)
	}
	if configuredWAL != c.storage.limits.WALBytes {
		return fmt.Errorf("%w: SQLite WAL size limit %d, want %d", ErrIntegrity, configuredWAL, c.storage.limits.WALBytes)
	}
	return nil
}

// EnforceWorkerWALBudget checkpoints the WAL before another worker mutation
// can be admitted. The worker serializes calls so a pinned reader can permit
// at most one bounded mutation batch before the exact process is retired.
func (c *Catalog) EnforceWorkerWALBudget(ctx context.Context) error {
	size, err := c.walSize()
	if err != nil {
		return err
	}
	if size > c.storage.limits.WALBytes {
		return &StorageQuotaError{
			Resource: "WAL", Limit: c.storage.limits.WALBytes, Used: size,
		}
	}
	if size < c.storage.limits.WALCheckpointBytes {
		return nil
	}
	if err := c.checkpointSQLiteStorage(ctx); err != nil {
		return err
	}
	size, err = c.walSize()
	if err != nil {
		return err
	}
	if size > c.storage.limits.WALBytes {
		return &StorageQuotaError{
			Resource: "WAL", Limit: c.storage.limits.WALBytes, Used: size,
		}
	}
	if size >= c.storage.limits.WALCheckpointBytes {
		return fmt.Errorf("%w: SQLite WAL remained at %d bytes after truncate checkpoint", ErrIntegrity, size)
	}
	return nil
}

func (c *Catalog) walSize() (int64, error) {
	info, err := os.Stat(c.storage.dbPath + "-wal")
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("catalog: inspect SQLite WAL size: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 0 {
		return 0, fmt.Errorf("%w: invalid SQLite WAL file", ErrIntegrity)
	}
	return info.Size(), nil
}

func (c *Catalog) checkpointSQLiteStorage(ctx context.Context) error {
	var busy, logFrames, checkpointedFrames int64
	if err := c.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointedFrames); err != nil {
		return fmt.Errorf("catalog: checkpoint SQLite WAL: %w", err)
	}
	if busy != 0 {
		return fmt.Errorf("catalog: SQLite WAL checkpoint remained busy with %d frames (%d checkpointed)", logFrames, checkpointedFrames)
	}
	return nil
}

func validBlobStorageName(name string) bool {
	length := len(ContentHash{})
	if strings.HasPrefix(name, ".stage-") {
		name = strings.TrimPrefix(name, ".stage-")
		length = len(StageID{})
	}
	if len(name) != hex.EncodedLen(length) {
		return false
	}
	decoded, err := hex.DecodeString(name)
	return err == nil && len(decoded) == length
}
