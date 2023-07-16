package objmeta

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
)

type parquetBackend struct {
	logger log.Logger
}

// NewParquetBackend create a backend which use parquet file as a database.
// TODO implement.
func NewParquetBackend(
	logger log.Logger,
	reg *prometheus.Registry,
	confContentYaml []byte,
) (Backend, error) {
	return &parquetBackend{
		logger: logger,
	}, nil
}

// SetBlockMeta set block meta.
func (m *parquetBackend) SetBlockMeta(ctx context.Context, blockMeta *objmetapb.BlockMeta) error {
	return nil
}

// GetBlockMeta get block meta.
func (m *parquetBackend) GetBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (*objmetapb.BlockMeta, error) {
	return nil, nil
}

// ExistsBlockMeta return true if block meta exist.
func (m *parquetBackend) ExistsBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	return false, nil
}

// DelBlockMeta delete block meta.
func (m *parquetBackend) DelBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error) {
	return false, nil
}

// DelBlockAllMeta delete block all type meta.
func (m *parquetBackend) DelBlockAllMeta(ctx context.Context, blockID string) error {
	return nil
}

// ListBlocks list block id list and invoke function f.
func (m *parquetBackend) ListBlocks(ctx context.Context, f func(s []string) error) error {
	return nil
}

// AcquireSyncLock Accrue a lock for sync. if return true, it accrued.
func (m *parquetBackend) AcquireSyncLock(ctx context.Context, maxTTL time.Duration) (bool, func() error, error) {
	// if backend is object bucket, not support lock, just choose by k8s pod_name first instance.
	if strings.HasSuffix(os.Getenv("POD_NAME"), "-0") {
		return true, func() error { return nil }, nil
	}
	return false, func() error { return nil }, nil
}
