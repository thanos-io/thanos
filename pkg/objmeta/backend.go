package objmeta

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objmeta/objmetapb"
	"gopkg.in/yaml.v3"
)

// Backend store the object metadata and provider a reader.
type Backend interface {
	SetBlockMeta(ctx context.Context, blockMeta *objmetapb.BlockMeta) error
	GetBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (*objmetapb.BlockMeta, error)
	ExistsBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error)
	DelBlockMeta(ctx context.Context, blockID string, metaType objmetapb.Type) (bool, error)
	DelBlockAllMeta(ctx context.Context, blockID string) error
	ListBlocks(ctx context.Context, f func(s []string) error) error
	AcquireSyncLock(ctx context.Context, maxTTL time.Duration) (bool, func() error, error)
}

type objMetaBackendType string

const (
	objMetaBackendTypeRedis objMetaBackendType = "redis"
)

// NewBackend create a backend from conf.
func NewBackend(
	logger log.Logger,
	reg *prometheus.Registry,
	confContentYaml []byte,
) (Backend, error) {
	var config struct {
		Type          string      `yaml:"type"`
		BackendConfig interface{} `yaml:"config"`
	}
	if err := yaml.Unmarshal(confContentYaml, &config); err != nil {
		return nil, errors.Wrapf(err, "parse objMeta config content")
	}
	backendConfYaml, err := yaml.Marshal(config.BackendConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "encode backendConfig")
	}
	switch objMetaBackendType(strings.ToLower(config.Type)) {
	case objMetaBackendTypeRedis:
		backend, err := NewRedisBackend(logger, reg, backendConfYaml)
		if err != nil {
			return nil, err
		}
		return backend, nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupported objMeta backend config type:%s", config.Type))
	}
}
