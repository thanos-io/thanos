// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecache

import (
	"regexp"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	cache "github.com/thanos-io/thanos/pkg/cache"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// BucketCacheProvider is a type used to evaluate all bucket cache providers.
type BucketCacheProvider string

const (
	MemcachedBucketCacheProvider BucketCacheProvider = "memcached" // Memcached cache-provider for caching bucket.
)

// CachingBucketWithBackendConfig is a configuration of caching bucket used by Store component.
type CachingBucketWithBackendConfig struct {
	Type          BucketCacheProvider `yaml:"backend"`
	BackendConfig interface{}         `yaml:"backend_config"`

	CachingBucketConfig CachingBucketConfig `yaml:"caching_config"`
}

// NewCachingBucketFromYaml uses YAML configuration to create new caching bucket.
func NewCachingBucketFromYaml(yamlContent []byte, bucket objstore.Bucket, logger log.Logger, reg prometheus.Registerer) (objstore.InstrumentedBucket, error) {
	level.Info(logger).Log("msg", "loading caching bucket configuration")

	config := &CachingBucketWithBackendConfig{}
	config.CachingBucketConfig = DefaultCachingBucketConfig()

	if err := yaml.UnmarshalStrict(yamlContent, config); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(config.BackendConfig)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	var c cache.Cache

	switch config.Type {
	case MemcachedBucketCacheProvider:
		var memcached cacheutil.MemcachedClient
		memcached, err := cacheutil.NewMemcachedClient(logger, "caching-bucket", backendConfig, reg)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create memcached client")
		}
		c = cache.NewMemcachedCache("caching-bucket", logger, memcached, reg)
	default:
		return nil, errors.Errorf("unsupported cache type: %s", config.Type)
	}

	cb, err := NewCachingBucket(bucket, logger, reg)
	if err != nil {
		return nil, err
	}

	// Configure cache.
	metaFilenameSuffix := "/" + metadata.MetaFilename
	deletionMarkFilenameSuffix := "/" + metadata.DeletionMarkFilename

	var isMetaFile = func(name string) bool {
		return strings.HasSuffix(name, metaFilenameSuffix) || strings.HasSuffix(name, deletionMarkFilenameSuffix)
	}

	chunksMatcher := regexp.MustCompile(`^.*/chunks/\d+$`)

	cb.CacheGetRange("chunks", c, func(name string) bool { return chunksMatcher.MatchString(name) }, config.CachingBucketConfig)
	cb.CacheExists("metafile", c, isMetaFile, config.CachingBucketConfig)
	cb.CacheGet("metafile", c, isMetaFile, config.CachingBucketConfig)
	cb.CacheIter("dir", c, func(dir string) bool { return dir == "" }, config.CachingBucketConfig) // Cache Iter requests for root.

	return cb, nil
}
