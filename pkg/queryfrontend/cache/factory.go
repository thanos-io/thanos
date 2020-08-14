// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"fmt"
	"strings"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
)

type ResponseCacheProvider string

// TODO: add other cache providers when available.
const (
	INMEMORY  ResponseCacheProvider = "IN-MEMORY"
	MEMCACHED ResponseCacheProvider = "MEMCACHED"
)

// ResponseCacheConfig specifies the response cache config.
type ResponseCacheConfig struct {
	Type   ResponseCacheProvider `yaml:"type"`
	Config interface{}           `yaml:"config"`
}

func NewResponseCacheConfig(confContentYaml []byte, logger log.Logger, reg prometheus.Registerer) (*queryrange.ResultsCacheConfig, error) {
	cacheConfig := &ResponseCacheConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	var resultsCache *queryrange.ResultsCacheConfig
	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):
		resultsCache, err = newInMemoryResponseCacheConfig(backendConfig)
	case string(MEMCACHED):
		resultsCache, err = newMemcachedCache(backendConfig, logger, reg)
	default:
		return nil, errors.Errorf("response cache with type %s is not supported", cacheConfig.Type)
	}

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s response cache", cacheConfig.Type))
	}

	return resultsCache, nil
}
