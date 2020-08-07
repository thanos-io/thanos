// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cache

import (
	"fmt"
	"strings"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type ResponseCacheProvider string

// TODO: add other cache providers when available.
const (
	INMEMORY ResponseCacheProvider = "IN-MEMORY"
)

// ResponseCacheConfig specifies the response cache config.
type ResponseCacheConfig struct {
	Type   ResponseCacheProvider `yaml:"type"`
	Config interface{}           `yaml:"config"`
}

func NewResponseCacheConfig(confContentYaml []byte) (*queryrange.ResultsCacheConfig, error) {
	cacheConfig := &ResponseCacheConfig{}
	if err := yaml.UnmarshalStrict(confContentYaml, cacheConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}

	backendConfig, err := yaml.Marshal(cacheConfig.Config)
	if err != nil {
		return nil, errors.Wrap(err, "marshal content of cache backend configuration")
	}

	var resultsCacheConf *queryrange.ResultsCacheConfig
	switch strings.ToUpper(string(cacheConfig.Type)) {
	case string(INMEMORY):
		resultsCacheConf, err = newInMemoryResponseCacheConfig(backendConfig)
	default:
		return nil, errors.Errorf("response cache with type %s is not supported", cacheConfig.Type)
	}

	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("create %s response cache", cacheConfig.Type))
	}

	return resultsCacheConf, nil
}
