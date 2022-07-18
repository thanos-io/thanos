// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thanos-io/thanos/internal/cortex/util/flagext"
)

func TestIndexCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      IndexCacheConfig
		expected error
	}{
		"default config should pass": {
			cfg: func() IndexCacheConfig {
				cfg := IndexCacheConfig{}
				flagext.DefaultValues(&cfg)
				return cfg
			}(),
		},
		"unsupported backend should fail": {
			cfg: IndexCacheConfig{
				Backend: "xxx",
			},
			expected: errUnsupportedIndexCacheBackend,
		},
		"no memcached addresses should fail": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
			},
			expected: errNoIndexCacheAddresses,
		},
		"one memcached address should pass": {
			cfg: IndexCacheConfig{
				Backend: "memcached",
				Memcached: MemcachedClientConfig{
					Addresses: "dns+localhost:11211",
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}
