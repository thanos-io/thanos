// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/cacheutil"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/internal/cortex/chunk/cache"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

func TestConfig_Validate(t *testing.T) {

	type testCase struct {
		name   string
		config Config
		err    string
	}

	testCases := []testCase{
		{
			name: "invalid query range options",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval: 10 * time.Hour,
					HorizontalShards:       10,
					MinQuerySplitInterval:  1 * time.Hour,
					MaxQuerySplitInterval:  day,
				},
			},
			err: "split queries interval and dynamic query split interval cannot be set at the same time",
		},
		{
			name: "invalid parameters for dynamic query range split",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval: 0,
					HorizontalShards:       0,
					MinQuerySplitInterval:  1 * time.Hour,
				},
			},
			err: "min horizontal shards should be greater than 0 when query split threshold is enabled",
		},
		{
			name: "invalid parameters for dynamic query range split - 2",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval: 0,
					HorizontalShards:       10,
					MaxQuerySplitInterval:  0,
					MinQuerySplitInterval:  1 * time.Hour,
				},
			},
			err: "max query split interval should be greater than 0 when query split threshold is enabled",
		},
		{
			name: "invalid parameters for dynamic query range split - 3",
			config: Config{
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval: 0,
					HorizontalShards:       10,
					MaxQuerySplitInterval:  1 * time.Hour,
					MinQuerySplitInterval:  0,
				},
				LabelsConfig: LabelsConfig{
					DefaultTimeRange: day,
				},
			},
			err: "min query split interval should be greater than 0 when query split threshold is enabled",
		},
		{
			name: "valid config with caching",
			config: Config{
				DownstreamURL: "localhost:8080",
				QueryRangeConfig: QueryRangeConfig{
					SplitQueriesByInterval: 10 * time.Hour,
					HorizontalShards:       0,
					MaxQuerySplitInterval:  0,
					MinQuerySplitInterval:  0,
					ResultsCacheConfig: &queryrange.ResultsCacheConfig{
						CacheConfig:                cache.Config{},
						Compression:                "",
						CacheQueryableSamplesStats: false,
					},
				},
				LabelsConfig: LabelsConfig{
					DefaultTimeRange: day,
				},
			},
			err: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.err != "" {
				testutil.NotOk(t, err)
				testutil.Equals(t, tc.err, err.Error())
			} else {
				testutil.Ok(t, err)
				fmt.Println(err)
			}
		})
	}
}

// Regression guard: the query-frontend response cache parses strictly while the store
// caches spell the same setting `ttl` (pkg/store/cache/factory.go). Writing `ttl` here
// used to abort startup with "field ttl not found in type ...ResponseCacheConfig".
func TestNewCacheConfig_TTLAlias(t *testing.T) {
	for _, tc := range []struct {
		name     string
		yaml     string
		err      string
		expected time.Duration
	}{
		{
			name:     "redis ttl is accepted as an alias for expiration",
			yaml:     "type: REDIS\nconfig:\n  addr: localhost:6379\n  ttl: 48h\n",
			expected: 48 * time.Hour,
		},
		{
			name:     "redis expiration still works",
			yaml:     "type: REDIS\nconfig:\n  addr: localhost:6379\n  expiration: 12h\n",
			expected: 12 * time.Hour,
		},
		{
			name: "redis rejects ttl and expiration together",
			yaml: "type: REDIS\nconfig:\n  addr: localhost:6379\n  ttl: 48h\n  expiration: 12h\n",
			err:  "must not be set at the same time",
		},
		{
			name:     "memcached ttl is accepted as an alias for expiration",
			yaml:     "type: MEMCACHED\nconfig:\n  addresses: [localhost:11211]\n  ttl: 48h\n",
			expected: 48 * time.Hour,
		},
		{
			name: "memcached rejects ttl and expiration together",
			yaml: "type: MEMCACHED\nconfig:\n  addresses: [localhost:11211]\n  ttl: 48h\n  expiration: 12h\n",
			err:  "must not be set at the same time",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := NewCacheConfig(log.NewNopLogger(), []byte(tc.yaml))
			if tc.err != "" {
				testutil.NotOk(t, err)
				testutil.Assert(t, strings.Contains(err.Error(), tc.err), "got %q, want it to contain %q", err.Error(), tc.err)
				return
			}
			testutil.Ok(t, err)
			got := cfg.Redis.Expiration
			if cfg.Memcache.Expiration != 0 {
				got = cfg.Memcache.Expiration
			}
			testutil.Equals(t, tc.expected, got)
		})
	}
}

// Regression guard: the e2e helpers build the cache config as a Go struct and marshal
// it, so both `expiration` and `ttl` appear in the YAML with zero values (cfggen forbids
// omitempty on config fields). A present but zero key must not be read as a conflict, or
// query-frontend refuses to start.
func TestNewCacheConfig_MarshalledStructRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  CacheProviderConfig
	}{
		{
			name: "memcached",
			cfg: CacheProviderConfig{
				Type:   MEMCACHED,
				Config: MemcachedResponseCacheConfig{Memcached: cacheutil.MemcachedClientConfig{Addresses: []string{"memcached:11211"}}},
			},
		},
		{
			name: "redis",
			cfg: CacheProviderConfig{
				Type:   REDIS,
				Config: RedisResponseCacheConfig{Redis: cacheutil.RedisClientConfig{Addr: "redis:6379"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := yaml.Marshal(tc.cfg)
			testutil.Ok(t, err)
			_, err = NewCacheConfig(log.NewNopLogger(), b)
			testutil.Ok(t, err)
		})
	}
}
