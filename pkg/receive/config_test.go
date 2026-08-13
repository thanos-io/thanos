// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  any
		err  error
	}{
		{
			name: "<nil> config",
			cfg:  nil,
			err:  errEmptyConfigurationFile,
		},
		{
			name: "empty config",
			cfg:  []HashringConfig{},
			err:  errEmptyConfigurationFile,
		},
		{
			name: "unparsable config",
			cfg:  struct{}{},
			err:  errParseConfigurationFile,
		},
		{
			name: "valid config",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			err: nil, // means it's valid.
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content, err := json.Marshal(tc.cfg)
			testutil.Ok(t, err)

			tmpfile, err := os.CreateTemp("", "configwatcher_test.*.json")
			testutil.Ok(t, err)

			defer func() {
				testutil.Ok(t, os.Remove(tmpfile.Name()))
			}()

			_, err = tmpfile.Write(content)
			testutil.Ok(t, err)

			err = tmpfile.Close()
			testutil.Ok(t, err)

			cw, err := NewConfigWatcher(nil, nil, tmpfile.Name(), 1)
			testutil.Ok(t, err)
			defer cw.Stop()

			if err := cw.ValidateConfig(); err != nil && !errors.Is(err, tc.err) {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
			}
		})
	}
}

// TestParseConfig verifies that ParseConfig successfully unmarshals raw JSON
// configuration and executes a post-parsing normalization pass. Omitted
// "tenant_matcher_type" fields (both top-level and inside overrides) must
// automatically hydrate to "exact" to prevent downstream routing bugs.
func TestParseConfig(t *testing.T) {
	// JSON payload testing three distinct cases:
	// 1. Explicit "exact" matcher (should remain unchanged)
	// 2. Missing "tenant_matcher_type" (should trigger hydration to "exact")
	// 3. Explicit "glob" matcher (should preserve non-default matchers)
	inputJSON := []byte(`[
    {
        "hashring": "test-ring",
        "endpoints": ["node-1"],
        "shuffle_sharding_config": {
            "shard_size": 2,
            "overrides": [
                {
                    "shard_size": 3,
                    "tenants": ["tenant-1"],
                    "tenant_matcher_type": "exact"
                },
                {
                  "shard_size": 4,
                  "tenants": ["tenant-2"]
                },
                {
                    "shard_size": 5,
                    "tenants": ["tenant-3"],
                    "tenant_matcher_type": "glob"
                }
            ]
        }
    }
]`)
	configs, err := ParseConfig(inputJSON)
	testutil.Ok(t, err)
	// Assert that ParseConfig returns fully normalized data:
	// - Top-level TenantMatcherType is hydrated to TenantMatcherTypeExact.
	// - Omitted override matcher ("tenant-2") is hydrated to TenantMatcherTypeExact.
	// - Explicit matchers ("tenant-1", "tenant-3") retain their exact values.
	expected := []HashringConfig{
		{
			Hashring:          "test-ring",
			TenantMatcherType: TenantMatcherTypeExact,
			Endpoints: []Endpoint{
				{
					Address:          "node-1",
					CapNProtoAddress: "node-1:19391",
				},
			},
			ShuffleShardingConfig: ShuffleShardingConfig{
				ShardSize: 2,
				Overrides: []ShuffleShardingOverrideConfig{
					{
						ShardSize:         3,
						Tenants:           []string{"tenant-1"},
						TenantMatcherType: TenantMatcherTypeExact,
					},
					{
						ShardSize:         4,
						Tenants:           []string{"tenant-2"},
						TenantMatcherType: TenantMatcherTypeExact,
					},
					{
						ShardSize:         5,
						Tenants:           []string{"tenant-3"},
						TenantMatcherType: TenantMatcherGlob,
					},
				},
			},
		},
	}
	testutil.Equals(t, expected, configs)
}

func TestUnmarshalEndpointSlice(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		json      string
		endpoints []Endpoint
		expectErr bool
	}{
		{
			name:      "Endpoint with empty address",
			json:      `[{"az": "az-1"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391"}},
			expectErr: true,
		},
		{
			name:      "Endpoints as string slice",
			json:      `["node-1"]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391"}},
		},
		{
			name:      "Endpoints as endpoints slice",
			json:      `[{"address": "node-1", "az": "az-1"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:19391", AZ: "az-1"}},
		},
		{
			name:      "Endpoints as string slice with port",
			json:      `["node-1:80"]`,
			endpoints: []Endpoint{{Address: "node-1:80", CapNProtoAddress: "node-1:19391"}},
		},
		{
			name:      "Endpoints as string slice with capnproto port",
			json:      `[{"address": "node-1", "capnproto_address": "node-1:81"}]`,
			endpoints: []Endpoint{{Address: "node-1", CapNProtoAddress: "node-1:81"}},
		},
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			var endpoints []Endpoint
			err := json.Unmarshal([]byte(tcase.json), &endpoints)
			if tcase.expectErr {
				testutil.NotOk(t, err)
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.endpoints, endpoints)
		})
	}
}
