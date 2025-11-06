// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"
	"github.com/thanos-io/thanos/pkg/pantheon"
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  interface{}
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

func TestValidatePantheonV2WriterConfig(t *testing.T) {
	t.Parallel()

	validConfig := PantheonV2WriterConfig{
		Hashrings: []HashringConfig{
			{
				Endpoints: []Endpoint{{Address: "node1"}},
			},
		},
		PantheonCluster: &pantheon.PantheonCluster{
			DeletionDate: "",
			MetricScopes: []pantheon.MetricScope{
				{
					ScopeName: "test-scope",
					Shards:    2,
				},
			},
			DBGroups: []pantheon.DbGroup{
				{
					DbGroupName: "test-db-group",
					Replicas:    3,
					DbHpa: pantheon.DbHpaConfig{
						Enabled:     true,
						MaxReplicas: 10,
						MinReplicas: 1,
					},
					TenantSets: []pantheon.TenantSet{
						{
							MetricScopeName:   "test-scope",
							SpecialGroupNames: []string{},
							Shards:            []int{0, 1},
						},
					},
				},
			},
		},
	}

	for _, tc := range []struct {
		name string
		cfg  interface{}
		err  error
	}{
		{
			name: "empty hashrings",
			cfg: PantheonV2WriterConfig{
				Hashrings: []HashringConfig{},
			},
			err: errors.New("hashrings cannot be empty"),
		},
		{
			name: "unparsable config",
			cfg:  struct{}{},
			err:  errParseConfigurationFile,
		},
		{
			name: "valid config",
			cfg:  validConfig,
			err:  nil,
		},
		{
			name: "hashring with no endpoints",
			cfg: PantheonV2WriterConfig{
				Hashrings: []HashringConfig{
					{
						Endpoints: []Endpoint{},
					},
				},
			},
			err: errors.New("hashring 0 has no endpoints"),
		},
		{
			name: "valid config without pantheon cluster",
			cfg: PantheonV2WriterConfig{
				Hashrings: []HashringConfig{
					{
						Endpoints: []Endpoint{{Address: "node1"}},
					},
				},
			},
			err: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content, err := json.Marshal(tc.cfg)
			testutil.Ok(t, err)

			tmpfile, err := os.CreateTemp("", "pantheonv2_writer_configwatcher_test.*.json")
			testutil.Ok(t, err)

			defer func() {
				testutil.Ok(t, os.Remove(tmpfile.Name()))
			}()

			_, err = tmpfile.Write(content)
			testutil.Ok(t, err)

			err = tmpfile.Close()
			testutil.Ok(t, err)

			cw, err := NewPantheonV2WriterConfigWatcher(nil, nil, tmpfile.Name(), 1)
			testutil.Ok(t, err)
			defer cw.Stop()

			err = cw.ValidateConfig()
			if tc.err != nil {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}

func TestParsePantheonV2WriterConfig(t *testing.T) {
	t.Parallel()

	validConfig := PantheonV2WriterConfig{
		Hashrings: []HashringConfig{
			{
				Hashring: "pantheon-db-a0",
				Endpoints: []Endpoint{
					{Address: "node1", AZ: "az-1"},
					{Address: "node2", AZ: "az-2"},
				},
			},
		},
		PantheonCluster: &pantheon.PantheonCluster{
			DeletionDate: "",
			MetricScopes: []pantheon.MetricScope{
				{
					ScopeName: "hgcp",
					Shards:    3,
					SpecialMetricGroups: []pantheon.SpecialMetricGroup{
						{
							GroupName:   "kube-metrics",
							MetricNames: []string{"container_cpu_usage_seconds_total"},
						},
					},
				},
			},
			DBGroups: []pantheon.DbGroup{
				{
					DbGroupName: "pantheon-db-a0",
					Replicas:    5,
					DbHpa: pantheon.DbHpaConfig{
						Enabled:     true,
						MaxReplicas: 15,
						MinReplicas: 3,
					},
					TenantSets: []pantheon.TenantSet{
						{
							MetricScopeName:   "hgcp",
							SpecialGroupNames: []string{"kube-metrics"},
							Shards:            []int{0, 1, 2},
						},
					},
				},
			},
		},
	}

	content, err := json.Marshal(validConfig)
	testutil.Ok(t, err)

	config, err := parsePantheonV2WriterConfig(content)
	testutil.Ok(t, err)
	testutil.Assert(t, config != nil, "config should not be nil")
	testutil.Equals(t, 1, len(config.Hashrings), "should have 1 hashring")
	testutil.Equals(t, "pantheon-db-a0", config.Hashrings[0].Hashring, "hashring name should match")
	testutil.Equals(t, 2, len(config.Hashrings[0].Endpoints), "should have 2 endpoints")
	testutil.Assert(t, config.PantheonCluster != nil, "pantheon cluster should not be nil")
	testutil.Equals(t, "", config.PantheonCluster.DeletionDate, "cluster should have empty deletion date")
	testutil.Equals(t, 1, len(config.PantheonCluster.MetricScopes), "should have 1 metric scope")
	testutil.Equals(t, "hgcp", config.PantheonCluster.MetricScopes[0].ScopeName, "scope name should be hgcp")
	testutil.Equals(t, 3, config.PantheonCluster.MetricScopes[0].Shards, "should have 3 shards")
}

func TestParsePantheonV2WriterConfigErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		content   string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "invalid JSON",
			content:   `{invalid json}`,
			expectErr: true,
			errMsg:    "unmarshal error",
		},
		{
			name:      "empty hashrings",
			content:   `{"hashrings": []}`,
			expectErr: true,
			errMsg:    "hashrings cannot be empty",
		},
		{
			name: "hashring with no endpoints",
			content: `{
				"hashrings": [{
					"hashring": "test-hashring",
					"endpoints": []
				}]
			}`,
			expectErr: true,
			errMsg:    "hashring 0 has no endpoints",
		},
		{
			name: "valid config without pantheon cluster",
			content: `{
				"hashrings": [{
					"endpoints": [{"address": "node1"}]
				}]
			}`,
			expectErr: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePantheonV2WriterConfig([]byte(tc.content))
			if tc.expectErr {
				testutil.NotOk(t, err, "expected error for case: "+tc.name)
			} else {
				testutil.Ok(t, err)
			}
		})
	}
}
