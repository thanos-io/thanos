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
	t.Run("Endpoints as string slice", func(t *testing.T) {
		var endpoints []Endpoint
		testutil.Ok(t, json.Unmarshal([]byte(`["node-1"]`), &endpoints))
		testutil.Equals(t, endpoints, []Endpoint{{Address: "node-1"}})
	})
	t.Run("Endpoints as endpoints slice", func(t *testing.T) {
		var endpoints []Endpoint
		testutil.Ok(t, json.Unmarshal([]byte(`[{"address": "node-1", "az": "az-1"}]`), &endpoints))
		testutil.Equals(t, endpoints, []Endpoint{{Address: "node-1", AZ: "az-1"}})
	})
}
