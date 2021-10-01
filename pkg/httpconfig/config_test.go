// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package httpconfig

import (
	"testing"

	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBuildConfig(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		addresses []string
		err       bool
		expected  []Config
	}{
		{
			desc:      "single addr without path",
			addresses: []string{"localhost:9093"},
			expected: []Config{{
				EndpointsConfig: EndpointsConfig{
					StaticAddresses: []string{"localhost:9093"},
					Scheme:          "http",
				},
			}},
		},
		{
			desc:      "1st addr without path, 2nd with",
			addresses: []string{"localhost:9093", "localhost:9094/prefix"},
			expected: []Config{
				{
					EndpointsConfig: EndpointsConfig{
						StaticAddresses: []string{"localhost:9093"},
						Scheme:          "http",
					},
				},
				{
					EndpointsConfig: EndpointsConfig{
						StaticAddresses: []string{"localhost:9094"},
						Scheme:          "http",
						PathPrefix:      "/prefix",
					},
				},
			},
		},
		{
			desc:      "single addr with path and http scheme",
			addresses: []string{"http://localhost:9093"},
			expected: []Config{{
				EndpointsConfig: EndpointsConfig{
					StaticAddresses: []string{"localhost:9093"},
					Scheme:          "http",
				},
			}},
		},
		{
			desc:      "single addr with path and https scheme",
			addresses: []string{"https://localhost:9093"},
			expected: []Config{{
				EndpointsConfig: EndpointsConfig{
					StaticAddresses: []string{"localhost:9093"},
					Scheme:          "https",
				},
			}},
		},
		{
			desc:      "not supported scheme",
			addresses: []string{"ttp://localhost:9093"},
			err:       true,
		},
		{
			desc:      "invalid addr",
			addresses: []string{"this is not a valid addr"},
			err:       true,
		},
		{
			desc:      "empty addr",
			addresses: []string{""},
			err:       true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := BuildConfig(tc.addresses)
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.expected, cfg)
		})
	}
}
