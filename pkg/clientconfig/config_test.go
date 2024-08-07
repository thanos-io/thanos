// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package clientconfig

import (
	"testing"

	"github.com/efficientgo/core/testutil"
)

func TestBuildHTTPConfig(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		addresses []string
		err       bool
		expected  []Config
	}{
		{
			desc:      "single addr without path",
			addresses: []string{"localhost:9093"},
			expected: []Config{
				{
					HTTPConfig: HTTPConfig{
						EndpointsConfig: HTTPEndpointsConfig{
							StaticAddresses: []string{"localhost:9093"},
							Scheme:          "http",
						},
					},
				},
			},
		},
		{
			desc:      "1st addr without path, 2nd with",
			addresses: []string{"localhost:9093", "localhost:9094/prefix"},
			expected: []Config{
				{
					HTTPConfig: HTTPConfig{
						EndpointsConfig: HTTPEndpointsConfig{
							StaticAddresses: []string{"localhost:9093"},
							Scheme:          "http",
						},
					},
				},
				{
					HTTPConfig: HTTPConfig{
						EndpointsConfig: HTTPEndpointsConfig{
							StaticAddresses: []string{"localhost:9094"},
							Scheme:          "http",
							PathPrefix:      "/prefix",
						},
					},
				},
			},
		},
		{
			desc:      "single addr with path and http scheme",
			addresses: []string{"http://localhost:9093"},
			expected: []Config{
				{
					HTTPConfig: HTTPConfig{
						EndpointsConfig: HTTPEndpointsConfig{
							StaticAddresses: []string{"localhost:9093"},
							Scheme:          "http",
						},
					},
				},
			},
		},
		{
			desc:      "single addr with path and https scheme",
			addresses: []string{"https://localhost:9093"},
			expected: []Config{
				{
					HTTPConfig: HTTPConfig{
						EndpointsConfig: HTTPEndpointsConfig{
							StaticAddresses: []string{"localhost:9093"},
							Scheme:          "https",
						},
					},
				},
			},
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
			cfg, err := BuildConfigFromHTTPAddresses(tc.addresses)
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.expected, cfg)
		})
	}
}

func TestBuildGRPCConfig(t *testing.T) {
	for _, tc := range []struct {
		desc      string
		addresses []string
		err       bool
		expected  []Config
	}{
		{
			desc:      "single addr",
			addresses: []string{"localhost:9093"},
			expected: []Config{
				{
					GRPCConfig: &GRPCConfig{
						EndpointAddrs: []string{"localhost:9093"},
					},
				},
			},
		},
		{
			desc:      "multiple addr",
			addresses: []string{"localhost:9093", "localhost:9094"},
			expected: []Config{
				{
					GRPCConfig: &GRPCConfig{
						EndpointAddrs: []string{"localhost:9093"},
					},
				},
				{
					GRPCConfig: &GRPCConfig{
						EndpointAddrs: []string{"localhost:9094"},
					},
				},
			},
		},
		{
			desc:      "empty addr",
			addresses: []string{""},
			err:       true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := BuildConfigFromGRPCAddresses(tc.addresses)
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.expected, cfg)
		})
	}
}
