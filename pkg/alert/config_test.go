// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package alert

import (
	"testing"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/httpconfig"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestUnmarshalAPIVersion(t *testing.T) {
	for _, tc := range []struct {
		v string

		err      bool
		expected APIVersion
	}{
		{
			v:        "v1",
			expected: APIv1,
		},
		{
			v:   "v3",
			err: true,
		},
		{
			v:   "{}",
			err: true,
		},
	} {
		var got APIVersion
		err := yaml.Unmarshal([]byte(tc.v), &got)
		if tc.err {
			testutil.NotOk(t, err)
			continue
		}
		testutil.Ok(t, err)
		testutil.Equals(t, tc.expected, got)
	}
}

func TestBuildAlertmanagerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		address string

		err      bool
		expected AlertmanagerConfig
	}{
		{
			address: "http://localhost:9093",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"localhost:9093"},
					Scheme:          "http",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "https://am.example.com",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"am.example.com"},
					Scheme:          "https",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "dns+http://localhost:9093",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"dns+localhost:9093"},
					Scheme:          "http",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "dnssrv+http://localhost",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"dnssrv+localhost"},
					Scheme:          "http",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "ssh+http://localhost",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"localhost"},
					Scheme:          "ssh+http",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "dns+https://localhost/path/prefix/",
			expected: AlertmanagerConfig{
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"dns+localhost:9093"},
					Scheme:          "https",
					PathPrefix:      "/path/prefix/",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "http://user:pass@localhost:9093",
			expected: AlertmanagerConfig{
				HTTPClientConfig: httpconfig.ClientConfig{
					BasicAuth: httpconfig.BasicAuth{
						Username: "user",
						Password: "pass",
					},
				},
				EndpointsConfig: httpconfig.EndpointsConfig{
					StaticAddresses: []string{"localhost:9093"},
					Scheme:          "http",
				},
				APIVersion: APIv1,
			},
		},
		{
			address: "://user:pass@localhost:9093",
			err:     true,
		},
		{
			address: "http://user:pass@",
			err:     true,
		},
		{
			address: "dnssrv+_http._tcp.example.com",
			err:     true,
		},
	} {
		t.Run(tc.address, func(t *testing.T) {
			cfg, err := BuildAlertmanagerConfig(tc.address, time.Duration(0))
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.expected, cfg)
		})
	}
}
