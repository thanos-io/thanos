package alert

import (
	"testing"
	"time"

	"github.com/thanos-io/thanos/pkg/http"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestBuildAlertmanagerConfiguration(t *testing.T) {
	for _, tc := range []struct {
		address string

		err      bool
		expected AlertmanagerConfig
	}{
		{
			address: "http://localhost:9093",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"localhost:9093"},
				Scheme:          "http",
			},
		},
		{
			address: "https://am.example.com",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"am.example.com"},
				Scheme:          "https",
			},
		},
		{
			address: "dns+http://localhost:9093",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"dns+localhost:9093"},
				Scheme:          "http",
			},
		},
		{
			address: "dnssrv+http://localhost",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"dnssrv+localhost"},
				Scheme:          "http",
			},
		},
		{
			address: "ssh+http://localhost",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"localhost"},
				Scheme:          "ssh+http",
			},
		},
		{
			address: "dns+https://localhost/path/prefix/",
			expected: AlertmanagerConfig{
				StaticAddresses: []string{"dns+localhost:9093"},
				Scheme:          "https",
				PathPrefix:      "/path/prefix/",
			},
		},
		{
			address: "http://user:pass@localhost:9093",
			expected: AlertmanagerConfig{
				HTTPClientConfig: http.ClientConfig{
					BasicAuth: http.BasicAuth{
						Username: "user",
						Password: "pass",
					},
				},
				StaticAddresses: []string{"localhost:9093"},
				Scheme:          "http",
			},
		},
		{
			address: "://user:pass@localhost:9093",
			err:     true,
		},
	} {
		t.Run(tc.address, func(t *testing.T) {
			cfg, err := BuildAlertmanagerConfig(nil, tc.address, time.Duration(0))
			if tc.err {
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.expected, cfg)
		})
	}
}
