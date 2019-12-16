package alert

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
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
				StaticAddresses: []string{"dns+localhost"},
				Scheme:          "https",
				PathPrefix:      "/path/prefix/",
			},
		},
		{
			address: "http://user:pass@localhost:9093",
			expected: AlertmanagerConfig{
				HTTPClientConfig: HTTPClientConfig{
					BasicAuth: BasicAuth{
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

type mockEntry struct {
	name  string
	qtype dns.QType
}

type mockResolver struct {
	entries map[mockEntry][]string
	err     error
}

func (m mockResolver) Resolve(ctx context.Context, name string, qtype dns.QType) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if res, ok := m.entries[mockEntry{name: name, qtype: qtype}]; ok {
		return res, nil
	}
	return nil, errors.Errorf("mockResolver not found response for name: %s", name)
}

func TestUpdate(t *testing.T) {
	for _, tc := range []struct {
		cfg      AlertmanagerConfig
		resolver mockResolver

		resolved []string
		err      bool
	}{
		{
			cfg: AlertmanagerConfig{
				StaticAddresses: []string{"dns+alertmanager.example.com:9095"},
			},
			resolver: mockResolver{
				entries: map[mockEntry][]string{
					mockEntry{name: "alertmanager.example.com:9095", qtype: dns.A}: []string{"1.1.1.1:9095", "2.2.2.2:9095"},
				},
			},
			resolved: []string{"1.1.1.1:9095", "2.2.2.2:9095"},
		},
		{
			cfg: AlertmanagerConfig{
				StaticAddresses: []string{"dns+alertmanager.example.com"},
			},
			resolver: mockResolver{
				entries: map[mockEntry][]string{
					mockEntry{name: "alertmanager.example.com:9093", qtype: dns.A}: []string{"1.1.1.1:9093", "2.2.2.2:9093"},
				},
			},
			resolved: []string{"1.1.1.1:9093", "2.2.2.2:9093"},
		},
		{
			cfg: AlertmanagerConfig{
				StaticAddresses: []string{"alertmanager.example.com:9096"},
			},
			resolved: []string{"alertmanager.example.com:9096"},
		},
		{
			cfg: AlertmanagerConfig{
				StaticAddresses: []string{"dnssrv+_web._tcp.alertmanager.example.com"},
			},
			resolver: mockResolver{
				entries: map[mockEntry][]string{
					mockEntry{name: "_web._tcp.alertmanager.example.com", qtype: dns.SRV}: []string{"1.1.1.1:9097", "2.2.2.2:9097"},
				},
			},
			resolved: []string{"1.1.1.1:9097", "2.2.2.2:9097"},
		},
		{
			cfg: AlertmanagerConfig{
				StaticAddresses: []string{"dnssrv+_web._tcp.notfound.example.com"},
			},
			resolver: mockResolver{
				entries: map[mockEntry][]string{},
			},
			err: true,
		},
	} {
		t.Run(strings.Join(tc.cfg.StaticAddresses, ","), func(t *testing.T) {
			am, err := NewAlertmanager(nil, tc.cfg)
			testutil.Ok(t, err)
			ctx := context.Background()
			err = am.Update(ctx, &tc.resolver)
			if tc.err {
				t.Logf("%v", err)
				testutil.NotOk(t, err)
				return
			}

			testutil.Equals(t, tc.resolved, am.resolved)
		})
	}

}
