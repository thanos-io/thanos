package alertmanager

import (
	"context"
	"net/url"
	"testing"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestParseAlertmanagerAddress(t *testing.T) {
	type expected struct {
		hasErr bool
		qtype  dns.QType
		url    *url.URL
	}
	tests := []struct {
		addr     string
		expected expected
	}{
		// no schema or no support schema
		{"alertmanager", expected{hasErr: true}},
		{"alertmanager:9093", expected{hasErr: true}},
		{"tcp://alertmanager:9093", expected{hasErr: true}},

		// correct cases
		{"http://alertmanager:9093", expected{hasErr: false, qtype: dns.QType(""), url: &url.URL{Scheme: "http", Host: "alertmanager:9093"}}},
		{"dns+http://alertmanager:9093", expected{hasErr: false, qtype: dns.QType("dns"), url: &url.URL{Scheme: "http", Host: "alertmanager:9093"}}},
		{"dnssrv+http://alertmanager:9093", expected{hasErr: false, qtype: dns.QType("dnssrv"), url: &url.URL{Scheme: "http", Host: "alertmanager:9093"}}},
		{"dnssrvnoa+http://alertmanager:9093", expected{hasErr: false, qtype: dns.QType("dnssrvnoa"), url: &url.URL{Scheme: "http", Host: "alertmanager:9093"}}},
	}
	for _, tt := range tests {
		gotQType, gotParsedUrl, err := parseAlertmanagerAddress(tt.addr)
		if tt.expected.hasErr {
			testutil.NotOk(t, err)
		} else {
			testutil.Ok(t, err)
		}
		testutil.Equals(t, tt.expected.qtype, gotQType)
		testutil.Equals(t, tt.expected.url, gotParsedUrl)
	}
}

type mockResolver struct {
	resultIPs map[string][]string
	err       error
}

func (m mockResolver) Resolve(ctx context.Context, name string, qtype dns.QType) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if res, ok := m.resultIPs[name]; ok {
		return res, nil
	}
	return nil, errors.Errorf("mockResolver not found response for name: %s", name)
}

func TestRuleAlertmanagerResolveWithPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:19093": {"1.1.1.1:9300"},
		},
	}

	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com:19093"}}

	ctx := context.TODO()
	err := am.Update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.Get()
	testutil.Equals(t, expected, gotURLs)
}

func TestRuleAlertmanagerResolveWithoutPort(t *testing.T) {
	mockResolver := mockResolver{
		resultIPs: map[string][]string{
			"alertmanager.com:9093": {"1.1.1.1:9300"},
		},
	}
	am := alertmanagerSet{resolver: mockResolver, addrs: []string{"dns+http://alertmanager.com"}}

	ctx := context.TODO()
	err := am.Update(ctx)
	testutil.Ok(t, err)

	expected := []*url.URL{
		{
			Scheme: "http",
			Host:   "1.1.1.1:9300",
		},
	}
	gotURLs := am.Get()
	testutil.Equals(t, expected, gotURLs)
}
