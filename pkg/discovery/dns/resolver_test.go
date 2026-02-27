// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"net"
	"sort"
	"testing"

	"github.com/go-kit/log"

	"github.com/pkg/errors"

	"github.com/efficientgo/core/testutil"
)

type mockHostnameResolver struct {
	resultIPs  map[string][]net.IPAddr
	resultSRVs map[string][]*net.SRV
	err        error

	// Per-network results for LookupIPAddrByNetwork. Key format: "network:host".
	resultByNetwork map[string][]net.IPAddr
	errByNetwork    map[string]error
	isNotFound      func(error) bool
}

func (m mockHostnameResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resultIPs[host], nil
}

func (m mockHostnameResolver) LookupIPAddrByNetwork(_ context.Context, network, host string) ([]net.IPAddr, error) {
	key := network + ":" + host
	if m.errByNetwork != nil {
		if err, ok := m.errByNetwork[key]; ok {
			return nil, err
		}
	}
	if m.resultByNetwork != nil {
		return m.resultByNetwork[key], nil
	}
	return nil, nil
}

func (m mockHostnameResolver) LookupSRV(_ context.Context, _, _, name string) (cname string, addrs []*net.SRV, err error) {
	if m.err != nil {
		return "", nil, m.err
	}
	return "", m.resultSRVs[name], nil
}

func (m mockHostnameResolver) IsNotFound(err error) bool {
	if m.isNotFound != nil {
		return m.isNotFound(err)
	}
	return false
}

type DNSSDTest struct {
	testName       string
	addr           string
	qtype          QType
	expectedResult []string
	expectedErr    error
	resolver       *mockHostnameResolver
}

var (
	errorFromResolver = errors.New("error from resolver")

	dnsSDTests = []DNSSDTest{
		{
			testName:       "single ip from dns lookup of host port",
			addr:           "test.mycompany.com:8080",
			qtype:          A,
			expectedResult: []string{"192.168.0.1:8080"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
				},
			},
		},
		// Scheme is preserved.
		{
			testName:       "single ip from dns lookup of host port with scheme",
			addr:           "http://test.mycompany.com:8080",
			qtype:          A,
			expectedResult: []string{"http://192.168.0.1:8080"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
				},
			},
		},
		{
			testName:       "error on dns lookup when no port is specified",
			addr:           "test.mycompany.com",
			qtype:          A,
			expectedResult: nil,
			expectedErr:    errors.New("missing port in address given for dns lookup: test.mycompany.com"),
			resolver:       &mockHostnameResolver{},
		},
		{
			testName:       "multiple SRV records from SRV lookup",
			addr:           "_test._tcp.mycompany.com",
			qtype:          SRV,
			expectedResult: []string{"192.168.0.1:8080", "192.168.0.2:8081"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.mycompany.com": {
						&net.SRV{Target: "alt1.mycompany.com.", Port: 8080},
						&net.SRV{Target: "alt2.mycompany.com.", Port: 8081},
					},
				},
				resultIPs: map[string][]net.IPAddr{
					"alt1.mycompany.com.": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
					"alt2.mycompany.com.": {net.IPAddr{IP: net.ParseIP("192.168.0.2")}},
				},
			},
		},
		{
			testName:       "multiple SRV records from SRV lookup with specified port",
			addr:           "_test._tcp.mycompany.com:8082",
			qtype:          SRV,
			expectedResult: []string{"192.168.0.1:8082", "192.168.0.2:8082"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.mycompany.com": {
						&net.SRV{Target: "alt1.mycompany.com.", Port: 8080},
						&net.SRV{Target: "alt2.mycompany.com.", Port: 8081},
					},
				},
				resultIPs: map[string][]net.IPAddr{
					"alt1.mycompany.com.": {net.IPAddr{IP: net.ParseIP("192.168.0.1")}},
					"alt2.mycompany.com.": {net.IPAddr{IP: net.ParseIP("192.168.0.2")}},
				},
			},
		},
		{
			testName:       "error from SRV resolver",
			addr:           "_test._tcp.mycompany.com",
			qtype:          SRV,
			expectedResult: nil,
			expectedErr:    errors.Wrapf(errorFromResolver, "lookup SRV records \"_test._tcp.mycompany.com\""),
			resolver:       &mockHostnameResolver{err: errorFromResolver},
		},
		{
			testName:       "multiple SRV records from SRV no A lookup",
			addr:           "_test._tcp.mycompany.com",
			qtype:          SRVNoA,
			expectedResult: []string{"192.168.0.1:8080", "192.168.0.2:8081"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.mycompany.com": {
						&net.SRV{Target: "192.168.0.1", Port: 8080},
						&net.SRV{Target: "192.168.0.2", Port: 8081},
					},
				},
			},
		},
		{
			testName:       "multiple SRV records from SRV no A lookup with specified port",
			addr:           "_test._tcp.mycompany.com:8082",
			qtype:          SRVNoA,
			expectedResult: []string{"192.168.0.1:8082", "192.168.0.2:8082"},
			expectedErr:    nil,
			resolver: &mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					"_test._tcp.mycompany.com": {
						&net.SRV{Target: "192.168.0.1", Port: 8080},
						&net.SRV{Target: "192.168.0.2", Port: 8081},
					},
				},
			},
		},
		{
			testName:       "error from SRV no A lookup",
			addr:           "_test._tcp.mycompany.com",
			qtype:          SRV,
			expectedResult: nil,
			expectedErr:    errors.Wrapf(errorFromResolver, "lookup SRV records \"_test._tcp.mycompany.com\""),
			resolver:       &mockHostnameResolver{err: errorFromResolver},
		},
		{
			testName:       "error on bad qtype",
			addr:           "test.mycompany.com",
			qtype:          "invalid",
			expectedResult: nil,
			expectedErr:    errors.New("invalid lookup scheme \"invalid\""),
			resolver:       &mockHostnameResolver{},
		},
	}
)

func TestDnsSD_Resolve(t *testing.T) {
	for _, tt := range dnsSDTests {
		t.Run(tt.testName, func(t *testing.T) {
			testDnsSd(t, tt)
		})
	}
}

func testDnsSd(t *testing.T, tt DNSSDTest) {
	ctx := context.TODO()
	dnsSD := dnsSD{tt.resolver, log.NewNopLogger()}

	result, err := dnsSD.Resolve(ctx, tt.addr, tt.qtype)
	if tt.expectedErr != nil {
		testutil.NotOk(t, err)
		testutil.Assert(t, tt.expectedErr.Error() == err.Error(), "expected error '%v', but got '%v'", tt.expectedErr.Error(), err.Error())
	} else {
		testutil.Ok(t, err)
	}
	sort.Strings(result)
	testutil.Equals(t, tt.expectedResult, result)
}

func TestDnsSD_ResolveDualStack(t *testing.T) {
	ipv6 := net.ParseIP("2001:db8::1")
	ipv4 := net.ParseIP("192.168.1.1")
	host := "test.mycompany.com"

	tests := []struct {
		name           string
		addr           string
		resolver       *mockHostnameResolver
		expectedResult []string
		expectedErr    error
	}{
		{
			name: "both families resolve",
			addr: host + ":8080",
			resolver: &mockHostnameResolver{
				resultByNetwork: map[string][]net.IPAddr{
					"ip6:" + host: {{IP: ipv6}},
					"ip4:" + host: {{IP: ipv4}},
				},
			},
			expectedResult: []string{"[2001:db8::1]:8080", "192.168.1.1:8080"},
		},
		{
			name: "IPv6 only",
			addr: host + ":8080",
			resolver: &mockHostnameResolver{
				resultByNetwork: map[string][]net.IPAddr{
					"ip6:" + host: {{IP: ipv6}},
				},
			},
			expectedResult: []string{"[2001:db8::1]:8080"},
		},
		{
			name: "IPv4 only",
			addr: host + ":8080",
			resolver: &mockHostnameResolver{
				resultByNetwork: map[string][]net.IPAddr{
					"ip4:" + host: {{IP: ipv4}},
				},
			},
			expectedResult: []string{"192.168.1.1:8080"},
		},
		{
			name:           "requires port",
			addr:           host,
			resolver:       &mockHostnameResolver{},
			expectedResult: nil,
			expectedErr:    errors.New("missing port in address given for dnsdualstack lookup: " + host),
		},
		{
			name: "IPv6 fails, IPv4 succeeds",
			addr: host + ":8080",
			resolver: &mockHostnameResolver{
				resultByNetwork: map[string][]net.IPAddr{
					"ip4:" + host: {{IP: ipv4}},
				},
				errByNetwork: map[string]error{
					"ip6:" + host: errors.New("network unreachable"),
				},
			},
			expectedResult: []string{"192.168.1.1:8080"},
		},
		{
			name: "IPv4 fails, IPv6 succeeds",
			addr: host + ":8080",
			resolver: &mockHostnameResolver{
				resultByNetwork: map[string][]net.IPAddr{
					"ip6:" + host: {{IP: ipv6}},
				},
				errByNetwork: map[string]error{
					"ip4:" + host: errors.New("network unreachable"),
				},
			},
			expectedResult: []string{"[2001:db8::1]:8080"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			dnsSD := dnsSD{tt.resolver, log.NewNopLogger()}

			result, err := dnsSD.Resolve(ctx, tt.addr, ADualStack)
			if tt.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Assert(t, tt.expectedErr.Error() == err.Error(), "expected error '%v', but got '%v'", tt.expectedErr.Error(), err.Error())
			} else {
				testutil.Ok(t, err)
			}
			sort.Strings(result)
			sort.Strings(tt.expectedResult)
			testutil.Equals(t, tt.expectedResult, result)
		})
	}
}

func TestDnsSD_ResolveDualStack_Errors(t *testing.T) {
	host := "test.mycompany.com"
	addr := host + ":8080"

	t.Run("both families fail propagates error", func(t *testing.T) {
		resolver := &mockHostnameResolver{
			errByNetwork: map[string]error{
				"ip6:" + host: errors.New("network unreachable"),
				"ip4:" + host: errors.New("network unreachable"),
			},
		}
		dnsSD := dnsSD{resolver, log.NewNopLogger()}

		_, err := dnsSD.Resolve(context.TODO(), addr, ADualStack)
		testutil.NotOk(t, err)
	})

	t.Run("not-found on both families returns empty", func(t *testing.T) {
		notFound := &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		resolver := &mockHostnameResolver{
			errByNetwork: map[string]error{
				"ip6:" + host: notFound,
				"ip4:" + host: notFound,
			},
			isNotFound: func(err error) bool {
				dnsErr, ok := err.(*net.DNSError)
				return ok && dnsErr.IsNotFound
			},
		}
		dnsSD := dnsSD{resolver, log.NewNopLogger()}

		result, err := dnsSD.Resolve(context.TODO(), addr, ADualStack)
		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(result))
	})

	t.Run("not-found on one family real error on other propagates", func(t *testing.T) {
		notFound := &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		resolver := &mockHostnameResolver{
			errByNetwork: map[string]error{
				"ip6:" + host: notFound,
				"ip4:" + host: errors.New("server misbehaving"),
			},
			isNotFound: func(err error) bool {
				dnsErr, ok := err.(*net.DNSError)
				return ok && dnsErr.IsNotFound
			},
		}
		dnsSD := dnsSD{resolver, log.NewNopLogger()}

		_, err := dnsSD.Resolve(context.TODO(), addr, ADualStack)
		testutil.NotOk(t, err)
	})

}
