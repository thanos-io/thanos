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
	resultIPs          map[string][]net.IPAddr
	resultDualStackIPs map[string][]net.IPAddr
	resultSRVs         map[string][]*net.SRV
	err                error
	dualStackErr       error
	notFoundErr        bool
}

func (m mockHostnameResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resultIPs[host], nil
}

func (m mockHostnameResolver) LookupIPAddrDualStack(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.dualStackErr != nil {
		return nil, m.dualStackErr
	}
	if m.resultDualStackIPs != nil {
		return m.resultDualStackIPs[host], nil
	}
	return m.resultIPs[host], nil
}

func (m mockHostnameResolver) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	if m.err != nil {
		return "", nil, m.err
	}
	return "", m.resultSRVs[name], nil
}

func (m mockHostnameResolver) IsNotFound(err error) bool {
	return m.notFoundErr
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

	tests := []struct {
		name           string
		addr           string
		resolver       *mockHostnameResolver
		expectedResult []string
		expectedErr    error
	}{
		{
			name: "dual-stack returns both IPv6 and IPv4",
			addr: "test.mycompany.com:8080",
			resolver: &mockHostnameResolver{
				resultDualStackIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {
						{IP: ipv6},
						{IP: ipv4},
					},
				},
			},
			expectedResult: []string{"[2001:db8::1]:8080", "192.168.1.1:8080"},
		},
		{
			name: "dual-stack IPv6 only",
			addr: "test.mycompany.com:8080",
			resolver: &mockHostnameResolver{
				resultDualStackIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {{IP: ipv6}},
				},
			},
			expectedResult: []string{"[2001:db8::1]:8080"},
		},
		{
			name: "dual-stack IPv4 only",
			addr: "test.mycompany.com:8080",
			resolver: &mockHostnameResolver{
				resultDualStackIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {{IP: ipv4}},
				},
			},
			expectedResult: []string{"192.168.1.1:8080"},
		},
		{
			name: "dual-stack preserves scheme",
			addr: "http://test.mycompany.com:8080",
			resolver: &mockHostnameResolver{
				resultDualStackIPs: map[string][]net.IPAddr{
					"test.mycompany.com": {
						{IP: ipv6},
						{IP: ipv4},
					},
				},
			},
			expectedResult: []string{"http://[2001:db8::1]:8080", "http://192.168.1.1:8080"},
		},
		{
			name:           "dual-stack requires port",
			addr:           "test.mycompany.com",
			resolver:       &mockHostnameResolver{},
			expectedResult: nil,
			expectedErr:    errors.New("missing port in address given for dnsdualstack lookup: test.mycompany.com"),
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
	ipv4 := net.ParseIP("192.168.1.1")

	tests := []struct {
		name           string
		dualStackErr   error
		dualStackAddrs []net.IPAddr
		expectErr      bool
		expectAddrs    int
	}{
		{
			name:         "network unreachable error propagates",
			dualStackErr: errors.New("network unreachable"),
			expectErr:    true,
			expectAddrs:  0,
		},
		{
			name:         "timeout error propagates",
			dualStackErr: errors.New("i/o timeout"),
			expectErr:    true,
			expectAddrs:  0,
		},
		{
			name:         "DNS server failure propagates",
			dualStackErr: errors.New("server misbehaving"),
			expectErr:    true,
			expectAddrs:  0,
		},
		{
			name:           "successful resolution with addresses",
			dualStackErr:   nil,
			dualStackAddrs: []net.IPAddr{{IP: ipv4}},
			expectErr:      false,
			expectAddrs:    1,
		},
		{
			name:           "empty result without error",
			dualStackErr:   nil,
			dualStackAddrs: []net.IPAddr{},
			expectErr:      false,
			expectAddrs:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			resolver := &mockHostnameResolver{
				dualStackErr: tt.dualStackErr,
			}
			if tt.dualStackAddrs != nil {
				resolver.resultDualStackIPs = map[string][]net.IPAddr{
					"test.mycompany.com": tt.dualStackAddrs,
				}
			}
			dnsSD := dnsSD{resolver, log.NewNopLogger()}

			result, err := dnsSD.Resolve(ctx, "test.mycompany.com:8080", ADualStack)

			if tt.expectErr {
				testutil.NotOk(t, err)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tt.expectAddrs, len(result))
			}
		})
	}
}

func TestDnsSD_ResolveDualStack_DNSError(t *testing.T) {
	notFoundErr := &net.DNSError{
		Err:        "no such host",
		Name:       "test.mycompany.com",
		IsNotFound: true,
	}

	t.Run("not-found error is logged but returns empty results", func(t *testing.T) {
		ctx := context.TODO()
		resolver := &mockHostnameResolver{
			dualStackErr: notFoundErr,
			notFoundErr:  true,
		}
		dnsSD := dnsSD{resolver, log.NewNopLogger()}

		result, err := dnsSD.Resolve(ctx, "test.mycompany.com:8080", ADualStack)

		testutil.Ok(t, err)
		testutil.Equals(t, 0, len(result))
	})

	t.Run("non-not-found DNS error propagates", func(t *testing.T) {
		ctx := context.TODO()
		resolver := &mockHostnameResolver{
			dualStackErr: notFoundErr,
			notFoundErr:  false,
		}
		dnsSD := dnsSD{resolver, log.NewNopLogger()}

		result, err := dnsSD.Resolve(ctx, "test.mycompany.com:8080", ADualStack)

		testutil.NotOk(t, err)
		testutil.Equals(t, 0, len(result))
	})
}
