package dns

import (
	"context"
	"net"
	"sort"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/pkg/errors"
)

type mockHostnameResolver struct {
	resultIPs  map[string][]net.IPAddr
	resultSRVs map[string][]*net.SRV
	err        error
}

func (m mockHostnameResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.resultIPs[host], nil
}

func (m mockHostnameResolver) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	if m.err != nil {
		return "", nil, m.err
	}
	return "", m.resultSRVs[name], nil
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
			expectedErr:    errors.Errorf("missing port in address given for dns lookup: test.mycompany.com"),
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
			expectedErr:    errors.Errorf("invalid lookup scheme \"invalid\""),
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
	dnsSD := dnsSD{tt.resolver}

	result, err := dnsSD.Resolve(ctx, tt.addr, tt.qtype)
	if tt.expectedErr != nil {
		testutil.Assert(t, err != nil, "expected error but none was returned")
		testutil.Assert(t, tt.expectedErr.Error() == err.Error(), "expected error '%v', but got '%v'", tt.expectedErr.Error(), err.Error())
	} else {
		testutil.Assert(t, err == nil, "expected no error but got %v", err)
	}
	sort.Strings(result)
	testutil.Equals(t, tt.expectedResult, result)
}
