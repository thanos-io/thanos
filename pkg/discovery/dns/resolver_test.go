package dns

import (
	"context"
	"net"
	"strconv"
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
	qtype          string
	expectedResult []string
	expectedErr    error
	resolver       *mockHostnameResolver
}

var (
	port            = 8888
	defaultPort     = 9999
	dnsHostNoPort   = "test.mycompany.com"
	dnsHostWithPort = "test.mycompany.com" + strconv.Itoa(port)
	ip              = "192.168.0.1"
	ip2             = "192.168.0.2"
	srvHost         = "_test._tcp.mycompany.com"
	invalidQtype    = "dnsinvalid"
	mockResolver    = &mockHostnameResolver{
		resultIPs: map[string][]net.IPAddr{
			dnsHostNoPort:   {net.IPAddr{IP: net.ParseIP(ip)}},
			dnsHostWithPort: {net.IPAddr{IP: net.ParseIP(ip)}},
		},
		resultSRVs: map[string][]*net.SRV{
			srvHost: {
				&net.SRV{Target: ip, Port: uint16(port)},
				&net.SRV{Target: ip2, Port: uint16(port)},
			},
		},
		err: nil,
	}
	errorFromResolver = errors.New("error from resolver")

	dnsSDTests = []DNSSDTest{
		{
			"single ip from dns lookup of host:port",
			dnsHostNoPort + ":" + strconv.Itoa(port),
			"dns",
			[]string{ip + ":" + strconv.Itoa(port)},
			nil,
			&mockHostnameResolver{
				resultIPs: map[string][]net.IPAddr{
					dnsHostNoPort: {net.IPAddr{IP: net.ParseIP(ip)}},
				},
			},
		},
		{
			"multiple srv records from srv lookup",
			srvHost,
			"dnssrv",
			[]string{ip + ":" + strconv.Itoa(port), ip2 + ":" + strconv.Itoa(port)},
			nil,
			&mockHostnameResolver{
				resultSRVs: map[string][]*net.SRV{
					srvHost: {
						&net.SRV{Target: ip, Port: uint16(port)},
						&net.SRV{Target: ip2, Port: uint16(port)},
					},
				},
			},
		},
		{
			"error on dns lookup when no port is specified",
			dnsHostNoPort,
			"dns",
			nil,
			errors.Errorf("missing port in address given for dns lookup: %v", dnsHostNoPort),
			&mockHostnameResolver{},
		},
		{
			"error on bad qtype",
			dnsHostNoPort,
			invalidQtype,
			nil,
			errors.Errorf("invalid lookup scheme %q", invalidQtype),
			&mockHostnameResolver{},
		},
		{
			"error from resolver",
			srvHost,
			"dnssrv",
			nil,
			errors.Wrapf(errorFromResolver, "lookup SRV records %q", srvHost),
			&mockHostnameResolver{err: errorFromResolver},
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
	testutil.Assert(t, len(result) == len(tt.expectedResult), "expected %v hosts, but got %v", len(tt.expectedResult), len(result))
	for i, host := range result {
		testutil.Assert(t, tt.expectedResult[i] == host, "expected host %v is missing", tt.expectedResult[i])
	}
}
