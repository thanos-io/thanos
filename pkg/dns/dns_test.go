package dns

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
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

var (
	defaultPort          = 9999
	ip1                  = "192.168.0.1"
	ip2                  = "192.168.0.2"
	srvIp                = "192.168.0.3"
	srvPort1             = 1111
	srvPort2             = 2222
	dnsHost1             = "test.mycompany.com"
	dnsHost2             = "test2.mycompany.com"
	srvHost              = "_test._tcp.mycompany.com"
	httpDnsHost1         = "http://" + dnsHost1
	httpDnsHost2         = "http://" + dnsHost2
	httpDnsHostPort1     = "http://" + dnsHost2 + strconv.Itoa(defaultPort)
	httpDnsHostPort2     = "http://" + dnsHost2 + strconv.Itoa(defaultPort)

	ipsWithPort          = []string{ip1 + ":" + strconv.Itoa(defaultPort), ip2 + ":" + strconv.Itoa(defaultPort)}
)

func TestDnsSD_Resolve_ShouldReturnAllIPs_WhenGivenPrefixedDomains(t *testing.T) {
	mockResolver := setupResolver()

	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	domainsWithPrefix := []string {
		"dns+" + httpDnsHost1, // dns+http://test.mycompany.com
		"dns+" + dnsHost2,     // dns+test2.mycompany.com.com
		"dnssrv+" + srvHost,   // dnssrv+_test._tcp.mycompany.com
	}

	urls, err := dnsSD.Resolve(ctx, domainsWithPrefix, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 4, "expected 4 urls but got %v", len(urls))
	containsHostPort(t, urls, ip1, strconv.Itoa(defaultPort))
	containsHostPort(t, urls, ip2, strconv.Itoa(defaultPort))
	containsHostPort(t, urls, srvIp, strconv.Itoa(srvPort1))
	containsHostPort(t, urls, srvIp, strconv.Itoa(srvPort2))
}

func TestDnsSD_Resolve_ShouldReturnOnlyGivenDomains_WhenGivenDomainsWithNoPrefix(t *testing.T) {
	mockResolver := setupResolver()

	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	domainsWithoutPrefix := []string {
		httpDnsHost1 + ":" + strconv.Itoa(defaultPort), // http://test.mycompany.com:9999
		dnsHost2,                                       // test2.mycompany.com:9999
		srvHost + ":" + strconv.Itoa(defaultPort),      // _test._tcp.mycompany.com:9999
	}

	urls, err := dnsSD.Resolve(ctx, domainsWithoutPrefix, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 3, "expected 3 urls but got %v", len(urls))
	containsHostPort(t, urls, httpDnsHost1, strconv.Itoa(defaultPort))
	// TODO(ivan): confirm this is a desired behaviour - no longer adding default port if you don't resolve the address
	// potential change only to alertmanagers. I don't think we should be adding anything anyway - aka. this is the right behaviour.
	containsHostPort(t, urls, dnsHost2, "")
	containsHostPort(t, urls, srvHost, strconv.Itoa(defaultPort))
}

func TestDnsSD_Resolve_ShouldReturnTheOriginalIPs_WhenGivenIPs(t *testing.T) {
	mockResolver := setupResolver()
	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	ipsWithPort := []string {
		ip1 + ":" + strconv.Itoa(defaultPort),
		ip2 + ":" + strconv.Itoa(defaultPort),
	}

	urls, err := dnsSD.Resolve(ctx, ipsWithPort, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 2, "expected 2 urls but got %v", len(urls))
	containsHostPort(t, urls, ip1, strconv.Itoa(defaultPort))
	containsHostPort(t, urls, ip2, strconv.Itoa(defaultPort))
}

func TestDnsSD_Resolve_ShouldReturnErr_WhenGivenInvalidLookupScheme(t *testing.T) {
	mockResolver := setupResolver()
	dnsSD := dnsSD{mockResolver}

	_, err := dnsSD.Resolve(context.TODO(), []string{"dnstxt+"+srvHost}, defaultPort)
	testutil.NotOk(t, err)
}

func setupResolver() *mockHostnameResolver {
	return &mockHostnameResolver{
		resultIPs: map[string][]net.IPAddr{
			dnsHost1: {net.IPAddr{IP: net.ParseIP(ip1)}},
			dnsHost2: {net.IPAddr{IP: net.ParseIP(ip2)}},
		},
		resultSRVs: map[string][]*net.SRV{
			srvHost: {
				&net.SRV{Target: srvIp, Port: uint16(srvPort1)},
				&net.SRV{Target: srvIp, Port: uint16(srvPort2)},
			},
		},
		err: nil,
	}
}

func containsHostPort(t *testing.T, urls []*url.URL, host string, port string) {
	contains := false
	for _, url := range urls {
		var hostFromUrl, portFromUrl string
		portIndex := strings.LastIndex(url.Host, ":")
		if portIndex < 0 {
			hostFromUrl = url.Host
			portFromUrl = ""
		} else {
			hostFromUrl = url.Host[:portIndex]
			portFromUrl = url.Host[portIndex+1:]
		}

		if hostFromUrl == host && portFromUrl == port {
			contains = true
			break
		}
	}
	if !contains {
		t.Fatalf("expected host %v:%v is missing", host, port)
	}
}
