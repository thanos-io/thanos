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
	ipHost1              = "test.ipv4.mycompany.com"
	ipHost2              = "test2.ipv4.mycompany.com"
	srvHost              = "test.srv.mycompany.com"
	ipDomain1            = "http://" + ipHost1
	ipDomain2            = "http://" + ipHost2
	srvDomain            = "http://" + srvHost
	domainsWithPrefix    = []string{"dns+" + ipDomain1, "dns+" + ipDomain2, "dnssrv+" + srvDomain}
	domainsWithoutPrefix = []string{ipDomain1, ipDomain2, srvDomain}
	ipsWithPort          = []string{ip1 + ":" + strconv.Itoa(defaultPort), ip2 + ":" + strconv.Itoa(defaultPort)}
)

func TestDnsSD_Resolve_ShouldReturnAllIPs_WhenGivenPrefixedDomains(t *testing.T) {
	mockResolver := setupResolver()

	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	urls, err := dnsSD.Resolve(ctx, domainsWithPrefix, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 4, "expected 4 urls but got %v", len(urls))
	containsHostPort(t, urls, ip1, defaultPort)
	containsHostPort(t, urls, ip2, defaultPort)
	containsHostPort(t, urls, srvIp, srvPort1)
	containsHostPort(t, urls, srvIp, srvPort2)
}

func TestDnsSD_Resolve_ShouldReturnOnlyGivenDomains_WhenGivenDomainsWithNoPrefix(t *testing.T) {
	mockResolver := setupResolver()

	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	urls, err := dnsSD.Resolve(ctx, domainsWithoutPrefix, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 3, "expected 3 urls but got %v", len(urls))
	// TODO(ivan): confirm this is a desired behaviour - adding a default port even when no prefix is given.
	// This is changing the current behaviour which is - don't do anything to the provided addresses if they are not prefixed.
	containsHostPort(t, urls, ipHost1, defaultPort)
	containsHostPort(t, urls, ipHost2, defaultPort)
	containsHostPort(t, urls, srvHost, defaultPort)
}

func TestDnsSD_Resolve_ShouldReturnTheOriginalIPs_WhenGivenIPs(t *testing.T) {
	mockResolver := setupResolver()
	dnsSD := dnsSD{mockResolver}
	ctx := context.TODO()

	urls, err := dnsSD.Resolve(ctx, ipsWithPort, defaultPort)
	testutil.Ok(t, err)
	testutil.Assert(t, len(urls) == 2, "expected 2 urls but got %v", len(urls))
	containsHostPort(t, urls, ip1, defaultPort)
	containsHostPort(t, urls, ip2, defaultPort)
}

func setupResolver() *mockHostnameResolver {
	return &mockHostnameResolver{
		resultIPs: map[string][]net.IPAddr{
			ipHost1: {net.IPAddr{IP: net.ParseIP(ip1)}},
			ipHost2: {net.IPAddr{IP: net.ParseIP(ip2)}},
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

func containsHostPort(t *testing.T, urls []*url.URL, host string, port int) {
	contains := false
	for _, url := range urls {
		portIndex := strings.Index(url.Host, ":")
		hostFromUrl := url.Host[:portIndex]
		portFromUrl := url.Host[portIndex+1:]
		if hostFromUrl == host && portFromUrl == strconv.Itoa(port) {
			contains = true
			break
		}
	}
	if !contains {
		t.Errorf("expected host %v:%v is missing", host, port)
	}
}
