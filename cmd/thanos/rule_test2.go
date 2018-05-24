package main

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"

	"github.com/improbable-eng/thanos/pkg/testutil"
)

type NetResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

type netResolverMock struct {
	ipdAddrFn func(ctx context.Context, host string) ([]net.IPAddr, error)
	srvFn     func(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

func (m *netResolverMock) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	if m.ipdAddrFn == nil {
		return nil, errors.New("mock does not have LookupIPAddr behaviour set")
	}
	return m.ipdAddrFn(ctx, host)
}

func (m *netResolverMock) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	if m.srvFn == nil {
		return "", nil, errors.New("mock does not have LookupIPAddr behaviour set")
	}
	return m.srvFn(ctx, service, proto, name)
}

func TestAlertmanagerSet(t *testing.T) {
	for _, tcase := range []struct {
		addr            []string
		m               NetResolver
		expectedCurrent []string
	}{
		{
			addr:            []string{"http://alertmanager.com:9094"},
			m:               &netResolverMock{},
			expectedCurrent: []string{"http://alertmanager.com:9094"},
		},
		{
			addr:            []string{"dns://alertmanager.com:9094"},
			m:               &netResolverMock{},
			expectedCurrent: []string{"dns://alertmanager.com:9094"},
		},
	} {
		var expectedURLs []*url.URL
		for _, c := range tcase.expectedCurrent {
			cURL, err := url.Parse(c)
			testutil.Ok(t, err)

			expectedURLs = append(expectedURLs, cURL)
		}

		// Alertmanager needs to use net resolver.
		//s := newAlertmanagerSet(tcase.addr, tcase.m)

		//testutil.Ok(t, s.update(context.Background()))
		//testutil.Equals(t, expectedURLs, s.get())
	}
}
