package storeutils

import (
	"context"

	"github.com/thanos-io/thanos/pkg/discovery/dns"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

type AddressProvider interface {
	GetAddresses(context.Context) []string
}

type dnsProvider struct {
	provider     *dns.Provider
	dnsAddresses []string
	logger       log.Logger
}

func NewAddressProvider(logger log.Logger, dnsAddresses []string) AddressProvider {
	return &dnsProvider{
		logger:       logger,
		dnsAddresses: dnsAddresses,
		provider:     dns.NewProvider(logger, prometheus.DefaultRegisterer, dns.GolangResolverType),
	}
}

func (d *dnsProvider) GetAddresses(ctx context.Context) []string {
	level.Info(d.logger).Log("msg", "resolving SRV records", "addresses", d.dnsAddresses)
	err := d.provider.Resolve(ctx, d.dnsAddresses)
	if err != nil {
		level.Error(d.logger).Log("msg", "failed to resolve SRV records", "err", err)
	}

	return d.provider.Addresses()
}
