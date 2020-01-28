// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/discovery/dns/miekgdns"
)

// MetaTarget stores the information about a resolved target and its sticky bit.
type MetaTarget struct {
	addr   string
	sticky bool
}

// GetAddr returns the MetaTarget's address.
func (mt *MetaTarget) GetAddr() string {
	return mt.addr
}

// IsSticky returns true if the target ought to be sticky.
func (mt *MetaTarget) IsSticky() bool {
	return mt.sticky
}

// Provider is a stateful cache for asynchronous DNS resolutions. It provides a way to resolve addresses and obtain them.
type Provider struct {
	sync.Mutex
	resolver Resolver
	// A map from domain name to a slice of resolved targets + their sticky bits.
	resolved map[string][]MetaTarget
	logger   log.Logger

	resolverAddrs         *prometheus.GaugeVec
	resolverLookupsCount  prometheus.Counter
	resolverFailuresCount prometheus.Counter
}

type ResolverType string

const (
	GolangResolverType   ResolverType = "golang"
	MiekgdnsResolverType ResolverType = "miekgdns"
)

func (t ResolverType) ToResolver(logger log.Logger) ipLookupResolver {
	var r ipLookupResolver
	switch t {
	case GolangResolverType:
		r = net.DefaultResolver
	case MiekgdnsResolverType:
		r = &miekgdns.Resolver{ResolvConf: miekgdns.DefaultResolvConfPath}
	default:
		level.Warn(logger).Log("msg", "no such resolver type, defaulting to golang", "type", t)
		r = net.DefaultResolver
	}
	return r
}

// NewProvider returns a new empty provider with a given resolver type.
// If empty resolver type is net.DefaultResolver.w
func NewProvider(logger log.Logger, reg prometheus.Registerer, resolverType ResolverType) *Provider {
	p := &Provider{
		resolver: NewResolver(resolverType.ToResolver(logger)),
		resolved: make(map[string][]MetaTarget),
		logger:   logger,
		resolverAddrs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dns_provider_results",
			Help: "The number of resolved endpoints for each configured address",
		}, []string{"addr"}),
		resolverLookupsCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dns_lookups_total",
			Help: "The number of DNS lookups resolutions attempts",
		}),
		resolverFailuresCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dns_failures_total",
			Help: "The number of DNS lookup failures",
		}),
	}

	if reg != nil {
		reg.MustRegister(p.resolverAddrs)
		reg.MustRegister(p.resolverLookupsCount)
		reg.MustRegister(p.resolverFailuresCount)
	}

	return p
}

// Clone returns a new provider from an existing one.
func (p *Provider) Clone() *Provider {
	return &Provider{
		resolver:              p.resolver,
		resolved:              make(map[string][]MetaTarget),
		logger:                p.logger,
		resolverAddrs:         p.resolverAddrs,
		resolverLookupsCount:  p.resolverLookupsCount,
		resolverFailuresCount: p.resolverFailuresCount,
	}
}

// Resolve stores a list of provided addresses or their DNS records if requested.
// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
// Addresses with a suffix `+sticky` will be made sticky i.e. we will always consider them
// as part of the active storeset.
// defaultPort is used for non-SRV records when a port is not supplied.
func (p *Provider) Resolve(ctx context.Context, addrs []string) {
	p.Lock()
	defer p.Unlock()

	for _, addr := range addrs {
		var resolved []string
		var sticky bool

		if strings.HasSuffix(addr, "+sticky") {
			sticky = true
			addr = strings.TrimSuffix(addr, "+sticky")
		}

		qtypeAndName := strings.SplitN(addr, "+", 2)
		if len(qtypeAndName) != 2 {
			// No lookup specified. Add to results and continue to the next address.
			p.resolved[addr] = []MetaTarget{MetaTarget{addr: addr, sticky: sticky}}
			continue
		}
		qtype, name := qtypeAndName[0], qtypeAndName[1]

		resolved, err := p.resolver.Resolve(ctx, name, QType(qtype))
		p.resolverLookupsCount.Inc()
		if err != nil {
			// The DNS resolution failed. Continue without modifying the old records.
			p.resolverFailuresCount.Inc()
			level.Error(p.logger).Log("msg", "dns resolution failed", "addr", addr, "err", err, "sticky", sticky)
			continue
		}
		metaTargets := []MetaTarget{}
		for _, rTarget := range resolved {
			metaTargets = append(metaTargets, MetaTarget{addr: rTarget, sticky: sticky})
		}
		p.resolved[addr] = metaTargets
	}

	// Remove stored addresses that are no longer requested.
	for existingAddr := range p.resolved {
		if !contains(addrs, existingAddr) {
			delete(p.resolved, existingAddr)
			p.resolverAddrs.DeleteLabelValues(existingAddr)
		} else {
			p.resolverAddrs.WithLabelValues(existingAddr).Set(float64(len(p.resolved[existingAddr])))
		}
	}
}

// Addresses returns the latest addresses present in the Provider.
func (p *Provider) Addresses() []MetaTarget {
	p.Lock()
	defer p.Unlock()

	var result []MetaTarget
	for _, addrs := range p.resolved {
		result = append(result, addrs...)
	}
	return result
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if str == s {
			return true
		}
	}
	return false
}
