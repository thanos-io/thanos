package dns

import (
	"context"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Provider is a stateful cache for asynchronous DNS resolutions. It provides a way to resolve addresses and obtain them.
type Provider struct {
	sync.Mutex
	resolver Resolver
	// A map from domain name to a slice of resolved targets.
	resolved map[string][]string
	logger   log.Logger

	resolverLookupsCount  prometheus.Counter
	resolverFailuresCount prometheus.Counter
}

// NewProvider returns a new empty provider with a default resolver.
func NewProvider(logger log.Logger, reg prometheus.Registerer) *Provider {
	p := &Provider{
		resolver: NewResolver(),
		resolved: make(map[string][]string),
		logger:   logger,
		resolverLookupsCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:      "dns_lookups_total",
			Help:      "The number of DNS lookups resolutions attempts",
		}),
		resolverFailuresCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name:      "dns_failures_total",
			Help:      "The number of DNS lookup failures",
		}),
	}

	if reg != nil {
		reg.MustRegister(p.resolverLookupsCount)
		reg.MustRegister(p.resolverFailuresCount)
	}

	return p
}

// Resolve stores a list of provided addresses or their DNS records if requested.
// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
// defaultPort is used for non-SRV records when a port is not supplied.
func (p *Provider) Resolve(ctx context.Context, addrs []string) {
	p.Lock()
	defer p.Unlock()

	for _, addr := range addrs {
		var resolved []string
		qtypeAndName := strings.SplitN(addr, "+", 2)
		if len(qtypeAndName) != 2 {
			// No lookup specified. Add to results and continue to the next address.
			p.resolved[addr] = []string{addr}
			continue
		}
		qtype, name := qtypeAndName[0], qtypeAndName[1]

		resolved, err := p.resolver.Resolve(ctx, name, QType(qtype))
		p.resolverLookupsCount.Inc()
		if err != nil {
			// The DNS resolution failed. Continue without modifying the old records.
			p.resolverFailuresCount.Inc()
			level.Error(p.logger).Log("msg", "dns resolution failed", "addr", addr, "err", err)
			continue
		}
		p.resolved[addr] = resolved
	}

	// Remove stored addresses that are no longer requested.
	var entriesToDelete []string
	for existingAddr := range p.resolved {
		if !contains(addrs, existingAddr) {
			entriesToDelete = append(entriesToDelete, existingAddr)
		}
	}
	for _, toDelete := range entriesToDelete {
		delete(p.resolved, toDelete)
	}
}

// Addresses returns the latest addresses present in the Provider.
func (p *Provider) Addresses() []string {
	p.Lock()
	defer p.Unlock()

	var result []string
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
