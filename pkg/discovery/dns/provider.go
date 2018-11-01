package dns

import (
	"context"
	"strings"
	"sync"
)

// Provider is a store for DNS resolved addresses. It provides a way to resolve addresses and obtain them.
type Provider struct {
	sync.Mutex
	resolver Resolver
	addrs    []string
}

// NewProvider returns a new empty Provider. If resolver is nil, the default resolver will be used.
func NewProvider(resolver Resolver) *Provider {
	if resolver == nil {
		resolver = NewResolver(nil)
	}
	return &Provider{resolver: resolver}
}

// Resolve stores a list of provided addresses or their DNS records if requested.
// Addresses prefixed with `dns+` or `dnssrv+` will be resolved through respective DNS lookup (A/AAAA or SRV).
// defaultPort is used for non-SRV records when a port is not supplied.
func (p *Provider) Resolve(ctx context.Context, addrs []string) error {
	p.Lock()
	defer p.Unlock()

	var result []string
	for _, addr := range addrs {
		var resolvedHosts []string
		qtypeAndName := strings.SplitN(addr, "+", 2)
		if len(qtypeAndName) != 2 {
			// No lookup specified. Add to results and continue to the next address.
			result = append(result, addr)
			continue
		}
		qtype, name := qtypeAndName[0], qtypeAndName[1]
		resolvedHosts, err := p.resolver.Resolve(ctx, name, qtype)
		if err != nil {
			// The DNS resolution failed. Exit without modifying the old records.
			return err
		}
		result = append(result, resolvedHosts...)
	}

	p.addrs = result

	return nil
}

// Addresses returns the latest addresses present in the Provider.
func (p *Provider) Addresses() []string {
	p.Lock()
	defer p.Unlock()
	cp := make([]string, len(p.addrs))
	copy(cp, p.addrs)
	return cp
}
