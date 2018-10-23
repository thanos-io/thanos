package dns

import (
	"context"
	"strings"
	"sync"
)

type Provider struct {
	sync.Mutex
	resolver Resolver
	addrs    []string
}

func NewProvider(resolver Resolver) *Provider {
	return &Provider{resolver: resolver}
}

func (p *Provider) Resolve(ctx context.Context, addrs []string, defaultPort int) error {
	p.Lock()
	defer p.Unlock()

	var result []string
	for _, addr := range addrs {
		var (
			name          string
			qtype         string
			nameQtype     []string
			resolvedHosts []string
		)
		if nameQtype = strings.SplitN(addr, "+", 2); len(nameQtype) != 2 {
			// No lookup specified. Add to results and continue.
			result = append(result, addr)
			continue
		}
		name, qtype = nameQtype[1], nameQtype[0]
		resolvedHosts, err := p.resolver.Resolve(ctx, name, qtype, defaultPort)
		if err != nil {
			// The DNS resolution failed. Exit without modifying the old records.
			return err
		}
		result = append(result, resolvedHosts...)
	}

	p.addrs = result

	return nil
}

func (p *Provider) Addresses() []string {
	p.Lock()
	defer p.Unlock()
	cpy := make([]string, len(p.addrs))
	for i, addr := range p.addrs {
		cpy[i] = addr
	}
	return cpy
}
