package dns

import (
	"context"
	"sync"
)

type Provider struct {
	sync.Mutex
	discoverer ServiceDiscoverer
	resolved   []string
}

func NewProvider(discoverer ServiceDiscoverer) *Provider {
	return &Provider{discoverer: discoverer}
}

func (p *Provider) Resolve(ctx context.Context, addrs []string, defaultPort int) error {
	p.Lock()
	defer p.Unlock()
	urls, err := p.discoverer.Resolve(ctx, addrs, defaultPort)
	if err != nil {
		// The DNS resolution failed. Keep the old records and exit.
		return err
	}

	p.resolved = make([]string, len(urls))
	for i, url := range urls {
		p.resolved[i] = url.Host
	}

	return nil
}

func (p *Provider) Addresses() []string {
	p.Lock()
	defer p.Unlock()
	cpy := make([]string, len(p.resolved))
	for i, addr := range p.resolved {
		cpy[i] = addr
	}
	return cpy
}
