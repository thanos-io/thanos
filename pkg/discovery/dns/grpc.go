// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"sync"
	"time"

	grpcresolver "google.golang.org/grpc/resolver"
)

var (
	_ grpcresolver.Builder  = &builder{}
	_ grpcresolver.Resolver = &resolver{}
)

type builder struct {
	resolveInterval time.Duration
	provider        *Provider
}

func RegisterGRPCResolver(provider *Provider, interval time.Duration) {
	grpcresolver.Register(&builder{
		resolveInterval: interval,
		provider:        provider,
	})
}

func (b *builder) Scheme() string { return "thanos" }

func (b *builder) Build(t grpcresolver.Target, cc grpcresolver.ClientConn, _ grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &resolver{
		provider: b.provider,
		target:   t.Endpoint(),
		ctx:      ctx,
		cancel:   cancel,
		cc:       cc,
		interval: b.resolveInterval,
	}
	r.wg.Add(1)
	go r.run()

	return r, nil
}

type resolver struct {
	provider *Provider

	target   string
	ctx      context.Context
	cancel   context.CancelFunc
	cc       grpcresolver.ClientConn
	interval time.Duration

	wg sync.WaitGroup
}

func (r *resolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *resolver) ResolveNow(_ grpcresolver.ResolveNowOptions) {}

func (r *resolver) resolve() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.interval)
	defer cancel()
	return r.provider.Resolve(ctx, []string{r.target})
}

func (r *resolver) addresses() []string {
	return r.provider.AddressesForHost(r.target)
}

func (r *resolver) run() {
	defer r.wg.Done()
	for {
		if err := r.resolve(); err != nil {
			r.cc.ReportError(err)
		} else {
			state := grpcresolver.State{}
			for _, addr := range r.addresses() {
				raddr := grpcresolver.Address{Addr: addr}
				state.Addresses = append(state.Addresses, raddr)
			}
			_ = r.cc.UpdateState(state)
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.interval):
		}
	}
}
