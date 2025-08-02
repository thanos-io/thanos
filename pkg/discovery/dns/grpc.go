// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	grpcresolver "google.golang.org/grpc/resolver"
)

var (
	_ grpcresolver.Builder  = &builder{}
	_ grpcresolver.Resolver = &resolver{}
)

type builder struct {
	resolveInterval time.Duration
	provider        *Provider
	logger          log.Logger
}

func RegisterGRPCResolver(logger log.Logger, provider *Provider, interval time.Duration) {
	grpcresolver.Register(&builder{
		resolveInterval: interval,
		provider:        provider,
		logger:          logger,
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
		logger:   b.logger,
	}

	// perform initial, synchronous resolution to populate the state.
	level.Info(r.logger).Log("msg", "performing initial gRPC endpoint resolution", "target", r.target)
	if err := r.updateResolver(); err != nil {
		level.Error(r.logger).Log("msg", "initial gRPC endpoint resolution failed", "target", r.target, "err", err)
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

	wg     sync.WaitGroup
	logger log.Logger
}

func (r *resolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *resolver) ResolveNow(_ grpcresolver.ResolveNowOptions) {}

func (r *resolver) resolve() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.interval)
	defer cancel()
	return r.provider.Resolve(ctx, []string{r.target}, false)
}

func (r *resolver) addresses() []string {
	return r.provider.AddressesForHost(r.target)
}

func (r *resolver) updateResolver() error {
	if err := r.resolve(); err != nil {
		r.cc.ReportError(err)
		return err
	}
	state := grpcresolver.State{}
	addrs := r.addresses()
	if len(addrs) == 0 {
		level.Info(r.logger).Log("msg", "no addresses resolved", "target", r.target)
		return nil
	}
	for _, addr := range addrs {
		state.Addresses = append(state.Addresses, grpcresolver.Address{Addr: addr})
	}
	if err := r.cc.UpdateState(state); err != nil {
		return err
	}
	return nil
}

func (r *resolver) run() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(r.interval):
			if err := r.updateResolver(); err != nil {
				level.Error(r.logger).Log("msg", "failed to update state for gRPC resolver", "err", err)
			}
		}
	}
}
