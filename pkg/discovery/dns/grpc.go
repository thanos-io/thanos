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

const resolveNowMinInterval = 200 * time.Millisecond

var (
	_ grpcresolver.Builder  = &builder{}
	_ grpcresolver.Resolver = &resolver{}
)

type builder struct {
	resolveInterval     time.Duration
	provider            *Provider
	logger              log.Logger
	injectTestAddresses []string
}

func RegisterGRPCResolver(logger log.Logger, provider *Provider, interval time.Duration, injectTestAddresses []string) {
	grpcresolver.Register(&builder{
		resolveInterval:     interval,
		provider:            provider,
		logger:              logger,
		injectTestAddresses: injectTestAddresses,
	})
}

func (b *builder) Scheme() string { return "thanos" }

func (b *builder) Build(t grpcresolver.Target, cc grpcresolver.ClientConn, _ grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &resolver{
		provider:            b.provider,
		target:              t.Endpoint(),
		ctx:                 ctx,
		cancel:              cancel,
		cc:                  cc,
		interval:            b.resolveInterval,
		resolveNowCh:        make(chan struct{}, 1),
		logger:              b.logger,
		injectTestAddresses: b.injectTestAddresses,
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

	target       string
	ctx          context.Context
	cancel       context.CancelFunc
	cc           grpcresolver.ClientConn
	interval     time.Duration
	resolveNowCh chan struct{}

	wg                  sync.WaitGroup
	logger              log.Logger
	injectTestAddresses []string
}

func (r *resolver) Close() {
	r.cancel()
	r.wg.Wait()
}

func (r *resolver) ResolveNow(_ grpcresolver.ResolveNowOptions) {
	select {
	case r.resolveNowCh <- struct{}{}:
	default:
	}
}

func (r *resolver) resolve() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.interval)
	defer cancel()
	return r.provider.Resolve(ctx, []string{r.target}, false)
}

func (r *resolver) addresses() []string {
	if len(r.injectTestAddresses) > 0 {
		return r.injectTestAddresses
	}
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

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	lastResolve := time.Now()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
		case <-r.resolveNowCh:
			if wait := resolveNowMinInterval - time.Since(lastResolve); wait > 0 {
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(wait):
				}
			}
			level.Debug(r.logger).Log("msg", "gRPC endpoint re-resolution requested", "target", r.target)
		}
		if err := r.updateResolver(); err != nil {
			level.Error(r.logger).Log("msg", "failed to update state for gRPC resolver", "err", err)
		}
		lastResolve = time.Now()
	}
}
