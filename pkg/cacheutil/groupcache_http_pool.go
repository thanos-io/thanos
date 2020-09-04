// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/groupcache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

type groupcacheHTTPPool struct {
	logger log.Logger

	config GroupcacheHTTPPoolConfig
	peers  *groupcache.HTTPPool

	// DNS provider used to keep the groupcache peer list updated.
	dnsProvider *dns.Provider

	// Channel used to notify internal goroutines when they should quit.
	stop chan struct{}
}

// NewGroupcacheHTTPPool makes a new MemcachedClient.
func NewGroupcacheHTTPPool(logger log.Logger, reg prometheus.Registerer, me string, conf []byte) (*groupcacheHTTPPool, error) {
	config, err := ParseGroupcacheConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewGroupcacheHTTPPoolWithConfig(logger, reg, me, config)
}

// NewGroupcacheHTTPPoolWithConfig makes a new MemcachedClient.
func NewGroupcacheHTTPPoolWithConfig(logger log.Logger, _ prometheus.Registerer, me string, config GroupcacheConfig) (*groupcacheHTTPPool, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	return &groupcacheHTTPPool{logger: logger, config: config.HTTPPoolConfig, peers: groupcache.NewHTTPPool(me)}, nil // me  TODO(kakkoyun): Test.
}

func (c *groupcacheHTTPPool) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	c.peers.ServeHTTP(resp, req)
}

func (c *groupcacheHTTPPool) UpdateLoop() error {
	ticker := time.NewTicker(c.config.DNSProviderUpdateInterval)
	defer ticker.Stop()

	// TODO(kakkoyun): Error handling.
	for {
		select {
		case <-ticker.C:
			err := c.resolveAddrs()
			if err != nil {
				level.Warn(c.logger).Log("msg", "failed update groupcache peer list", "err", err)
			}
		case <-c.stop:
			return nil
		}
	}
}

func (c *groupcacheHTTPPool) Stop() {
	close(c.stop)
}

func (c *groupcacheHTTPPool) resolveAddrs() error {
	// Resolve configured addresses with a reasonable timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// If some of the dns resolution fails, log the error.
	if err := c.dnsProvider.Resolve(ctx, c.config.Addresses); err != nil {
		level.Error(c.logger).Log("msg", "failed to resolve addresses for groupcache", "addresses", strings.Join(c.config.Addresses, ","), "err", err)
	}
	// Fail in case no server address is resolved.
	peerList := c.dnsProvider.Addresses()
	if len(peerList) == 0 {
		// TODO(kakkoyun): Do we really need to fail?
		return errors.New("no server address resolved")
	}

	c.peers.Set(peerList...)
	return nil
}
