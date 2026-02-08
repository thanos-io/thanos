// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package godns

import (
	"context"
	"net"

	"github.com/pkg/errors"
)

// Resolver is a wrapper for net.Resolver.
type Resolver struct {
	*net.Resolver
}

func (r *Resolver) LookupIPAddrByNetwork(ctx context.Context, network, host string) ([]net.IPAddr, error) {
	ips, err := r.LookupIP(ctx, network, host)
	if err != nil {
		return nil, err
	}
	result := make([]net.IPAddr, len(ips))
	for i, ip := range ips {
		result[i] = net.IPAddr{IP: ip}
	}
	return result, nil
}

// IsNotFound checkout if DNS record is not found.
func (r *Resolver) IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	err = errors.Cause(err)
	dnsErr, ok := err.(*net.DNSError)
	return ok && dnsErr.IsNotFound
}
