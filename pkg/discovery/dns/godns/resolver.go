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

func (r *Resolver) LookupIPAddrDualStack(ctx context.Context, host string) ([]net.IPAddr, error) {
	seen := make(map[string]struct{})
	var result []net.IPAddr

	for _, network := range []string{"ip6", "ip4"} {
		select {
		case <-ctx.Done():
			if len(result) > 0 {
				return result, nil
			}
			return nil, ctx.Err()
		default:
		}

		ips, err := r.LookupIP(ctx, network, host)
		if err != nil {
			continue
		}

		for _, ip := range ips {
			ipStr := ip.String()
			if _, ok := seen[ipStr]; !ok {
				seen[ipStr] = struct{}{}
				result = append(result, net.IPAddr{IP: ip})
			}
		}
	}

	if len(result) == 0 {
		return nil, &net.DNSError{
			Err:        "no such host",
			Name:       host,
			IsNotFound: true,
		}
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
