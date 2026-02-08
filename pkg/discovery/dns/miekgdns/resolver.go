// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package miekgdns

import (
	"context"
	"net"

	"github.com/miekg/dns"
	"github.com/pkg/errors"
)

// DefaultResolvConfPath is a common, default resolv.conf file present on linux server.
const DefaultResolvConfPath = "/etc/resolv.conf"

// Resolver is a drop-in Resolver for *part* of std lib Golang net.DefaultResolver methods.
type Resolver struct {
	ResolvConf string
}

func (r *Resolver) LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error) {
	return r.lookupSRV(service, proto, name, 1, 8)
}

func (r *Resolver) lookupSRV(service, proto, name string, currIteration, maxIterations int) (cname string, addrs []*net.SRV, err error) {
	// We want to protect from infinite loops when resolving DNS records recursively.
	if currIteration > maxIterations {
		return "", nil, errors.Errorf("maximum number of recursive iterations reached (%d)", maxIterations)
	}
	var target string
	if service == "" && proto == "" {
		target = name
	} else {
		target = "_" + service + "._" + proto + "." + name
	}

	response, err := r.lookupWithSearchPath(target, dns.Type(dns.TypeSRV))
	if err != nil {
		return "", nil, err
	}

	for _, record := range response.Answer {
		switch addr := record.(type) {
		case *dns.SRV:
			addrs = append(addrs, &net.SRV{
				Weight:   addr.Weight,
				Target:   addr.Target,
				Priority: addr.Priority,
				Port:     addr.Port,
			})
		case *dns.CNAME:
			// Recursively resolve it.
			_, resp, err := r.lookupSRV("", "", addr.Target, currIteration+1, maxIterations)
			if err != nil {
				return "", nil, errors.Wrapf(err, "recursively resolve %s", addr.Target)
			}
			addrs = append(addrs, resp...)
		default:
			return "", nil, errors.Errorf("invalid SRV response record %s", record)
		}
	}

	return "", addrs, nil
}

func (r *Resolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	return r.lookupIPAddr(host, 1, 8)
}

func (r *Resolver) LookupIPAddrByNetwork(ctx context.Context, network, host string) ([]net.IPAddr, error) {
	var qtype uint16
	switch network {
	case "ip6":
		qtype = dns.TypeAAAA
	case "ip4":
		qtype = dns.TypeA
	default:
		return nil, errors.Errorf("unsupported network %q", network)
	}
	return r.lookupIPAddrByNetwork(ctx, host, qtype, 1, 8)
}

func (r *Resolver) lookupIPAddrByNetwork(ctx context.Context, host string, qtype uint16, currIteration, maxIterations int) ([]net.IPAddr, error) {
	if currIteration > maxIterations {
		return nil, errors.Errorf("maximum number of recursive iterations reached (%d)", maxIterations)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	response, err := r.lookupWithSearchPath(host, dns.Type(qtype))
	if err != nil {
		return nil, err
	}

	var result []net.IPAddr
	for _, record := range response.Answer {
		switch addr := record.(type) {
		case *dns.A:
			result = append(result, net.IPAddr{IP: addr.A})
		case *dns.AAAA:
			result = append(result, net.IPAddr{IP: addr.AAAA})
		case *dns.CNAME:
			addrs, err := r.lookupIPAddrByNetwork(ctx, addr.Target, qtype, currIteration+1, maxIterations)
			if err != nil {
				continue
			}
			result = append(result, addrs...)
		}
	}

	if len(result) == 0 {
		return nil, ErrNoSuchHost
	}
	return result, nil
}

func (r *Resolver) lookupIPAddr(host string, currIteration, maxIterations int) ([]net.IPAddr, error) {
	// We want to protect from infinite loops when resolving DNS records recursively.
	if currIteration > maxIterations {
		return nil, errors.Errorf("maximum number of recursive iterations reached (%d)", maxIterations)
	}

	response, err := r.lookupWithSearchPath(host, dns.Type(dns.TypeAAAA))
	if err != nil || len(response.Answer) == 0 {
		// Ugly fallback to A lookup.
		response, err = r.lookupWithSearchPath(host, dns.Type(dns.TypeA))
		if err != nil {
			return nil, err
		}
	}

	var resp []net.IPAddr
	for _, record := range response.Answer {
		switch addr := record.(type) {
		case *dns.A:
			resp = append(resp, net.IPAddr{IP: addr.A})
		case *dns.AAAA:
			resp = append(resp, net.IPAddr{IP: addr.AAAA})
		case *dns.CNAME:
			// Recursively resolve it.
			addrs, err := r.lookupIPAddr(addr.Target, currIteration+1, maxIterations)
			if err != nil {
				return nil, errors.Wrapf(err, "recursively resolve %s", addr.Target)
			}
			resp = append(resp, addrs...)
		default:
			return nil, errors.Errorf("invalid A, AAAA or CNAME response record %s", record)
		}
	}
	return resp, nil
}

func (r *Resolver) IsNotFound(err error) bool {
	return errors.Is(errors.Cause(err), ErrNoSuchHost)
}
