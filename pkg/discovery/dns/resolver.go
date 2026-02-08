// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/pkg/errors"
)

type QType string

const (
	// A qtype performs A/AAAA lookup.
	A = QType("dns")
	// SRV qtype performs SRV lookup with A/AAAA lookup for each SRV result.
	SRV = QType("dnssrv")
	// SRVNoA qtype performs SRV lookup without any A/AAAA lookup for each SRV result.
	SRVNoA = QType("dnssrvnoa")
	// ADualStack qtype performs both A and AAAA lookup, returning all addresses.
	ADualStack = QType("dnsdualstack")
)

type Resolver interface {
	// Resolve performs a DNS lookup and returns a list of records.
	// name is the domain name to be resolved.
	// qtype is the query type. Accepted values are `dns` for A/AAAA lookup, `dnssrv` for SRV lookup,
	// `dnssrvnoa` for SRV lookup without A/AAAA, and `dnsdualstack` for combined A and AAAA lookup.
	// If scheme is passed through name, it is preserved on IP results.
	Resolve(ctx context.Context, name string, qtype QType) ([]string, error)
}

type ipLookupResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupIPAddrByNetwork(ctx context.Context, network, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
	IsNotFound(err error) bool
}

type dnsSD struct {
	resolver ipLookupResolver
	logger   log.Logger
}

// NewResolver creates a resolver with given underlying resolver.
func NewResolver(resolver ipLookupResolver, logger log.Logger) Resolver {
	return &dnsSD{resolver: resolver, logger: logger}
}

func (s *dnsSD) Resolve(ctx context.Context, name string, qtype QType) ([]string, error) {
	var (
		res    []string
		scheme string
	)

	schemeSplit := strings.Split(name, "//")
	if len(schemeSplit) > 1 {
		scheme = schemeSplit[0]
		name = schemeSplit[1]
	}

	// Split the host and port if present.
	host, port, err := net.SplitHostPort(name)
	if err != nil {
		// The host could be missing a port.
		host, port = name, ""
	}

	switch qtype {
	case A:
		if port == "" {
			return nil, errors.Errorf("missing port in address given for dns lookup: %v", name)
		}
		ips, err := s.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			// We exclude error from std Golang resolver for the case of the domain (e.g `NXDOMAIN`) not being found by DNS
			// server. Since `miekg` does not consider this as an error,  when the host cannot be found, empty slice will be
			// returned.
			if !s.resolver.IsNotFound(err) {
				return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
			}
			if ips == nil {
				level.Error(s.logger).Log("msg", "failed to lookup IP addresses", "host", host, "err", err)
			}
		}
		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
		}
	case SRV, SRVNoA:
		_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
		if err != nil {
			if !s.resolver.IsNotFound(err) {
				return nil, errors.Wrapf(err, "lookup SRV records %q", host)
			}
			if len(recs) == 0 {
				level.Error(s.logger).Log("msg", "failed to lookup SRV records", "host", host, "err", err)
			}
		}

		for _, rec := range recs {
			// Only use port from SRV record if no explicit port was specified.
			resPort := port
			if resPort == "" {
				resPort = strconv.Itoa(int(rec.Port))
			}

			if qtype == SRVNoA {
				// Remove the final dot from rooted DNS names (this is for compatibility with Prometheus)
				target := strings.TrimRight(rec.Target, ".")
				res = append(res, appendScheme(scheme, net.JoinHostPort(target, resPort)))
				continue
			}
			// Do A lookup for the domain in SRV answer.
			resIPs, err := s.resolver.LookupIPAddr(ctx, rec.Target)
			if err != nil {
				if !s.resolver.IsNotFound(err) {
					return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
				}
				if len(resIPs) == 0 {
					level.Error(s.logger).Log("msg", "failed to lookup IP addresses", "srv", host, "a", rec.Target, "err", err)
				}
			}
			for _, resIP := range resIPs {
				res = append(res, appendScheme(scheme, net.JoinHostPort(resIP.String(), resPort)))
			}
		}
	case ADualStack:
		if port == "" {
			return nil, errors.Errorf("missing port in address given for dnsdualstack lookup: %v", name)
		}
		var ips []net.IPAddr
		var lastErr error

		for _, network := range []string{"ip6", "ip4"} {
			addrs, err := s.resolver.LookupIPAddrByNetwork(ctx, network, host)
			if err != nil {
				if !s.resolver.IsNotFound(err) {
					lastErr = err
				}
				continue
			}
			ips = append(ips, addrs...)
		}

		if len(ips) == 0 && lastErr != nil {
			return nil, errors.Wrapf(lastErr, "lookup IP addresses (dual-stack) %q", host)
		}

		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
		}
		if len(ips) == 0 {
			level.Error(s.logger).Log("msg", "failed to lookup IP addresses (dual-stack)", "host", host)
		}
	default:
		return nil, errors.Errorf("invalid lookup scheme %q", qtype)
	}

	if res == nil && err == nil {
		level.Warn(s.logger).Log("msg", "IP address lookup yielded no results. No host found or no addresses found", "host", host)
	}

	return res, nil
}

func appendScheme(scheme, host string) string {
	if scheme == "" {
		return host
	}
	return scheme + "//" + host
}
