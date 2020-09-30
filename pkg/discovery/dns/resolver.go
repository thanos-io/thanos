// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package dns

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

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
)

type Resolver interface {
	// Resolve performs a DNS lookup and returns a list of records.
	// name is the domain name to be resolved.
	// qtype is the query type. Accepted values are `dns` for A/AAAA lookup and `dnssrv` for SRV lookup.
	// If scheme is passed through name, it is preserved on IP results.
	Resolve(ctx context.Context, name string, qtype QType) ([]string, error)
}

type ipLookupResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

type dnsSD struct {
	resolver ipLookupResolver
	logger   log.Logger
	// https://github.com/thanos-io/thanos/issues/3186
	// This flag is used to prevent components from crashing if hosts are not found.
	returnErrOnNotFound bool
}

// NewResolver creates a resolver with given underlying resolver.
func NewResolver(resolver ipLookupResolver, logger log.Logger, returnErrOnNotFound bool) Resolver {
	return &dnsSD{resolver: resolver, logger: logger, returnErrOnNotFound: returnErrOnNotFound}
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
			dnsErr, ok := err.(*net.DNSError)
			// https://github.com/thanos-io/thanos/issues/3186
			// Default DNS resolver can make thanos components crash if DSN resolutions results in EAI_NONAME.
			// the flag returnErrOnNotFound can be used to prevent such crash.
			if !(!s.returnErrOnNotFound && ok && dnsErr.IsNotFound) {
				return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
			}
			level.Error(s.logger).Log("msg", "failed to lookup IP addresses", "host", host, "err", err)
		}
		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
		}
	case SRV, SRVNoA:
		_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup SRV records %q", host)
		}

		for _, rec := range recs {
			// Only use port from SRV record if no explicit port was specified.
			resPort := port
			if resPort == "" {
				resPort = strconv.Itoa(int(rec.Port))
			}

			if qtype == SRVNoA {
				res = append(res, appendScheme(scheme, net.JoinHostPort(rec.Target, resPort)))
				continue
			}
			// Do A lookup for the domain in SRV answer.
			resIPs, err := s.resolver.LookupIPAddr(ctx, rec.Target)
			if err != nil {
				return nil, errors.Wrapf(err, "look IP addresses %q", rec.Target)
			}
			for _, resIP := range resIPs {
				res = append(res, appendScheme(scheme, net.JoinHostPort(resIP.String(), resPort)))
			}
		}
	default:
		return nil, errors.Errorf("invalid lookup scheme %q", qtype)
	}

	// https://github.com/thanos-io/thanos/issues/3186
	// This happens when miekg is used as resolver. When the host cannot be found, nothing is returned.
	if res == nil && err == nil {
		level.Warn(s.logger).Log("msg", "IP address lookup yielded no results nor errors", "host", host)
	}

	return res, nil
}

func appendScheme(scheme, host string) string {
	if scheme == "" {
		return host
	}
	return scheme + "//" + host
}
