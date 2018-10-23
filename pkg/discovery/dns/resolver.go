package dns

import (
	"context"
	"net"
	"strconv"

	"github.com/pkg/errors"
)

type Resolver interface {
	// Resolve perorms a DNS lookup and returns a list of records.
	// name is the domain name to be resolved.
	// qtype is the query type. Accepted values are `dns` for A/AAAA lookup and `dnssrv` for SRV lookup.
	// defaultPort is used if no port is provided for a A/AAAA lookup.
	// TODO(ivan): probably use custom type or an enum for qtype
	Resolve(ctx context.Context, name string, qtype string, defaultPort int) ([]string, error)
}

type ipSrvResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

type dnsSD struct {
	resolver ipSrvResolver
}

// NewResolver provides a resolver with a specific net.Resolver. If resolver is nil, the default resolver will be used.
func NewResolver(resolver *net.Resolver) Resolver {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &dnsSD{resolver: resolver}
}

func (s *dnsSD) Resolve(ctx context.Context, name string, qtype string, defaultPort int) ([]string, error) {
	var res []string
	// Split the host and port if present.
	host, port, err := net.SplitHostPort(name)
	if err != nil {
		// The host could be missing a port.
		host, port = name, ""
	}

	switch qtype {
	case "dns":
		if port == "" {
			port = strconv.Itoa(defaultPort)
		}
		ips, err := s.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
		}
		for _, ip := range ips {
			res = append(res, net.JoinHostPort(ip.String(), port))
		}
	case "dnssrv":
		_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup SRV records %q", host)
		}
		for _, rec := range recs {
			// Only use port from SRV record if no explicit port was specified.
			if port == "" {
				port = strconv.Itoa(int(rec.Port))
			}
			res = append(res, net.JoinHostPort(rec.Target, port))
		}
	default:
		return nil, errors.Errorf("invalid lookup scheme %q", qtype)
	}

	return res, nil
}
