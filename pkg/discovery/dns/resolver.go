package dns

import (
	"context"
	"net"
	"strconv"

	"net/url"

	"github.com/pkg/errors"
)

type Resolver interface {
	// Resolve performs a DNS lookup and returns a list of records.
	// name is the domain name to be resolved.
	// qtype is the query type. Accepted values are `dns` for A/AAAA lookup and `dnssrv` for SRV lookup.
	// If qtype is `dns`, the domain name to be resolved requires a port or an error will be returned.
	// TODO(ivan): probably use custom type or an enum for qtype
	Resolve(ctx context.Context, name string, qtype string) ([]string, error)
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

func (s *dnsSD) Resolve(ctx context.Context, name string, qtype string) ([]string, error) {
	var (
		res      []string
		scheme   string
		hostPort = name
	)

	u, err := url.Parse(name)
	// If hostname is provided without a scheme or a // prefix, url.Parse assumes the hostname is the scheme and leaves
	// the Host field empty.
	if u.Host != "" {
		hostPort = u.Host
		scheme = u.Scheme
	}

	// Split the host and port if present.
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		// The host could be missing a port.
		host, port = hostPort, ""
	}

	switch qtype {
	case "dns":
		if port == "" {
			return nil, errors.Errorf("missing port in address given for dns lookup: %v", name)
		}
		ips, err := s.resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
		}
		for _, ip := range ips {
			res = append(res, appendScheme(scheme, net.JoinHostPort(ip.String(), port)))
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
			res = append(res, appendScheme(scheme, net.JoinHostPort(rec.Target, port)))
		}
	default:
		return nil, errors.Errorf("invalid lookup scheme %q", qtype)
	}

	return res, nil
}

func appendScheme(scheme, host string) string {
	if scheme == "" {
		return host
	}
	return scheme + "://" + host
}
