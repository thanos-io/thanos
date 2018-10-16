package dns

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type ServiceDiscoverer interface {

	Resolve(ctx context.Context, addrs []string, defaultPort int) ([]*url.URL, error)
}

type ipSrvResolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

type dnsSD struct {
	resolver ipSrvResolver
}

func NewServiceDiscoverer(resolver *net.Resolver) ServiceDiscoverer {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &dnsSD{resolver: resolver}
}

func (s dnsSD) Resolve(ctx context.Context, addrs []string, defaultPort int) ([]*url.URL, error) {
	var res []*url.URL
	for _, addr := range addrs {
		// Check if a valid IP. Strip the port for the purpose of the check if it is present.
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			// The provided addr can still be a valid IP, just be missing a port. Add the port and check.
			host = addr
			port = strconv.Itoa(defaultPort)
		}
		ip := net.ParseIP(host)
		if ip != nil {
			// The address is a valid IP, add it to the results and avoid further lookup.
			// TODO(ivan): confirm if adding a default port is desired
			res = append(res, &url.URL{
				Host: net.JoinHostPort(host, port),
			})
			continue
		}

		// The addr is not an IP so treat it like a domain name
		u, err := url.Parse(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "parse URL %q", addr)
		}
		host, port, err = net.SplitHostPort(u.Host)
		if err != nil {
			host, port = u.Host, ""
		}
		var (
			hosts  []string
			proto  = u.Scheme
			lookup = "none"
		)
		if ps := strings.SplitN(u.Scheme, "+", 2); len(ps) == 2 {
			lookup, proto = ps[0], ps[1]
		}
		switch lookup {
		case "dns":
			if port == "" {
				port = strconv.Itoa(defaultPort)
			}
			ips, err := s.resolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, errors.Wrapf(err, "lookup IP addresses %q", host)
			}
			for _, ip := range ips {
				hosts = append(hosts, net.JoinHostPort(ip.String(), port))
			}
		case "dnssrv":
			_, recs, err := s.resolver.LookupSRV(ctx, "", proto, host)
			if err != nil {
				return nil, errors.Wrapf(err, "lookup SRV records %q", host)
			}
			for _, rec := range recs {
				// Only use port from SRV record if no explicit port was specified.
				srvPort := port
				if srvPort == "" {
					srvPort = strconv.Itoa(int(rec.Port))
				}
				hosts = append(hosts, net.JoinHostPort(rec.Target, srvPort))
			}
		case "none":
			if port == "" {
				port = strconv.Itoa(defaultPort)
			}
			hosts = append(hosts, net.JoinHostPort(host, port))
		default:
			return nil, errors.Errorf("invalid lookup scheme %q", lookup)
		}

		for _, h := range hosts {
			res = append(res, &url.URL{
				Scheme: proto,
				Host:   h,
				Path:   u.Path,
				User:   u.User,
			})
		}
	}

	return res, nil
}
