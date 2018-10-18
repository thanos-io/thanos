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
		// Check if lookup is needed and strip the prefix if present.
		ps := strings.SplitN(addr, "+", 2)
		if len(ps) != 2 {
			// Address does not contain lookup prefix. Don't resolve it and just return it.
			res = append(res, &url.URL{
				Host:   addr,
			})
			continue
		}
		// Note: we have no way of telling if the address contains an invalid lookup prefix, or the + is part of the url.
		// I believe the former is more likely, therefore setting ps[0] as the prefix seems like the better flow, as it
		// will result in an error returned to the user later on
		lookup, addrWithoutPrefix := ps[0], ps[1]

		// Check if valid url.
		u, err := url.Parse(addrWithoutPrefix)
		if err != nil {
			return nil, errors.Wrapf(err, "parse URL %q", addrWithoutPrefix)
		}
		unsplitHost := u.Host
		if unsplitHost == "" {
			// If scheme is not specified in the url, u.Host will be empty. Then use the original address.
			unsplitHost = addrWithoutPrefix
		}
		// Split the host and port if present.
		host, port, err := net.SplitHostPort(unsplitHost)
		if err != nil {
			// The host could be missing a port.
			host, port = unsplitHost, ""
		}
		var hosts  []string

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
			_, recs, err := s.resolver.LookupSRV(ctx, "", "", host)
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
		default:
			return nil, errors.Errorf("invalid lookup scheme %q", lookup)
		}

		for _, h := range hosts {
			res = append(res, &url.URL{
				Scheme: u.Scheme,
				Host:   h,
				Path:   u.Path,
				User:   u.User,
			})
		}
	}

	return res, nil
}