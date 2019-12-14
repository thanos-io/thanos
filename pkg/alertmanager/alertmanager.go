package alertmanager

import (
	"context"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

const (
	defaultAlertmanagerPort = 9093
)

//  Alertmanager replica URLs to push firing alerts. Ruler claims success if
// push to at least one alertmanager from discovered succeeds. The scheme
//should not be empty e.g `http` might be used. The scheme may be prefixed
//with 'dns+' or 'dnssrv+' to detect Alertmanager IPs through respective
//DNS lookups. The port defaults to 9093 or the SRV record's value.
//The URL path is used as a prefix for the regular Alertmanager API path.
type AlertManager interface {
	// Gets the address of the configured alertmanager
	Get() []*url.URL

	// Update and parse the raw url
	Update(ctx context.Context) error
}

type alertmanagerSet struct {
	resolver dns.Resolver
	addrs    []string
	mtx      sync.Mutex
	current  []*url.URL
}

func NewAlertmanagerSet(logger log.Logger, addrs []string, dnsSDResolver dns.ResolverType) *alertmanagerSet {
	return &alertmanagerSet{
		resolver: dns.NewResolver(dnsSDResolver.ToResolver(logger)),
		addrs:    addrs,
	}
}

// Gets the address of the configured alertmanager
func (s *alertmanagerSet) Get() []*url.URL {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.current
}

// Update and parse the raw url
func (s *alertmanagerSet) Update(ctx context.Context) error {
	var result []*url.URL
	for _, addr := range s.addrs {
		var (
			qtype         dns.QType
			resolvedHosts []string
		)

		qtype, u, err := parseAlertmanagerAddress(addr)
		if err != nil {
			return errors.Wrapf(err, "parse URL %q", addr)
		}

		// Get only the host and resolve it if needed.
		host := u.Host
		if qtype != "" {
			if qtype == dns.A {
				_, _, err = net.SplitHostPort(host)
				if err != nil {
					// The host could be missing a port. Append the defaultAlertmanagerPort.
					host = host + ":" + strconv.Itoa(defaultAlertmanagerPort)
				}
			}
			resolvedHosts, err = s.resolver.Resolve(ctx, host, qtype)
			if err != nil {
				return errors.Wrap(err, "alertmanager resolve")
			}
		} else {
			resolvedHosts = []string{host}
		}

		for _, host := range resolvedHosts {
			result = append(result, &url.URL{
				Scheme: u.Scheme,
				Host:   host,
				Path:   u.Path,
				User:   u.User,
			})
		}
	}

	s.mtx.Lock()
	s.current = result
	s.mtx.Unlock()

	return nil
}

func parseAlertmanagerAddress(addr string) (qType dns.QType, parsedUrl *url.URL, err error) {
	qType = ""
	parsedUrl, err = url.Parse(addr)
	if err != nil {
		return qType, nil, err
	}

	// The Scheme might contain DNS resolver type separated by + so we split it a part.
	if schemeParts := strings.Split(parsedUrl.Scheme, "+"); len(schemeParts) > 1 {
		parsedUrl.Scheme = schemeParts[len(schemeParts)-1]
		qType = dns.QType(strings.Join(schemeParts[:len(schemeParts)-1], "+"))
	}

	switch parsedUrl.Scheme {
	case "http", "https":
	case "":
		return "", nil, errors.New("The scheme should not be empty, e.g `http` or `https`")
	default:
		return "", nil, errors.New("Scheme should  be `http` or `https`")
	}

	return qType, parsedUrl, err
}
