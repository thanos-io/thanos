package cluster

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/pkg/errors"
)

type PeerDiscovery interface {
	ResolvePeers(myAddress string, waitIfEmpty bool) ([]string, error)
}

func StaticPeerList(peers []string) func(log.Logger) PeerDiscovery {
	return func(logger log.Logger) PeerDiscovery {
		return &staticPeerDiscovery{
			peers:  peers,
			logger: logger,
		}
	}
}

type staticPeerDiscovery struct {
	peers  []string
	logger log.Logger
}

func (s *staticPeerDiscovery) ResolvePeers(myAddress string, waitIfEmpty bool) ([]string, error) {
	res := net.DefaultResolver
	level.Debug(s.logger).Log("msg", "Resolving peers", "peers", strings.Join(s.peers, ","))
	ctx := context.Background()
	var resolvedPeers []string

	for _, peer := range s.peers {
		host, port, err := net.SplitHostPort(peer)
		if err != nil {
			return nil, errors.Wrapf(err, "split host/port for peer %s", peer)
		}

		ips, err := res.LookupIPAddr(ctx, host)
		if err != nil {
			// Assume direct address.
			resolvedPeers = append(resolvedPeers, peer)
			continue
		}

		if len(ips) == 0 {
			var lookupErrSpotted bool
			retryCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			err := runutil.Retry(2*time.Second, retryCtx.Done(), func() error {
				if lookupErrSpotted {
					// We need to invoke cancel in next run of retry when lookupErrSpotted to preserve LookupIPAddr error.
					cancel()
				}

				ips, err = res.LookupIPAddr(retryCtx, host)
				if err != nil {
					lookupErrSpotted = true
					return errors.Wrapf(err, "IP Addr lookup for peer %s", peer)
				}

				ips = removeMyAddr(ips, port, myAddress)
				if len(ips) == 0 {
					if !waitIfEmpty {
						return nil
					}
					return errors.New("empty IPAddr result. Retrying")
				}

				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		for _, ip := range ips {
			resolvedPeers = append(resolvedPeers, net.JoinHostPort(ip.String(), port))
		}
	}

	return resolvedPeers, nil
}

func removeMyAddr(ips []net.IPAddr, targetPort string, myAddr string) []net.IPAddr {
	var result []net.IPAddr

	for _, ip := range ips {
		if net.JoinHostPort(ip.String(), targetPort) == myAddr {
			continue
		}
		result = append(result, ip)
	}

	return result
}
