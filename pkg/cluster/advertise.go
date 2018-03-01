package cluster

import (
	"net"

	"github.com/hashicorp/go-sockaddr"
	"github.com/pkg/errors"
)

// calculateAdvertiseIP attempts to clone logic from deep within memberlist
// (NetTransport.FinalAdvertiseAddr) in order to surface its conclusions to the
// application, so we can provide more actionable error messages if the user has
// inadvertantly misconfigured their cluster.
//
// https://github.com/hashicorp/memberlist/blob/022f081/net_transport.go#L126
func calculateAdvertiseIP(bindIP, advertiseIP string) (net.IP, error) {
	if advertiseIP != "" {
		ip := net.ParseIP(advertiseIP)
		if ip == nil {
			return nil, errors.Errorf("failed to parse advertise IP '%s'", advertiseIP)
		}
		if ip4 := ip.To4(); ip4 != nil {
			ip = ip4
		}
		return ip, nil
	}

	if bindIP == "0.0.0.0" {
		privateIP, err := sockaddr.GetPrivateIP()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get private IP")
		}
		if privateIP == "" {
			return nil, errors.Wrap(err, "no private IP found, explicit advertise IP not provided")
		}
		ip := net.ParseIP(privateIP)
		if ip == nil {
			return nil, errors.Errorf("failed to parse private IP '%s'", privateIP)
		}
		return ip, nil
	}

	ip := net.ParseIP(bindIP)
	if ip == nil {
		return nil, errors.Errorf("failed to parse bind IP '%s'", bindIP)
	}
	return ip, nil
}
