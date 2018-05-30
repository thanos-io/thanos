package cluster

import (
	"net"

	"strconv"

	"github.com/hashicorp/go-sockaddr"
	"github.com/pkg/errors"
)

// CalculateAdvertiseAddress deduce the external, advertise address that should be routable from other components.
func CalculateAdvertiseAddress(bindAddr, advertiseAddr string) (string, int, error) {
	if advertiseAddr != "" {
		advHost, advPort, err := net.SplitHostPort(advertiseAddr)
		if err != nil {
			return "", 0, errors.Wrap(err, "invalid advertise address")
		}
		advIntPort, err := strconv.Atoi(advPort)
		if err != nil {
			return "", 0, errors.Wrapf(err, "invalid advertise address '%s', wrong port", advertiseAddr)
		}
		return advHost, advIntPort, nil
	}

	// We expect bindAddr to be in form of ip:port.
	bindHost, bindPort, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return "", 0, errors.Wrapf(err, "invalid bind address '%s'", bindAddr)
	}

	bindIntPort, err := strconv.Atoi(bindPort)
	if err != nil {
		return "", 0, errors.Wrapf(err, "invalid bind address '%s', wrong port", bindAddr)
	}

	if bindIntPort == 0 {
		return "", 0, errors.Errorf("invalid bind address '%s'. We don't allow port to be 0", bindAddr)
	}

	if bindHost == "" || bindHost == "0.0.0.0" {
		privateIP, err := sockaddr.GetPrivateIP()
		if err != nil {
			return "", 0, errors.Wrap(err, "failed to get private IP")
		}
		if privateIP == "" {
			return "", 0, errors.Wrap(err, "no private IP found, explicit advertise addr not provided")
		}
		return privateIP, bindIntPort, nil
	}

	return bindHost, bindIntPort, nil
}
