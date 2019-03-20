package etcd

import (
	"context"
	"fmt"

	"github.com/go-kit/kit/log/level"

	"github.com/improbable-eng/thanos/pkg/cluster"

	"github.com/go-kit/kit/sd/etcdv3"

	"github.com/go-kit/kit/log"
)

type EtcdV3Client struct {
	client etcdv3.Client
	logger log.Logger
}

func NewEtcdV3Client(ctx context.Context, logger log.Logger, addrs []string, sdSecureOptions map[string]string) (*EtcdV3Client, error) {
	options := etcdv3.ClientOptions{}
	if cert, ok := sdSecureOptions["etcd-tls-cert"]; ok {
		options.Cert = cert
	}
	if key, ok := sdSecureOptions["etcd-tls-key"]; ok {
		options.Key = key
	}
	if ca, ok := sdSecureOptions["etcd-tls-ca"]; ok {
		options.CACert = ca
	}
	if username, ok := sdSecureOptions["etcd-username"]; ok {
		options.Username = username
	}
	if password, ok := sdSecureOptions["etcd-password"]; ok {
		options.Password = password
	}

	client, err := etcdv3.NewClient(ctx, addrs, options)
	if err != nil {
		level.Error(logger).Log("msg", "connect to EtcdV3 server failed", "addr", addrs, "err", err)
		return nil, err
	}

	return &EtcdV3Client{client: client, logger: logger}, nil

}

func (s *EtcdV3Client) Register(role cluster.Role, address string) error {
	key := fmt.Sprintf("%s%s/%s", cluster.RootPath, role, address)
	service := etcdv3.Service{
		Key:   key,
		Value: address,
	}

	level.Info(s.logger).Log("msg", "register to EtcdV3", "key", key)

	err := s.client.Register(service)
	return err
}

func (s *EtcdV3Client) RoleState(roles ...cluster.Role) ([]string, error) {
	addresses := []string{}
	for _, r := range roles {
		path := fmt.Sprintf("%s%s/", cluster.RootPath, r)
		data, err := s.client.GetEntries(path)
		if err != nil {
			level.Info(s.logger).Log("msg", "GetEntires from EtcdV3 failed", "err", err)
			continue
		}
		addresses = append(addresses, data...)
	}
	return addresses, nil
}

// TODO: add a watch func here?
