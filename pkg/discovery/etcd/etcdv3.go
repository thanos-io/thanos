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
	client *etcdv3.Client
	logger log.Logger
}

func NewEtcdV3Client(ctx context.Context, logger log.Logger, addrs []string) (*EtcdV3Client, error) {
	// machines := []string{"http://10.99.242.255:2379"}

	options := etcdv3.ClientOptions{}

	client, err := etcdv3.NewClient(ctx, addrs, options)
	if err != nil {
		level.Error(logger).Log("msg", "connect to service discovery server failed", "addr", addrs, "err", err)
		return nil, err
	}

	return &EtcdV3Client{client: &client, logger: logger}, nil

}

func (s *EtcdV3Client) Register(roleType cluster.Role, address string) error {
	service := etcdv3.Service{
		Key:   fmt.Sprintf("%s%s/%s", cluster.RootPath, roleType, address),
		Value: address,
	}

	level.Info(s.logger).Log("msg", "register", "key", fmt.Sprintf("%s%s/%s", cluster.RootPath, roleType, address))

	err := (*(s.client)).Register(service)
	return err
}

func (s *EtcdV3Client) RoleState(types ...cluster.Role) ([]string, error) {
	addresses := []string{}
	for _, t := range types {
		path := fmt.Sprintf("%s%s/", cluster.RootPath, t)
		fmt.Println(path)

		data, err := (*(s.client)).GetEntries(path)
		if err != nil {
			level.Info(s.logger).Log("msg", "client GetEntires fail", "err", err)
			continue
		}
		addresses = append(addresses, data...)
	}
	return addresses, nil
}

// TODO: add a watch func here?
