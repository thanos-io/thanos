package zk

import (
	"fmt"

	"github.com/go-kit/kit/log/level"

	"github.com/improbable-eng/thanos/pkg/cluster"

	"github.com/go-kit/kit/log"

	"github.com/go-kit/kit/sd/zk"
)

type ZKClient struct {
	client zk.Client
	logger log.Logger
}

func NewZKClient(logger log.Logger, addrs []string, sdSecureOptions map[string]string) (*ZKClient, error) {
	// TODO: add ACL / Credentials here
	options := []zk.Option{}

	client, err := zk.NewClient(addrs, logger, options...)
	if err != nil {
		level.Error(logger).Log("msg", "connect to ZooKeeper server failed", "addr", addrs, "err", err)
		return nil, err
	}

	return &ZKClient{client: client, logger: logger}, nil
}

func (s *ZKClient) Register(role cluster.Role, address string) error {
	path := fmt.Sprintf("%s%s/", cluster.RootPath, role)
	service := &zk.Service{
		Path: path,
		Name: address,
		Data: []byte(address),
	}
	level.Info(s.logger).Log("msg", "register to ZooKeeper", "key", path)

	// NOTE: the go-kit/kit/sd/ `createparentNode` will create `/thanos/[role]/[address]/` without data
	err := s.client.Register(service)
	return err
}

func (s *ZKClient) RoleState(roles ...cluster.Role) ([]string, error) {
	addresses := []string{}

	for _, r := range roles {
		path := fmt.Sprintf("%s%s", cluster.RootPath, r)
		nodes, _, err := s.client.GetEntries(path)
		if err != nil {
			level.Info(s.logger).Log("msg", "GetEntires from ZooKeeper failed", "path", path, "err", err)
			continue
		}

		// should not return empty address, while go-kit/kit/sd/zk will create empty node
		for _, node := range nodes {
			if node != "" {
				addresses = append(addresses, node)
			}
		}
	}
	return addresses, nil
}
