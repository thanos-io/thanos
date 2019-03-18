package zk

import (
	"fmt"

	"github.com/go-kit/kit/log/level"

	"github.com/improbable-eng/thanos/pkg/cluster"

	"github.com/go-kit/kit/log"

	zk2 "github.com/samuel/go-zookeeper/zk"

	"github.com/go-kit/kit/sd/zk"
)

type ZKClient struct {
	client *zk.Client
	logger log.Logger
}

func NewZKClient(logger log.Logger, addrs []string) (*ZKClient, error) {

	// servers := []string{"10.99.242.255:2379"}
	handler := zk.EventHandler(func(event zk2.Event) {
		fmt.Println(event)
	})

	client, err := zk.NewClient(addrs, logger, handler)
	if err != nil {
		level.Error(logger).Log("msg", "connect to service discovery server failed", "addr", addrs, "err", err)
		return nil, err
	}

	return &ZKClient{client: &client, logger: logger}, nil
}

func (s *ZKClient) Register(roleType cluster.Role, address string) error {
	service := &zk.Service{
		Path: fmt.Sprintf("%s%s/", cluster.RootPath, roleType),
		Name: address,
		Data: []byte(address),
	}
	// TODO: createparentNode 会导致 /thanos/query/[address]/ 空, 然后register加入了 /thanos/query/_xxxx/有值是address

	level.Info(s.logger).Log("msg", "register", "key", fmt.Sprintf("%s%s/", cluster.RootPath, roleType))

	err := (*(s.client)).Register(service)
	return err
}

func (s *ZKClient) RoleState(types ...cluster.Role) ([]string, error) {
	addresses := []string{}

	for _, t := range types {
		path := fmt.Sprintf("%s%s", cluster.RootPath, t)
		nodes, _, err := (*(s.client)).GetEntries(path)
		if err != nil {
			level.Info(s.logger).Log("msg", "client GetEntires fail", "path", path, "err", err)
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
