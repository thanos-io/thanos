package discovery

import (
	"context"

	"github.com/pkg/errors"

	"github.com/improbable-eng/thanos/pkg/discovery/zk"

	"github.com/improbable-eng/thanos/pkg/cluster"

	"github.com/go-kit/kit/log"

	"github.com/improbable-eng/thanos/pkg/discovery/etcd"
)

type Client interface {
	Register(role cluster.Role, address string) error
	RoleState(types ...cluster.Role) ([]string, error)
}

func NewClient(ctx context.Context, logger log.Logger, t string, addrs []string, sdSecureOptions map[string]string) (Client, error) {
	switch t {
	case "etcdv3":
		return etcd.NewEtcdV3Client(ctx, logger, addrs, sdSecureOptions)
	case "zookeeper", "zk":
		return zk.NewZKClient(logger, addrs, sdSecureOptions)
	}
	return nil, errors.New("sdType Not Supported")
}
