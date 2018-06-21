package main

import (
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func regCommonServerFlags(cmd *kingpin.CmdClause) (*string, *string, func(log.Logger, *prometheus.Registry, bool, string, bool) (*cluster.Peer, error)) {
	grpcBindAddr := cmd.Flag("grpc-address", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components if you use gossip, 'grpc-advertise-address' is empty and you require cross-node connection.").
		Default("0.0.0.0:10901").String()

	grpcAdvertiseAddr := cmd.Flag("grpc-advertise-address", "Explicit (external) host:port address to advertise for gRPC StoreAPI in gossip cluster. If empty, 'grpc-address' will be used.").
		String()

	httpBindAddr := regHTTPAddrFlag(cmd)

	clusterBindAddr := cmd.Flag("cluster.address", "Listen ip:port address for gossip cluster.").
		Default("0.0.0.0:10900").String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "Explicit (external) ip:port address to advertise for gossip in gossip cluster. Used internally for membership only").
		String()

	peers := cmd.Flag("cluster.peers", "Initial peers to join the cluster. It can be either <ip:port>, or <domain:port>. A lookup resolution is done only at the startup.").Strings()

	gossipInterval := cmd.Flag("cluster.gossip-interval", "Interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth.").
		Default(cluster.DefaultGossipInterval.String()).Duration()

	pushPullInterval := cmd.Flag("cluster.pushpull-interval", "Interval for gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.").
		Default(cluster.DefaultPushPullInterval.String()).Duration()

	refreshInterval := cmd.Flag("cluster.refresh-interval", "Interval for membership to refresh cluster.peers state, 0 disables refresh.").Default(cluster.DefaultRefreshInterval.String()).Duration()

	return grpcBindAddr,
		httpBindAddr,
		func(logger log.Logger, reg *prometheus.Registry, waitIfEmpty bool, httpAdvertiseAddr string, queryAPIEnabled bool) (*cluster.Peer, error) {
			host, port, err := cluster.CalculateAdvertiseAddress(*grpcBindAddr, *grpcAdvertiseAddr)
			if err != nil {
				return nil, errors.Wrapf(err, "calculate advertise StoreAPI addr for gossip based on bindAddr: %s and advAddr: %s", *grpcBindAddr, *grpcAdvertiseAddr)
			}

			advStoreAPIAddress := fmt.Sprintf("%s:%d", host, port)
			if cluster.IsUnroutable(advStoreAPIAddress) {
				level.Warn(logger).Log("msg", "this component advertises its gRPC StoreAPI on an unroutable address. This will not work cross-cluster", "addr", advStoreAPIAddress)
				level.Warn(logger).Log("msg", "provide --grpc-address as routable ip:port or --grpc-advertise-address as a routable host:port")
			}

			level.Info(logger).Log("msg", "StoreAPI address that will be propagated through gossip", "address", advStoreAPIAddress)

			advQueryAPIAddress := httpAdvertiseAddr
			if queryAPIEnabled {
				host, port, err := cluster.CalculateAdvertiseAddress(*httpBindAddr, advQueryAPIAddress)
				if err != nil {
					return nil, errors.Wrapf(err, "calculate advertise QueryAPI addr for gossip based on bindAddr: %s and advAddr: %s", *httpBindAddr, advQueryAPIAddress)
				}

				advQueryAPIAddress = fmt.Sprintf("%s:%d", host, port)
				if cluster.IsUnroutable(advQueryAPIAddress) {
					level.Warn(logger).Log("msg", "this component advertises its HTTP QueryAPI on an unroutable address. This will not work cross-cluster", "addr", advQueryAPIAddress)
					level.Warn(logger).Log("msg", "provide --http-address as routable ip:port or --http-advertise-address as a routable host:port")
				}

				level.Info(logger).Log("msg", "QueryAPI address that will be propagated through gossip", "address", advQueryAPIAddress)
			}

			return cluster.New(logger, reg, *clusterBindAddr, *clusterAdvertiseAddr, advStoreAPIAddress, advQueryAPIAddress, *peers, waitIfEmpty, *gossipInterval, *pushPullInterval, *refreshInterval)
		}
}

func regHTTPAddrFlag(cmd *kingpin.CmdClause) *string {
	return cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
}
