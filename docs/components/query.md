# Query

The query component implements the Prometheus HTTP v1 API to query data in a Thanos cluster via PromQL.
It joins a Thanos cluster mesh and can access all data exposed by store APIs within it. It is fully stateless and horizontally scalable.

The querier needs to be passed an initial list of peers to through which it can join the Thanos cluster.
This can either be a repeated list of peer addresses or a DNS name, which it will resolve for peer IP addresses.

```
$ thanos query \
    --http-address     "0.0.0.0:9090" \
    --cluster.peers    "thanos-cluster.example.org" \
```

The query layer can deduplicate series that were collected from high-availability pairs of data sources such as Prometheus.
A fixed replica label must be chosen for the entire cluster and can then be passed to query nodes on startup.
Two or more series that have that are only distinguished by the given replica label, will be merged into a single time series. This also hides gaps in collection of a single data source.


```
$ thanos query \
    --http-address     "0.0.0.0:9090" \
    --replica-label    "replica" \
    --cluster.peers    "thanos-cluster.example.org" \
```

## Deployment

## Flags

[embedmd]:# (flags/query.txt $)
```$
usage: thanos query [<flags>]

query node exposing PromQL enabled Query API with data retrieved from multiple
store nodes

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                                 GCP project to send Google Cloud Trace tracings
                                 to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                                 How often we send traces (1/<sample-factor>).
                                 If 0 no trace will be sent periodically, unless
                                 forced by baggage item. See
                                 `pkg/tracing/tracing.go` for details.
      --grpc-address="0.0.0.0:10901"  
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components if you use gossip,
                                 'grpc-advertise-address' is empty and you
                                 require cross-node connection.
      --grpc-advertise-address=GRPC-ADVERTISE-ADDRESS  
                                 Explicit (external) host:port address to
                                 advertise for gRPC StoreAPI in gossip cluster.
                                 If empty, 'grpc-address' will be used.
      --http-address="0.0.0.0:10902"  
                                 Listen host:port for HTTP endpoints.
      --cluster.address="0.0.0.0:10900"  
                                 Listen ip:port address for gossip cluster.
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                                 Explicit (external) ip:port address to
                                 advertise for gossip in gossip cluster. Used
                                 internally for membership only
      --cluster.peers=CLUSTER.PEERS ...  
                                 Initial peers to join the cluster. It can be
                                 either <ip:port>, or <domain:port>. A lookup
                                 resolution is done only at the startup.
      --cluster.gossip-interval=5s  
                                 Interval between sending gossip messages. By
                                 lowering this value (more frequent) gossip
                                 messages are propagated across the cluster more
                                 quickly at the expense of increased bandwidth.
      --cluster.pushpull-interval=5s  
                                 Interval for gossip state syncs. Setting this
                                 interval lower (more frequent) will increase
                                 convergence speeds across larger clusters at
                                 the expense of increased bandwidth usage.
      --cluster.refresh-interval=1m0s  
                                 Interval for membership to refresh
                                 cluster.peers state, 0 disables refresh.
      --http-advertise-address=HTTP-ADVERTISE-ADDRESS  
                                 Explicit (external) host:port address to
                                 advertise for HTTP QueryAPI in gossip cluster.
                                 If empty, 'http-address' will be used.
      --query.timeout=2m         Maximum time to process query by query node.
      --query.max-concurrent=20  Maximum number of queries processed
                                 concurrently by query node.
      --query.replica-label=QUERY.REPLICA-LABEL  
                                 Label to treat as a replica indicator along
                                 which data is deduplicated. Still you will be
                                 able to query without deduplication using
                                 'dedup=false' parameter.
      --selector-label=<name>="<value>" ...  
                                 Query selector labels that will be exposed in
                                 info endpoint (repeated).
      --store=<store> ...        Addresses of statically configured store API
                                 servers (repeatable).
      --query.auto-downsampling  Enable automatic adjustment (step / 5) to what
                                 source of data should be used in store gateways
                                 if no max_source_resolution param is specified.

```
