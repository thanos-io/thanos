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
      --log.level=info           log filtering level
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                                 GCP project to send Google Cloud Trace tracings
                                 to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                                 How often we send traces (1/<sample-factor>).
      --http-address="0.0.0.0:10902"  
                                 listen host:port for HTTP endpoints
      --query.timeout=2m         maximum time to process query by query node
      --query.max-concurrent=20  maximum number of queries processed
                                 concurrently by query node
      --query.replica-label=QUERY.REPLICA-LABEL  
                                 label to treat as a replica indicator along
                                 which data is deduplicated. Still you will be
                                 able to query without deduplication using
                                 'dedup=false' parameter
      --cluster.peers=CLUSTER.PEERS ...  
                                 initial peers to join the cluster. It can be
                                 either <ip:port>, or <domain:port>
      --cluster.address="0.0.0.0:10900"  
                                 listen address for cluster
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                                 explicit address to advertise in cluster

```
