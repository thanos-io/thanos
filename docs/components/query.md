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

