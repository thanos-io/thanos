# Rule

The rule component evaluates Prometheus recording and alerting rules against random query nodes in its cluster. Rule results are written back to disk in the Prometheus 2.0 storage format. Rule nodes at the same time participate in the cluster themselves as source store nodes and upload their generated TSDB blocks to an object store.

The data of each rule node can be labeled to satisfy the clusters labeling scheme. High-availability pairs can be run in parallel and should be distinguished by the designated replica label, just like regular Prometheus servers.

```
$ thanos rule \
    --data-dir         "/path/to/data" \
    --eval-interval    "30s" \
    --rule-files       "/path/to/rules/*.rules.yaml" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org"
```

As rule nodes outsource query processing to query nodes, they should generally experience little load. If necessary, functional sharding can be applied by splitting up the sets of rules between HA pairs.
Rules are processed with deduplicated data according to the replica label configured on query nodes.