# Rule

_**NOTE:** The rule component is experimental since it has conceptual tradeoffs that might not be faveorable for most use cases. It is recommended to keep deploying rules to the relevant Prometheus servers._

_The rule component should in particular not be used to circumvent solving rule deployment properly at the configuration management level._

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

## Deployment

## Flags

[embedmd]:# (flags/rule.txt $)
```$
usage: thanos rule [<flags>]

query node exposing PromQL enabled Query API with data retrieved from multiple
store nodes

Flags:
  -h, --help                    Show context-sensitive help (also try
                                --help-long and --help-man).
      --version                 Show application version.
      --log.level=info          log filtering level
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                                GCP project to send Google Cloud Trace tracings
                                to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                                How often we send traces (1/<sample-factor>).
      --label=<name>="<value>" ...  
                                labels applying to all generated metrics
                                (repeated)
      --data-dir="data/"        data directory
      --rule-file=rules/ ...    rule files that should be used by rule manager.
                                Can be in glob format (repeated)
      --http-address="0.0.0.0:10902"  
                                listen host:port for HTTP endpoints
      --grpc-address="0.0.0.0:10901"  
                                listen host:port for gRPC endpoints
      --eval-interval=30s       the default evaluation interval to use
      --tsdb.block-duration=2h  block duration for TSDB block
      --tsdb.retention=48h      block retention time on local disk
      --alertmanagers.url=ALERTMANAGERS.URL ...  
                                Alertmanager URLs to push firing alerts to. The
                                scheme may be prefixed with 'dns+' or 'dnssrv+'
                                to detect Alertmanager IPs through respective
                                DNS lookups. The port defaults to 9093 or the
                                SRV record's value. The URL path is used as a
                                prefix for the regular Alertmanager API path.
      --gcs.bucket=<bucket>     Google Cloud Storage bucket name for stored
                                blocks. If empty ruler won't store any block
                                inside Google Cloud Storage
      --cluster.peers=CLUSTER.PEERS ...  
                                initial peers to join the cluster. It can be
                                either <ip:port>, or <domain:port>
      --cluster.address="0.0.0.0:10900"  
                                listen address for cluster
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                                explicit address to advertise in cluster

```
