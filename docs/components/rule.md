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

ruler evaluating Prometheus rules against given Query nodes, exposing Store API
and storing old blocks in bucket

Flags:
  -h, --help                    Show context-sensitive help (also try
                                --help-long and --help-man).
      --version                 Show application version.
      --log.level=info          Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                                GCP project to send Google Cloud Trace tracings
                                to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                                How often we send traces (1/<sample-factor>). If
                                0 no trace will be sent periodically, unless
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
                                Explicit (external) ip:port address to advertise
                                for gossip in gossip cluster. Used internally
                                for membership only
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
                                convergence speeds across larger clusters at the
                                expense of increased bandwidth usage.
      --cluster.refresh-interval=1m0s  
                                Interval for membership to refresh cluster.peers
                                state, 0 disables refresh.
      --label=<name>="<value>" ...  
                                Labels to be applied to all generated metrics
                                (repeated).
      --data-dir="data/"        data directory
      --rule-file=rules/ ...    Rule files that should be used by rule manager.
                                Can be in glob format (repeated).
      --eval-interval=30s       The default evaluation interval to use.
      --tsdb.block-duration=2h  Block duration for TSDB block.
      --tsdb.retention=48h      Block retention time on local disk.
      --alertmanagers.url=ALERTMANAGERS.URL ...  
                                Alertmanager URLs to push firing alerts to. The
                                scheme may be prefixed with 'dns+' or 'dnssrv+'
                                to detect Alertmanager IPs through respective
                                DNS lookups. The port defaults to 9093 or the
                                SRV record's value. The URL path is used as a
                                prefix for the regular Alertmanager API path.
      --gcs.bucket=<bucket>     Google Cloud Storage bucket name for stored
                                blocks. If empty, ruler won't store any block
                                inside Google Cloud Storage.
      --alert.query-url=ALERT.QUERY-URL  
                                The external Thanos Query URL that would be set
                                in all alerts 'Source' field
      --s3.bucket=<bucket>      S3-Compatible API bucket name for stored blocks.
      --s3.endpoint=<api-url>   S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>     Access key for an S3-Compatible API.
      --s3.insecure             Whether to use an insecure connection with an
                                S3-Compatible API.
      --s3.signature-version2   Whether to use S3 Signature Version 2; otherwise
                                Signature Version 4 will be used.
      --s3.encrypt-sse          Whether to use Server Side Encryption

```
