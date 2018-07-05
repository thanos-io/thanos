# Sidecar

The sidecar component of Thanos gets deployed along with a Prometheus instance. It implements Thanos' Store API on top of Prometheus' remote-read API and advertises itself as a data source to the cluster. Thereby queriers in the cluster can treat Prometheus servers as yet another source of time series data without directly talking to its APIs.
Additionally, the sidecar uploads TSDB blocks to an object storage bucket as Prometheus produces them. This allows Prometheus servers to be run with relatively low retention while their historic data is made durable and queryable via object storage.

Prometheus servers connected to the Thanos cluster via the sidecar are subject to a few limitations for safe operations:

* The minimum Prometheus version is 2.0
* The `external_labels` section of the configuration implements is in line with the cluster's [labeling scheme](/docs-for-labeling-schemas)
* The `--storage.tsdb.min-block-duration` and `--storage.tsdb.max-block-duration` must be set to equal values. The default of `2h` is recommended.

The retention is recommended to not be lower than three times the block duration. This achieves resilience in the face of connectivity issues to the object storage since all local data will remain available within the Thanos cluster. If connectivity gets restored the backlog of blocks gets uploaded to the object storage.

```
$ thanos sidecar \
    --tsdb.path        "/path/to/prometheus/data/dir" \
    --prometheus.url   "http://localhost:9090" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org" \
```

## Deployment

## Flags

[embedmd]:# (flags/sidecar.txt $)
```$
usage: thanos sidecar [<flags>]

sidecar for Prometheus server

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
      --prometheus.url=http://localhost:9090  
                                 URL at which to reach Prometheus's API.
      --tsdb.path="./data"       Data directory of TSDB.
      --gcs.bucket=<bucket>      Google Cloud Storage bucket name for stored
                                 blocks. If empty, sidecar won't store any block
                                 inside Google Cloud Storage.
      --s3.bucket=<bucket>       S3-Compatible API bucket name for stored
                                 blocks.
      --s3.endpoint=<api-url>    S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>      Access key for an S3-Compatible API.
      --s3.insecure              Whether to use an insecure connection with an
                                 S3-Compatible API.
      --s3.signature-version2    Whether to use S3 Signature Version 2;
                                 otherwise Signature Version 4 will be used.
      --s3.encrypt-sse           Whether to use Server Side Encryption
      --reloader.config-file=""  Config file watched by the reloader.
      --reloader.config-envsubst-file=""  
                                 Output file for environment variable
                                 substituted config file.
      --reloader.rule-dir=RELOADER.RULE-DIR  
                                 Rule directory for the reloader to refresh.

```


## Reloader Configuration

Thanos can watch changes in Prometheus configuration and refresh Prometheus configuration if `--web.enable-lifecycle` enabled.

You can configure watching for changes in directory via `--reloader.rule-dir=DIR_NAME` flag.

Thanos sidecar can watch `--reloader.config-file=CONFIG_FILE` configuration file, evalute environment variables found in there and produce generated config in `--reloader.config-envsubst-file=OUT_CONFIG_FILE` file.
