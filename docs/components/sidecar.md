# Sidecar

The sidecar component of Thanos gets deployed along with a Prometheus instance. It implements Thanos' Store API on top of Prometheus' remote-read API and advertises itself as a data source to the cluster. Thereby queriers in the cluster can treat Prometheus servers as yet another source of time series data without directly talking to its APIs.
Additionally, the sidecar uploads TSDB blocks to an object storage bucket as Prometheus produces them. This allows Prometheus servers to be run with relatively low retention while their historic data is made durable and queryable via object storage.

Prometheus servers connected to the Thanos cluster via the sidecar are subject to a few limitations for safe operations:

* The minimum Prometheus version is 2.2.1
* The `external_labels` section of the configuration implements is in line with the desired label scheme (will be used by query layer to filter out store APIs to query).
* The `--web.enable-lifecycle` flag is enabled if you want to use `reload.*` flags.
* The `--storage.tsdb.min-block-duration` and `--storage.tsdb.max-block-duration` must be set to equal values to disable local compaction. The default of `2h` is recommended.

The retention is recommended to not be lower than three times the block duration. This achieves resilience in the face of connectivity issues 
to the object storage since all local data will remain available within the Thanos cluster. If connectivity gets restored the backlog of blocks gets uploaded to the object storage.

```
$ thanos sidecar \
    --tsdb.path        "/path/to/prometheus/data/dir" \
    --prometheus.url   "http://localhost:9090" \
    --objstore.config-file  "bucket.yml"
```

The example content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
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
      --log.format=logfmt        Log format to use.
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
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""  
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --http-address="0.0.0.0:10902"  
                                 Listen host:port for HTTP endpoints.
      --cluster.address="0.0.0.0:10900"  
                                 Listen ip:port address for gossip cluster.
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                                 Explicit (external) ip:port address to
                                 advertise for gossip in gossip cluster. Used
                                 internally for membership only.
      --cluster.peers=CLUSTER.PEERS ...  
                                 Initial peers to join the cluster. It can be
                                 either <ip:port>, or <domain:port>. A lookup
                                 resolution is done only at the startup.
      --cluster.gossip-interval=<gossip interval>  
                                 Interval between sending gossip messages. By
                                 lowering this value (more frequent) gossip
                                 messages are propagated across the cluster more
                                 quickly at the expense of increased bandwidth.
                                 Default is used from a specified network-type.
      --cluster.pushpull-interval=<push-pull interval>  
                                 Interval for gossip state syncs. Setting this
                                 interval lower (more frequent) will increase
                                 convergence speeds across larger clusters at
                                 the expense of increased bandwidth usage.
                                 Default is used from a specified network-type.
      --cluster.refresh-interval=1m  
                                 Interval for membership to refresh
                                 cluster.peers state, 0 disables refresh.
      --cluster.secret-key=CLUSTER.SECRET-KEY  
                                 Initial secret key to encrypt cluster gossip.
                                 Can be one of AES-128, AES-192, or AES-256 in
                                 hexadecimal format.
      --cluster.network-type=lan  
                                 Network type with predefined peers
                                 configurations. Sets of configurations
                                 accounting the latency differences between
                                 network types: local, lan, wan.
      --cluster.disable          If true gossip will be disabled and no cluster
                                 related server will be started.
      --prometheus.url=http://localhost:9090  
                                 URL at which to reach Prometheus's API. For
                                 better performance use local network.
      --tsdb.path="./data"       Data directory of TSDB.
      --reloader.config-file=""  Config file watched by the reloader.
      --reloader.config-envsubst-file=""  
                                 Output file for environment variable
                                 substituted config file.
      --reloader.rule-dir=RELOADER.RULE-DIR ...  
                                 Rule directories for the reloader to refresh
                                 (repeated field).
      --objstore.config-file=<bucket.config-yaml-path>  
                                 Path to YAML file that contains object store
                                 configuration.
      --objstore.config=<bucket.config-yaml>  
                                 Alternative to 'objstore.config-file' flag.
                                 Object store configuration in YAML.

```


## Reloader Configuration

Thanos can watch changes in Prometheus configuration and refresh Prometheus configuration if `--web.enable-lifecycle` enabled.

You can configure watching for changes in directory via `--reloader.rule-dir=DIR_NAME` flag.

Thanos sidecar can watch `--reloader.config-file=CONFIG_FILE` configuration file, evaluate environment variables found in there and produce generated config in `--reloader.config-envsubst-file=OUT_CONFIG_FILE` file.
