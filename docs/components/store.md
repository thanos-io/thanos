# Store

The store component of Thanos implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space. It joins a Thanos cluster on startup and advertises the data it can access.
It keeps a small amount of information about all remote blocks on local disk and keeps it in sync with the bucket. This data is generally safe to delete across restarts at the cost of increased startup times.

```
$ thanos store \
    --data-dir         "/local/state/data/dir" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org"
```

In general about 1MB of local disk space is required per TSDB block stored in the object storage bucket.

## Deployment
## Flags

[embedmd]:# (flags/store.txt $)
```$
usage: thanos store [<flags>]

store node giving access to blocks in a supported bucket provider

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
                                for membership only.
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
                                convergence speeds across larger clusters at the
                                expense of increased bandwidth usage. Default is
                                used from a specified network-type.
      --cluster.refresh-interval=1m  
                                Interval for membership to refresh cluster.peers
                                state, 0 disables refresh.
      --cluster.secret-key=CLUSTER.SECRET-KEY  
                                Initial secret key to encrypt cluster gossip.
                                Can be one of AES-128, AES-192, or AES-256 in
                                hexadecimal format.
      --cluster.network-type=lan  
                                Network type with predefined peers
                                configurations. Sets of configurations
                                accounting the latency differences between
                                network types: local, lan, wan.
      --data-dir="./data"       Data directory in which to cache remote blocks.
      --provider.type=<provider>  
                                Specify the provider for object store. If empty
                                or unsupport provider, Thanos won't read and
                                store any block to the object store. Now
                                supported GCS / S3.
      --provider.bucket=<bucket>  
                                The bucket name for stored blocks.
      --provider.endpoint=<api-url>  
                                The object store API endpoint for stored blocks.
                                Support S3-Compatible API
      --provider.access-key=<key>  
                                Access key for an object store API. Support
                                S3-Compatible API
      --provider.insecure       Whether to use an insecure connection with an
                                object store API. Support S3-Compatible API
      --provider.signature-version2  
                                Whether to use S3 Signature Version 2; otherwise
                                Signature Version 4 will be used
      --provider.encrypt-sse    Whether to use Server Side Encryption
      --index-cache-size=250MB  Maximum size of items held in the index cache.
      --chunk-pool-size=2GB     Maximum size of concurrently allocatable bytes
                                for chunks.

```
