# Store

The store component of Thanos implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space. It joins a Thanos cluster on startup and advertises the data it can access.
It keeps a small amount of information about all remote blocks on local disk and keeps it in sync with the bucket. This data is generally safe to delete across restarts at the cost of increased startup times.

```
$ thanos query \
    --tsdb.path        "/local/state/data/dir" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org"
```

In general about 1MB of local disk space is required per TSDB block stored in the object storage bucket.

## Deployment
## Flags

[embedmd]:# (flags/store.txt $)
```$
usage: thanos store --gcs.bucket=<bucket> [<flags>]

store node giving access to blocks in a GCS bucket

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
      --grpc-address="0.0.0.0:10901"  
                                listen address for gRPC endpoints
      --http-address="0.0.0.0:10902"  
                                listen address for HTTP endpoints
      --tsdb.path="./data"      data directory of TSDB
      --gcs.bucket=<bucket>     Google Cloud Storage bucket name for stored
                                blocks. If empty sidecar won't store any block
                                inside Google Cloud Storage
      --index-cache-size=250MB  Maximum size of items held in the index cache.
      --chunk-pool-size=2GB     Maximum size of concurrently allocatble bytes
                                for chunks.
      --cluster.peers=CLUSTER.PEERS ...  
                                initial peers to join the cluster. It can be
                                either <ip:port>, or <domain:port>
      --cluster.address="0.0.0.0:10900"  
                                listen address for clutser
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                                explicit address to advertise in cluster

```
