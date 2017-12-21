# Sidecar

The sidecar component of Thanos gets deployed along with a Prometheus instance. It implements Thanos' Store API on top of Prometheus' remote-read API and advertises itself as a data source to the cluster. Thereby queriers in the cluster can treat Prometheus servers as yet another source of time series data without directly talking to its APIs.
Additionally, the sidecar uploads TSDB blocks to an object storage bucket as Prometheus produces them. This allows Prometheus servers to be run with relatively low retention while their historic data is made durable and queryable via object storage.

Prometheus servers connected to the Thanos cluster via the sidecar are subject to a few limitations for safe operations:

* The minimum Prometheus version is 2.0
* The `external_labels` section of the configuration implements is in line with the cluster's [labeling scheme](/docs-for-labeling-schemas)
* The `--storage.tsdb.min-block-duration` and `--storage.tsdb.max-block-duration` must be set to equal values. The default of `2h` is recommended.

The retention is recommended to not be lower than three times the block duration. This achieves resilience in the face of connectivity issues to the object storage since all local data will remain available within the Thanos cluster. If connectivity gets restored the backlog of blocks gets uploaded to the object storage.

```
$ thanos query \
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
  -h, --help                 Show context-sensitive help (also try --help-long
                             and --help-man).
      --version              Show application version.
      --log.level=info       log filtering level
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                             GCP project to send Google Cloud Trace tracings to.
                             If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                             How often we send traces (1/<sample-factor>).
      --grpc-address="0.0.0.0:10901"  
                             listen address for gRPC endpoints
      --http-address="0.0.0.0:10902"  
                             listen address for HTTP endpoints
      --prometheus.url=http://localhost:9090  
                             URL at which to reach Prometheus's API
      --tsdb.path="./data"   data directory of TSDB
      --gcs.bucket=<bucket>  Google Cloud Storage bucket name for stored blocks.
                             If empty sidecar won't store any block inside
                             Google Cloud Storage
      --cluster.peers=CLUSTER.PEERS ...  
                             initial peers to join the cluster. It can be either
                             <ip:port>, or <domain:port>
      --cluster.address="0.0.0.0:10900"  
                             listen address for cluster
      --cluster.advertise-address=CLUSTER.ADVERTISE-ADDRESS  
                             explicit address to advertise in cluster

```
