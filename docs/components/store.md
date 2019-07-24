---
title: Store
type: docs
menu: components
---

# Store

The store component of Thanos implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space. It joins a Thanos cluster on startup and advertises the data it can access.
It keeps a small amount of information about all remote blocks on local disk and keeps it in sync with the bucket. This data is generally safe to delete across restarts at the cost of increased startup times.

```bash
$ thanos store \
    --data-dir        "/local/state/data/dir" \
    --objstore.config-file "bucket.yml"
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

In general about 1MB of local disk space is required per TSDB block stored in the object storage bucket.

## Flags

[embedmd]:# (flags/store.txt $)
```$
usage: thanos store [<flags>]

store node giving access to blocks in a bucket provider. Now supported GCS, S3,
Azure, Swift and Tencent COS.

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use.
      --tracing.config-file=<tracing.config-yaml-path>
                                 Path to YAML file that contains tracing
                                 configuration.
      --tracing.config=<tracing.config-yaml>
                                 Alternative to 'tracing.config-file' flag.
                                 Tracing configuration in YAML.
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --grpc-address="0.0.0.0:10901"
                                 Listen ip:port address for gRPC endpoints
                                 (StoreAPI). Make sure this address is routable
                                 from other components.
      --grpc-server-tls-cert=""  TLS Certificate for gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-key=""   TLS Key for the gRPC server, leave blank to
                                 disable TLS
      --grpc-server-tls-client-ca=""
                                 TLS CA to verify clients against. If no client
                                 CA is specified, there is no client
                                 verification on server side. (tls.NoClientCert)
      --data-dir="./data"        Data directory in which to cache remote blocks.
      --index-cache-size=250MB   Maximum size of items held in the index cache.
      --chunk-pool-size=2GB      Maximum size of concurrently allocatable bytes
                                 for chunks.
      --store.grpc.series-sample-limit=0
                                 Maximum amount of samples returned via a single
                                 Series call. 0 means no limit. NOTE: for
                                 efficiency we take 120 as the number of samples
                                 in chunk (it cannot be bigger than that), so
                                 the actual number of samples might be lower,
                                 even though the maximum could be hit.
      --store.grpc.series-max-concurrency=20
                                 Maximum number of concurrent Series calls.
      --objstore.config-file=<bucket.config-yaml-path>
                                 Path to YAML file that contains object store
                                 configuration.
      --objstore.config=<bucket.config-yaml>
                                 Alternative to 'objstore.config-file' flag.
                                 Object store configuration in YAML.
      --sync-block-duration=3m   Repeat interval for syncing the blocks between
                                 local and remote view.
      --block-sync-concurrency=20
                                 Number of goroutines to use when syncing blocks
                                 from object storage.

```
