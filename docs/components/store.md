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
      --min-block-start-time=0000-01-01T00:00:00Z
                                 Start of time range limit to serve. Thanos
                                 Store serves only blocks, which have start time
                                 greater than this value. Option can be a
                                 constant time in RFC3339 format or time
                                 duration relative to current time, such as
                                 -1.5d or 2h45m. Valid duration units are ms, s,
                                 m, h, d, w, y.
      --max-block-start-time=9999-12-31T23:59:59Z
                                 End of time range limit to serve. Thanos Store
                                 serves only blocks, which have start time is
                                 less than this value. Option can be a constant
                                 time in RFC3339 format or time duration
                                 relative to current time, such as -1.5d or
                                 2h45m. Valid duration units are ms, s, m, h, d,
                                 w, y.
      --min-block-end-time=0000-01-01T00:00:00Z
                                 Start of time range limit to serve. Thanos
                                 Store serves only blocks, which have end time
                                 greater than this value. Option can be a
                                 constant time in RFC3339 format or time
                                 duration relative to current time, such as
                                 -1.5d or 2h45m. Valid duration units are ms, s,
                                 m, h, d, w, y.
      --max-block-end-time=9999-12-31T23:59:59Z
                                 End of time range limit to serve. Thanos Store
                                 serves only blocks, which have end time is less
                                 than this value. Option can be a constant time
                                 in RFC3339 format or time duration relative to
                                 current time, such as -1.5d or 2h45m. Valid
                                 duration units are ms, s, m, h, d, w, y.

```

## Time & Duration based partioning

By default Thanos Store Gateway looks at all the data in Object Store and returns it based on query's time range.

There is a block syncing job, which synchronizes local state with remote storage. You can configure how often it runs via `--sync-block-duration=3m`. In most cases default should work well.


Recently Thanos Store introduced `--min-block-start-time`, `--max-block-start-time`, `--min-block-end-time`,`--max-block-end-time` flags, that allows you to shard Thanos Store based on constant time or duration relative to current time.
The `{min,max}-block-start-time` options only look at block's start time and `{min,max}-block-end-time` only at the end time.

For example setting: `--min-block-start-time=-6w` & `--max-block-start-time==-2w` will make Thanos Store Gateway look at blocks that fall within `now - 6 weeks` up to `now - 2 weeks` time range.

You can also set constant time in RFC3339 format. For example `--min-block-start-time=2018-01-01T00:00:00Z`, `--max-block-start-time=2019-01-01T23:59:59Z`.

The block filtering is done in Thanos Store's syncing job, which adds some delay, so Thanos Store might not see new blocks or filter out blocks immediately.

We recommend having overlapping time ranges with Thanos Sidecar and other Thanos Store gateways as this improves your resiliency to failures.
A lot of Object Store implementations provide eventual read-after-write consistency, which means that Thanos Store won't immediately see newly created & uploaded blocks.
Also Thanos Sidecar might fail to upload new blocks to Object Store due to network timeouts or Object Store downtime, so if your time ranges are too strict, you won't be able to query the data.

Thanos Querier deals with overlapping time series by merging them together. It's important to note, that by having more overlapping time series Thanos Querier will use more resources, like CPU, network & memory, as it will have to pull down all the overlapping blocks and merge them.

When configuring time partitioning keep in mind Thanos Compaction, as it builds bigger blocks and removes data based on your retention policies.
