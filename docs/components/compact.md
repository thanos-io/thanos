# Compact

The compactor component of Thanos applies the compaction procedure of the Prometheus 2.0 storage engine to block data stored in object storage.
It is generally not semantically concurrency safe and must be deployed as a singleton against a bucket.

Example:

```
$ thanos compact --gcs.bucket example-bucket --data-dir /tmp/thanos-compact
```

The compactor needs local disk space to store intermediate data for its processing. Generally, about 100GB are recommended for it to keep working as the compacted time ranges grow over time.
On-disk data is safe to delete between restarts and should be the first attempt to get crash-looping compactors unstuck.

## Deployment

## Flags

[embedmd]:# (flags/compact.txt $)
```$
usage: thanos compact --gcs.bucket=<bucket> [<flags>]

continously compacts blocks in an object store bucket

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
      --http-address="0.0.0.0:10902"  
                             listen host:port for HTTP endpoints
      --data-dir="./data"    data directory to cache blocks and process
                             compactions
      --gcs.bucket=<bucket>  Google Cloud Storage bucket name for stored blocks.
      --sync-delay=2h        minimum age of blocks before they are being
                             processed.

```
