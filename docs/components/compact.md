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
usage: thanos compact [<flags>]

continuously compacts blocks in an object store bucket

Flags:
  -h, --help                   Show context-sensitive help (also try --help-long
                               and --help-man).
      --version                Show application version.
      --log.level=info         Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                               GCP project to send Google Cloud Trace tracings
                               to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                               How often we send traces (1/<sample-factor>). If
                               0 no trace will be sent periodically, unless
                               forced by baggage item. See
                               `pkg/tracing/tracing.go` for details.
      --http-address="0.0.0.0:10902"  
                               Listen host:port for HTTP endpoints.
      --data-dir="./data"      Data directory in which to cache blocks and
                               process compactions.
      --gcs.bucket=<bucket>    Google Cloud Storage bucket name for stored
                               blocks.
      --s3.bucket=<bucket>     S3-Compatible API bucket name for stored blocks.
      --s3.endpoint=<api-url>  S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>    Access key for an S3-Compatible API.
      --s3.insecure            Whether to use an insecure connection with an
                               S3-Compatible API.
      --s3.signature-version2  Whether to use S3 Signature Version 2; otherwise
                               Signature Version 4 will be used.
      --s3.encrypt-sse         Whether to use Server Side Encryption
      --sync-delay=30m         Minimum age of fresh (non-compacted) blocks
                               before they are being processed.
  -w, --wait                   Do not exit after all compactions have been
                               processed and wait for new work.

```
