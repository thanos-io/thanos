---
title: Compact
type: docs
menu: components
---

# Compact

The compactor component of Thanos applies the compaction procedure of the Prometheus 2.0 storage engine to block data stored in object storage.
It is generally not semantically concurrency safe and must be deployed as a singleton against a bucket.

It is also responsible for downsampling of data - performing 5m downsampling after **40 hours** and 1h downsampling after **10 days**.

Example:

```bash
$ thanos compact --data-dir /tmp/thanos-compact --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

The compactor needs local disk space to store intermediate data for its processing. Generally, about 100GB are recommended for it to keep working as the compacted time ranges grow over time.
On-disk data is safe to delete between restarts and should be the first attempt to get crash-looping compactors unstuck.

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
      --log.format=logfmt      Log format to use.
      --tracing.config-file=<tracing.config-yaml-path>
                               Path to YAML file that contains tracing
                               configuration. See fomrat details:
                               https://thanos.io/tracing.md/#configuration
      --tracing.config=<tracing.config-yaml>
                               Alternative to 'tracing.config-file' flag.
                               Tracing configuration in YAML. See format
                               details:
                               https://thanos.io/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                               Listen host:port for HTTP endpoints.
      --data-dir="./data"      Data directory in which to cache blocks and
                               process compactions.
      --objstore.config-file=<bucket.config-yaml-path>
                               Path to YAML file that contains object store
                               configuration. See format details:
                               https://thanos.io/storage.md/#configuration
      --objstore.config=<bucket.config-yaml>
                               Alternative to 'objstore.config-file' flag.
                               Object store configuration in YAML. See format
                               details:
                               https://thanos.io/storage.md/#configuration
      --consistency-delay=30m  Minimum age of fresh (non-compacted) blocks
                               before they are being processed. Malformed blocks
                               older than the maximum of consistency-delay and
                               30m0s will be removed.
      --retention.resolution-raw=0d
                               How long to retain raw samples in bucket. 0d -
                               disables this retention
      --retention.resolution-5m=0d
                               How long to retain samples of resolution 1 (5
                               minutes) in bucket. 0d - disables this retention
      --retention.resolution-1h=0d
                               How long to retain samples of resolution 2 (1
                               hour) in bucket. 0d - disables this retention
  -w, --wait                   Do not exit after all compactions have been
                               processed and wait for new work.
      --downsampling.disable   Disables downsampling. This is not recommended as
                               querying long time ranges without non-downsampled
                               data is not efficient and useful e.g it is not
                               possible to render all samples for a human eye
                               anyway
      --block-sync-concurrency=20
                               Number of goroutines to use when syncing block
                               metadata from object storage.
      --compact.concurrency=1  Number of goroutines to use when compacting
                               groups.

```
