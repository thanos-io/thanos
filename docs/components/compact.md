---
title: Compactor
type: docs
menu: components
---

# Compactor

The `thanos compact` command applies the compaction procedure of the Prometheus 2.0 storage engine to block data stored in object storage.
It is generally not semantically concurrency safe and must be deployed as a singleton against a bucket.

It is also responsible for downsampling of data:

- creating 5m downsampling for blocks larger than **40 hours** (2d, 2w)
- creating 1h downsampling for blocks larger than **10 days** (2w).

Example:

```bash
thanos compact --data-dir /tmp/thanos-compact --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

The compactor needs local disk space to store intermediate data for its processing. Generally, about 100GB are recommended for it to keep working as the compacted time ranges grow over time.
On-disk data is safe to delete between restarts and should be the first attempt to get crash-looping compactors unstuck.

## Downsampling, Resolution and Retention

Resolution - distance between data points on your graphs. E.g.

- raw - the same as scrape interval at the moment of data ingestion
- 5m - data point is every 5 minutes
- 1h - data point is every 1h

Keep in mind, that the initial goal of downsampling is not saving disk space (Read further for elaboration on storage space consumption). The goal of downsampling is providing an opportunity to get fast results for range queries of big time intervals like months or years. In other words, if you set `--retention.resolution-raw` less then `--retention.resolution-5m` and `--retention.resolution-1h` - you might run into a problem of not being able to "zoom in" to your historical data.

To avoid confusion - you might want to think about `raw` data as about "zoom in" opportunity. Considering the values for mentioned options - always think "Will I need to zoom in to the day 1 year ago?" if the answer "yes" - you most likely want to keep raw data for as long as 1h and 5m resolution, otherwise you'll be able to see only downsampled representation of how your raw data looked like.

There's also a case when you might want to disable downsampling at all with `debug.disable-downsampling`. You might want to do it when you know for sure that you are not going to request long ranges of data (obviously, because without downsampling those requests are going to be much much more expensive than with it). A valid example of that case if when you only care about the last couple of weeks of your data or use it only for alerting, but if it's your case - you also need to ask yourself if you want to introduce Thanos at all instead of vanilla Prometheus?

Ideally, you will have equal retention set (or no retention at all) to all resolutions which allow both "zoom in" capabilities as well as performant long ranges queries. Since object storages are usually quite cheap, storage size might not matter that much, unless your goal with thanos is somewhat very specific and you know exactly what you're doing.

Not setting this flag, or setting it to `0d`, i.e. `--retention.resolution-X=0d`, will mean that samples at the `X` resolution level will be kept forever.

Please note that blocks are only deleted after they completely "fall off" of the specified retention policy. In other words, the "max time" of a block needs to be older than the amount of time you had specified.

## Storage space consumption

In fact, downsampling doesn't save you any space but instead it adds 2 more blocks for each raw block which are only slightly smaller or relatively similar size to raw block. This is required by internal downsampling implementation which to be mathematically correct holds various aggregations. This means that downsampling can increase the size of your storage a bit (~3x), but it gives massive advantage on querying long ranges.

## Groups

The compactor groups blocks using the external_labels added by the Prometheus who produced the block.
The labels must be both _unique_ and _persistent_ across different Prometheus instances.

By _unique_, we mean that the set of labels in a Prometheus instance must be different from all other sets of labels of
your Prometheus instances, so that the compactor will be able to group blocks by Prometheus instance.

By _persistent_, we mean that one Prometheus instance must keep the same labels if it restarts, so that the compactor will keep
compacting blocks from an instance even when a Prometheus instance goes down for some time.

## Block Deletion

Depending on the Object Storage provider like S3, GCS, Ceph etc; we can divide the storages into strongly consistent or eventually consistent.
Since there are no consistency guarantees provided by some Object Storage providers, we have to make sure that we have a consistent lock-free way of dealing with Object Storage irrespective of the choice of object storage.

In order to achieve this co-ordination, blocks are not deleted directly. Instead, blocks are marked for deletion by uploading
`deletion-mark.json` file for the block that was chosen to be deleted. This file contains unix time of when the block was marked for deletion.

## Flags

[embedmd]:# (flags/compact.txt $)
```$
usage: thanos compact [<flags>]

continuously compacts blocks in an object store bucket

Flags:
  -h, --help                    Show context-sensitive help (also try
                                --help-long and --help-man).
      --version                 Show application version.
      --log.level=info          Log filtering level.
      --log.format=logfmt       Log format to use. Possible options: logfmt or
                                json.
      --tracing.config-file=<file-path>
                                Path to YAML file with tracing configuration.
                                See format details:
                                https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                                Alternative to 'tracing.config-file' flag (lower
                                priority). Content of YAML file with tracing
                                configuration. See format details:
                                https://thanos.io/tip/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                                Listen host:port for HTTP endpoints.
      --http-grace-period=2m    Time to wait after an interrupt received for
                                HTTP Server.
      --data-dir="./data"       Data directory in which to cache blocks and
                                process compactions.
      --objstore.config-file=<file-path>
                                Path to YAML file that contains object store
                                configuration. See format details:
                                https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                                Alternative to 'objstore.config-file' flag
                                (lower priority). Content of YAML file that
                                contains object store configuration. See format
                                details:
                                https://thanos.io/tip/thanos/storage.md/#configuration
      --consistency-delay=30m   Minimum age of fresh (non-compacted) blocks
                                before they are being processed. Malformed
                                blocks older than the maximum of
                                consistency-delay and 48h0m0s will be removed.
      --retention.resolution-raw=0d
                                How long to retain raw samples in bucket.
                                Setting this to 0d will retain samples of this
                                resolution forever
      --retention.resolution-5m=0d
                                How long to retain samples of resolution 1 (5
                                minutes) in bucket. Setting this to 0d will
                                retain samples of this resolution forever
      --retention.resolution-1h=0d
                                How long to retain samples of resolution 2 (1
                                hour) in bucket. Setting this to 0d will retain
                                samples of this resolution forever
  -w, --wait                    Do not exit after all compactions have been
                                processed and wait for new work.
      --wait-interval=5m        Wait interval between consecutive compaction
                                runs and bucket refreshes. Only works when
                                --wait flag specified.
      --downsampling.disable    Disables downsampling. This is not recommended
                                as querying long time ranges without
                                non-downsampled data is not efficient and useful
                                e.g it is not possible to render all samples for
                                a human eye anyway
      --block-sync-concurrency=20
                                Number of goroutines to use when syncing block
                                metadata from object storage.
      --block-viewer.global.sync-block-interval=1m
                                Repeat interval for syncing the blocks between
                                local and remote view for /global Block Viewer
                                UI.
      --compact.concurrency=1   Number of goroutines to use when compacting
                                groups.
      --delete-delay=48h        Time before a block marked for deletion is
                                deleted from bucket. If delete-delay is non
                                zero, blocks will be marked for deletion and
                                compactor component will delete blocks marked
                                for deletion from the bucket. If delete-delay is
                                0, blocks will be deleted straight away. Note
                                that deleting blocks immediately can cause query
                                failures, if store gateway still has the block
                                loaded, or compactor is ignoring the deletion
                                because it's compacting the block at the same
                                time.
      --selector.relabel-config-file=<file-path>
                                Path to YAML file that contains relabeling
                                configuration that allows selecting blocks. It
                                follows native Prometheus relabel-config syntax.
                                See format details:
                                https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config
      --selector.relabel-config=<content>
                                Alternative to 'selector.relabel-config-file'
                                flag (lower priority). Content of YAML file that
                                contains relabeling configuration that allows
                                selecting blocks. It follows native Prometheus
                                relabel-config syntax. See format details:
                                https://prometheus.io/docs/prometheus/latest/configuration/configuration/#relabel_config
      --web.external-prefix=""  Static prefix for all HTML links and redirect
                                URLs in the bucket web UI interface. Actual
                                endpoints are still served on / or the
                                web.route-prefix. This allows thanos bucket web
                                UI to be served behind a reverse proxy that
                                strips a URL sub-path.
      --web.prefix-header=""    Name of HTTP request header used for dynamic
                                prefixing of UI links and redirects. This option
                                is ignored if web.external-prefix argument is
                                set. Security risk: enable this option only if a
                                reverse proxy in front of thanos is resetting
                                the header. The
                                --web.prefix-header=X-Forwarded-Prefix option
                                can be useful, for example, if Thanos UI is
                                served via Traefik reverse proxy with
                                PathPrefixStrip option enabled, which sends the
                                stripped prefix value in X-Forwarded-Prefix
                                header. This allows thanos UI to be served on a
                                sub-path.
      --bucket-web-label=BUCKET-WEB-LABEL
                                Prometheus label to use as timeline title in the
                                bucket web UI

```
