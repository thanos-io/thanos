---
title: Downsample
type: docs
menu: components
---

# Downsample

The downsample component of Thanos implements the downsample API on top of historical data in an object storage bucket. It continuously downsamples blocks in an object store bucket as a service.

```bash
$ thanos downsample \
    --data-dir        "/local/state/data/dir" \
    --objstore.config-file "bucket.yml"
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

## Flags

[embedmd]:# (flags/downsample.txt $)
```$
usage: thanos downsample [<flags>]

continuously downsamples blocks in an object store bucket

Flags:
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --log.format=logfmt     Log format to use.
      --tracing.config-file=<file-path>
                              Path to YAML file with tracing configuration. See
                              format details:
                              https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                              Alternative to 'tracing.config-file' flag (lower
                              priority). Content of YAML file with tracing
                              configuration. See format details:
                              https://thanos.io/tracing.md/#configuration
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --data-dir="./data"     Data directory in which to cache blocks and
                              process downsamplings.
      --objstore.config-file=<file-path>
                              Path to YAML file that contains object store
                              configuration. See format details:
                              https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                              Alternative to 'objstore.config-file' flag (lower
                              priority). Content of YAML file that contains
                              object store configuration. See format details:
                              https://thanos.io/storage.md/#configuration

```

## Probes

- Thanos downsample exposes two endpoints for probing.
  - `/-/healthy` starts as soon as initial setup completed.
  - `/-/ready` starts after all the bootstrapping completed (e.g object store bucket connection) and ready to serve traffic.

> NOTE: Metric endpoint starts immediately so, make sure you set up readiness probe on designated HTTP `/-/ready` path.
