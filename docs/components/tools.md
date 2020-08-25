---
title: Tools
type: docs
menu: components
---

# Tools

The `thanos tools` subcommand of Thanos is a set of additional CLI, short-living tools that
are meant to be ran for development or debugging purposes.

All commands added as tools should land in `tools.go` or file with `tools_` prefix.

## Flags

[embedmd]:# (flags/tools.txt $)
```$
usage: thanos tools <command> [<args> ...]

Tools utility commands

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tip/tracing.md/#configuration

Subcommands:
  tools bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues

  tools bucket ls [<flags>]
    List all blocks in the bucket

  tools bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way

  tools bucket web [<flags>]
    Web interface for remote storage bucket

  tools bucket replicate [<flags>]
    Replicate data from one object storage to another. NOTE: Currently it works
    only with Thanos blocks (meta.json has to have Thanos metadata).

  tools bucket downsample [<flags>]
    continuously downsamples blocks in an object store bucket

  tools rules-check --rules=RULES
    Check if the rule files are valid or not.


```

## Bucket

The `thanos tools bucket` subcommand of Thanos is a set of commands to inspect data in object storage buckets.
It is normally run as a standalone command to aid with troubleshooting.

Example:

```bash
thanos tools bucket verify --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

Bucket can be extended to add more subcommands that will be helpful when working with object storage buckets
by adding a new command within [`/cmd/thanos/tools_bucket.go`](/cmd/thanos/tools_bucket.go)  .

[embedmd]:# (flags/tools_bucket.txt $)
```$
usage: thanos tools bucket [<flags>] <command> [<args> ...]

Bucket utility commands

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration

Subcommands:
  tools bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues

  tools bucket ls [<flags>]
    List all blocks in the bucket

  tools bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way

  tools bucket web [<flags>]
    Web interface for remote storage bucket

  tools bucket replicate [<flags>]
    Replicate data from one object storage to another. NOTE: Currently it works
    only with Thanos blocks (meta.json has to have Thanos metadata).

  tools bucket downsample [<flags>]
    continuously downsamples blocks in an object store bucket


```

### Bucket Web

`tools bucket web` is used to inspect bucket blocks in form of interactive web UI.

This will start local webserver that will periodically update the view with given refresh.

<img src="../img/bucket-web.jpg" class="img-fluid" alt="web" />

Example:

```
thanos tools bucket web --objstore.config-file="..."
```

[embedmd]:# (flags/tools_bucket_web.txt $)
```$
usage: thanos tools bucket web [<flags>]

Web interface for remote storage bucket

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
      --http-address="0.0.0.0:10902"
                                Listen host:port for HTTP endpoints.
      --http-grace-period=2m    Time to wait after an interrupt received for
                                HTTP Server.
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
      --refresh=30m             Refresh interval to download metadata from
                                remote storage
      --timeout=5m              Timeout to download metadata from remote storage
      --label=LABEL             Prometheus label to use as timeline title

```

### Bucket Verify

`tools bucket verify` is used to verify and optionally repair blocks within the specified bucket.

Example:

```
thanos tools bucket verify --objstore.config-file="..."
```

When using the `--repair` option, make sure that the compactor job is disabled first.

[embedmd]:# (flags/tools_bucket_verify.txt $)
```$
usage: thanos tools bucket verify [<flags>]

Verify all blocks in the bucket against specified issues

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore-backup.config-file=<file-path>
                           Path to YAML file that contains object store-backup
                           configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
                           Used for repair logic to backup blocks before
                           removal.
      --objstore-backup.config=<content>
                           Alternative to 'objstore-backup.config-file' flag
                           (lower priority). Content of YAML file that contains
                           object store-backup configuration. See format
                           details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
                           Used for repair logic to backup blocks before
                           removal.
  -r, --repair             Attempt to repair blocks for which issues were
                           detected
  -i, --issues=index_issue... ...
                           Issues to verify (and optionally repair). Possible
                           values: [duplicated_compaction index_issue
                           overlapped_blocks]
      --id=ID ...          Block IDs to verify (and optionally repair) only. If
                           none is specified, all blocks will be verified.
                           Repeated field
      --delete-delay=0s    Duration after which blocks marked for deletion would
                           be deleted permanently from source bucket by
                           compactor component. If delete-delay is non zero,
                           blocks will be marked for deletion and compactor
                           component is required to delete blocks from source
                           bucket. If delete-delay is 0, blocks will be deleted
                           straight away. Use this if you want to get rid of or
                           move the block immediately. Note that deleting blocks
                           immediately can cause query failures, if store
                           gateway still has the block loaded, or compactor is
                           ignoring the deletion because it's compacting the
                           block at the same time.

```

### Bucket ls

`tools bucket ls` is used to list all blocks in the specified bucket.

Example:

```
thanos tools bucket ls -o json --objstore.config-file="..."
```

[embedmd]:# (flags/tools_bucket_ls.txt $)
```$
usage: thanos tools bucket ls [<flags>]

List all blocks in the bucket

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/tip/thanos/storage.md/#configuration
  -o, --output=""          Optional format in which to print each block's
                           information. Options are 'json', 'wide' or a custom
                           template.

```

### Bucket inspect

`tools bucket inspect` is used to inspect buckets in a detailed way using stdout in ASCII table format.

Example:

```
thanos tools bucket inspect -l environment=\"prod\" --objstore.config-file="..."
```

[embedmd]:# (flags/tools_bucket_inspect.txt $)
```$
usage: thanos tools bucket inspect [<flags>]

Inspect all blocks in the bucket in detailed, table-like way

Flags:
  -h, --help                 Show context-sensitive help (also try --help-long
                             and --help-man).
      --version              Show application version.
      --log.level=info       Log filtering level.
      --log.format=logfmt    Log format to use. Possible options: logfmt or
                             json.
      --tracing.config-file=<file-path>
                             Path to YAML file with tracing configuration. See
                             format details:
                             https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                             Alternative to 'tracing.config-file' flag (lower
                             priority). Content of YAML file with tracing
                             configuration. See format details:
                             https://thanos.io/tip/tracing.md/#configuration
      --objstore.config-file=<file-path>
                             Path to YAML file that contains object store
                             configuration. See format details:
                             https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                             Alternative to 'objstore.config-file' flag (lower
                             priority). Content of YAML file that contains
                             object store configuration. See format details:
                             https://thanos.io/tip/thanos/storage.md/#configuration
  -l, --selector=<name>=\"<value>\" ...
                             Selects blocks based on label, e.g. '-l
                             key1=\"value1\" -l key2=\"value2\"'. All key value
                             pairs must match.
      --sort-by=FROM... ...  Sort by columns. It's also possible to sort by
                             multiple columns, e.g. '--sort-by FROM --sort-by
                             UNTIL'. I.e., if the 'FROM' value is equal the rows
                             are then further sorted by the 'UNTIL' value.
      --timeout=5m           Timeout to download metadata from remote storage

```

### Bucket replicate

`bucket tools replicate` is used to replicate buckets from one object storage to another.

NOTE: Currently it works only with Thanos blocks (meta.json has to have Thanos metadata).

Example:
```
thanos tools bucket replicate --objstore.config-file="..." --objstore-to.config="..."
```

[embedmd]:# (flags/tools_bucket_replicate.txt $)
```$
usage: thanos tools bucket replicate [<flags>]

Replicate data from one object storage to another. NOTE: Currently it works only
with Thanos blocks (meta.json has to have Thanos metadata).

Flags:
  -h, --help                     Show context-sensitive help (also try
                                 --help-long and --help-man).
      --version                  Show application version.
      --log.level=info           Log filtering level.
      --log.format=logfmt        Log format to use. Possible options: logfmt or
                                 json.
      --tracing.config-file=<file-path>
                                 Path to YAML file with tracing configuration.
                                 See format details:
                                 https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                                 Alternative to 'tracing.config-file' flag
                                 (lower priority). Content of YAML file with
                                 tracing configuration. See format details:
                                 https://thanos.io/tip/tracing.md/#configuration
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
      --http-address="0.0.0.0:10902"
                                 Listen host:port for HTTP endpoints.
      --http-grace-period=2m     Time to wait after an interrupt received for
                                 HTTP Server.
      --objstore-to.config-file=<file-path>
                                 Path to YAML file that contains object store-to
                                 configuration. See format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
                                 The object storage which replicate data to.
      --objstore-to.config=<content>
                                 Alternative to 'objstore-to.config-file' flag
                                 (lower priority). Content of YAML file that
                                 contains object store-to configuration. See
                                 format details:
                                 https://thanos.io/tip/thanos/storage.md/#configuration
                                 The object storage which replicate data to.
      --resolution=0s... ...     Only blocks with these resolutions will be
                                 replicated. Repeated flag.
      --compaction=1... ...      Only blocks with these compaction levels will
                                 be replicated. Repeated flag.
      --matcher=key="value" ...  Only blocks whose external labels exactly match
                                 this matcher will be replicated.
      --single-run               Run replication only one time, then exit.

```

### Bucket downsample

`tools bucket downsample` is used to continuously downsample blocks in an object store bucket as a service.
It implements the downsample API on top of historical data in an object storage bucket.

```bash
thanos tools bucket downsample \
    --data-dir        "/local/state/data/dir" \
    --objstore.config-file "bucket.yml"
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

[embedmd]:# (flags/tools_bucket_downsample.txt $)
```$
usage: thanos tools bucket downsample [<flags>]

continuously downsamples blocks in an object store bucket

Flags:
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --log.format=logfmt     Log format to use. Possible options: logfmt or
                              json.
      --tracing.config-file=<file-path>
                              Path to YAML file with tracing configuration. See
                              format details:
                              https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                              Alternative to 'tracing.config-file' flag (lower
                              priority). Content of YAML file with tracing
                              configuration. See format details:
                              https://thanos.io/tip/tracing.md/#configuration
      --objstore.config-file=<file-path>
                              Path to YAML file that contains object store
                              configuration. See format details:
                              https://thanos.io/tip/thanos/storage.md/#configuration
      --objstore.config=<content>
                              Alternative to 'objstore.config-file' flag (lower
                              priority). Content of YAML file that contains
                              object store configuration. See format details:
                              https://thanos.io/tip/thanos/storage.md/#configuration
      --http-address="0.0.0.0:10902"
                              Listen host:port for HTTP endpoints.
      --http-grace-period=2m  Time to wait after an interrupt received for HTTP
                              Server.
      --data-dir="./data"     Data directory in which to cache blocks and
                              process downsamplings.

```
## Rules-check

The `tools rules-check` subcommand contains tools for validation of Prometheus rules.

This is allowing to check the rules with the same validation as is used by the Thanos rule node.

NOTE: The check is equivalent to the `promtool check rules` with addition of Thanos rule extended rules file syntax,
which includes `partial_response_strategy` field which `promtool` does not allow.

If the check fails the command fails with exit code `1`, otherwise `0`.

Example:

```
./thanos tools rules-check --rules cmd/thanos/testdata/rules-files/*.yaml
```

[embedmd]:# (flags/tools_rules-check.txt $)
```$
usage: thanos tools rules-check --rules=RULES

Check if the rule files are valid or not.

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tip/tracing.md/#configuration
      --rules=RULES ...    The rule files glob to check (repeated).

```

#### Probes

- The downsample service exposes two endpoints for probing:
  - `/-/healthy` starts as soon as initial setup completed.
  - `/-/ready` starts after all the bootstrapping completed (e.g object store bucket connection) and ready to serve traffic.

> NOTE: Metric endpoint starts immediately so, make sure you set up readiness probe on designated HTTP `/-/ready` path.
