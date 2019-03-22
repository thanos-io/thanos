# Bucket

The bucket component of Thanos is a set of commands to inspect data in object storage buckets.
It is normally run as a stand alone command to aid with troubleshooting.

Example:

```
$ thanos bucket verify --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

Bucket can be extended to add more subcommands that will be helpful when working with object storage buckets
by adding a new command within `/cmd/thanos/bucket.go`


## Deployment
## Flags

[embedmd]:# (flags/bucket.txt $)
```$
usage: thanos bucket [<flags>] <command> [<args> ...]

Bucket utility commands

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT
                           GCP project to send Google Cloud Trace tracings to.
                           If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1
                           How often we send traces (1/<sample-factor>). If 0 no
                           trace will be sent periodically, unless forced by
                           baggage item. See `pkg/tracing/tracing.go` for
                           details.
      --objstore.config-file=<bucket.config-yaml-path>
                           Path to YAML file that contains object store
                           configuration.
      --objstore.config=<bucket.config-yaml>
                           Alternative to 'objstore.config-file' flag. Object
                           store configuration in YAML.

Subcommands:
  bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues

  bucket ls [<flags>]
    List all blocks in the bucket

  bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way


```

### Verify

`bucket verify` is used to verify and optionally repair blocks within the specified bucket.

Example:

```
$ thanos bucket verify --gcs.bucket example-bucket
```

[embedmd]:# (flags/bucket_verify.txt)
```txt
usage: thanos bucket verify [<flags>]

Verify all blocks in the bucket against specified issues

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT
                           GCP project to send Google Cloud Trace tracings to.
                           If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1
                           How often we send traces (1/<sample-factor>). If 0 no
                           trace will be sent periodically, unless forced by
                           baggage item. See `pkg/tracing/tracing.go` for
                           details.
      --objstore.config-file=<bucket.config-yaml-path>
                           Path to YAML file that contains object store
                           configuration.
      --objstore.config=<bucket.config-yaml>
                           Alternative to 'objstore.config-file' flag. Object
                           store configuration in YAML.
      --objstore-backup.config-file=<bucket.config-yaml-path>
                           Path to YAML file that contains object store-backup
                           configuration. Used for repair logic to backup blocks
                           before removal.
      --objstore-backup.config=<bucket.config-yaml>
                           Alternative to 'objstore-backup.config-file' flag.
                           Object store-backup configuration in YAML. Used for
                           repair logic to backup blocks before removal.
  -r, --repair             Attempt to repair blocks for which issues were
                           detected
  -i, --issues=index_issue... ...
                           Issues to verify (and optionally repair). Possible
                           values: [duplicated_compaction index_issue
                           overlapped_blocks]
      --id-whitelist=ID-WHITELIST ...
                           Block IDs to verify (and optionally repair) only. If
                           none is specified, all blocks will be verified.
                           Repeated field

```

### ls

`bucket ls` is used to list all blocks in the specified bucket.

Example:

```
$ thanos bucket ls -o json --gcs.bucket example-bucket
```

[embedmd]:# (flags/bucket_ls.txt)
```txt
usage: thanos bucket ls [<flags>]

List all blocks in the bucket

Flags:
  -h, --help               Show context-sensitive help (also try --help-long and
                           --help-man).
      --version            Show application version.
      --log.level=info     Log filtering level.
      --log.format=logfmt  Log format to use.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT
                           GCP project to send Google Cloud Trace tracings to.
                           If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1
                           How often we send traces (1/<sample-factor>). If 0 no
                           trace will be sent periodically, unless forced by
                           baggage item. See `pkg/tracing/tracing.go` for
                           details.
      --objstore.config-file=<bucket.config-yaml-path>
                           Path to YAML file that contains object store
                           configuration.
      --objstore.config=<bucket.config-yaml>
                           Alternative to 'objstore.config-file' flag. Object
                           store configuration in YAML.
  -o, --output=""          Optional format in which to print each block's
                           information. Options are 'json', 'wide' or a custom
                           template.

```

### inspect

`bucket inspect` is used to inspect buckets in a detailed way.

Example:
```
$ thanos bucket inspect -l environment=\"prod\"
```

[embedmd]:# (flags/bucket_inspect.txt)
```txt
usage: thanos bucket inspect [<flags>]

Inspect all blocks in the bucket in detailed, table-like way

Flags:
  -h, --help                 Show context-sensitive help (also try --help-long
                             and --help-man).
      --version              Show application version.
      --log.level=info       Log filtering level.
      --log.format=logfmt    Log format to use.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT
                             GCP project to send Google Cloud Trace tracings to.
                             If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1
                             How often we send traces (1/<sample-factor>). If 0
                             no trace will be sent periodically, unless forced
                             by baggage item. See `pkg/tracing/tracing.go` for
                             details.
      --objstore.config-file=<bucket.config-yaml-path>
                             Path to YAML file that contains object store
                             configuration.
      --objstore.config=<bucket.config-yaml>
                             Alternative to 'objstore.config-file' flag. Object
                             store configuration in YAML.
  -l, --selector=<name>="<value>" ...
                             Selects blocks based on label, e.g. '-l
                             key1="value1" -l key2="value2"'. All key value
                             pairs must match.
      --sort-by=FROM... ...  Sort by columns. It's also possible to sort by
                             multiple columns, e.g. '--sort-by FROM --sort-by
                             UNTIL'. I.e., if the 'FROM' value is equal the rows
                             are then further sorted by the 'UNTIL' value.

```
