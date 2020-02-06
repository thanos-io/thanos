---
title: Bucket
type: docs
menu: components
---

# Bucket

The bucket component of Thanos is a set of commands to inspect data in object storage buckets.
It is normally run as a stand alone command to aid with troubleshooting.

Example:

```bash
$ thanos bucket verify --objstore.config-file=bucket.yml
```

The content of `bucket.yml`:

```yaml
type: GCS
config:
  bucket: example-bucket
```

Bucket can be extended to add more subcommands that will be helpful when working with object storage buckets
by adding a new command within `/cmd/thanos/bucket.go`.


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
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/storage.md/#configuration

Subcommands:
  bucket verify [<flags>]
    Verify all blocks in the bucket against specified issues

  bucket ls [<flags>]
    List all blocks in the bucket

  bucket inspect [<flags>]
    Inspect all blocks in the bucket in detailed, table-like way

  bucket web [<flags>]
    Web interface for remote storage bucket


```

### Web

`bucket web` is used to inspect bucket blocks in form of interactive web UI.

This will start local webserver that will periodically update the view with given refresh.

<img src="../img/bucket-web.jpg" class="img-fluid" alt="web" />

Example:

```
$ thanos bucket web --objstore.config-file="..."
```

[embedmd]:# (flags/bucket_web.txt)
```txt
usage: thanos bucket web [<flags>]

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
                                https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                                Alternative to 'tracing.config-file' flag (lower
                                priority). Content of YAML file with tracing
                                configuration. See format details:
                                https://thanos.io/tracing.md/#configuration
      --objstore.config-file=<file-path>
                                Path to YAML file that contains object store
                                configuration. See format details:
                                https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                                Alternative to 'objstore.config-file' flag
                                (lower priority). Content of YAML file that
                                contains object store configuration. See format
                                details:
                                https://thanos.io/storage.md/#configuration
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

### Verify

`bucket verify` is used to verify and optionally repair blocks within the specified bucket.

Example:

```
$ thanos bucket verify --objstore.config-file="..."
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
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/storage.md/#configuration
      --objstore-backup.config-file=<file-path>
                           Path to YAML file that contains object store-backup
                           configuration. See format details:
                           https://thanos.io/storage.md/#configuration Used for
                           repair logic to backup blocks before removal.
      --objstore-backup.config=<content>
                           Alternative to 'objstore-backup.config-file' flag
                           (lower priority). Content of YAML file that contains
                           object store-backup configuration. See format
                           details: https://thanos.io/storage.md/#configuration
                           Used for repair logic to backup blocks before
                           removal.
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
$ thanos bucket ls -o json --objstore.config-file="..."
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
      --log.format=logfmt  Log format to use. Possible options: logfmt or json.
      --tracing.config-file=<file-path>
                           Path to YAML file with tracing configuration. See
                           format details:
                           https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                           Alternative to 'tracing.config-file' flag (lower
                           priority). Content of YAML file with tracing
                           configuration. See format details:
                           https://thanos.io/tracing.md/#configuration
      --objstore.config-file=<file-path>
                           Path to YAML file that contains object store
                           configuration. See format details:
                           https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                           Alternative to 'objstore.config-file' flag (lower
                           priority). Content of YAML file that contains object
                           store configuration. See format details:
                           https://thanos.io/storage.md/#configuration
  -o, --output=""          Optional format in which to print each block's
                           information. Options are 'json', 'wide' or a custom
                           template.

```

### inspect

`bucket inspect` is used to inspect buckets in a detailed way using stdout in ASCII table format.

Example:
```
$ thanos bucket inspect -l environment=\"prod\" --objstore.config-file="..."
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
      --log.format=logfmt    Log format to use. Possible options: logfmt or
                             json.
      --tracing.config-file=<file-path>
                             Path to YAML file with tracing configuration. See
                             format details:
                             https://thanos.io/tracing.md/#configuration
      --tracing.config=<content>
                             Alternative to 'tracing.config-file' flag (lower
                             priority). Content of YAML file with tracing
                             configuration. See format details:
                             https://thanos.io/tracing.md/#configuration
      --objstore.config-file=<file-path>
                             Path to YAML file that contains object store
                             configuration. See format details:
                             https://thanos.io/storage.md/#configuration
      --objstore.config=<content>
                             Alternative to 'objstore.config-file' flag (lower
                             priority). Content of YAML file that contains
                             object store configuration. See format details:
                             https://thanos.io/storage.md/#configuration
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
