# Bucket

The bucket component of Thanos is a set of commands to inspect data in object storage buckets.
It is normally run as a stand alone command to aid with troubleshooting. 

Example:

```
$ thanos bucket verify --gcs.bucket example-bucket
```

Bucket can be extended to add more subcommands that will be helpful when working with object storage buckets
by adding a new command within `/cmd/thanos/bucket.go`


## Deployment
## Flags

[embedmd]:# (flags/bucket.txt $)
```$
usage: thanos bucket [<flags>] <command> [<args> ...]

inspect metric data in an object storage bucket

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
      --gcs-bucket=<bucket>    Google Cloud Storage bucket name for stored
                               blocks.
      --s3.bucket=<bucket>     S3-Compatible API bucket name for stored blocks.
      --s3.endpoint=<api-url>  S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>    Access key for an S3-Compatible API.
      --s3.insecure            Whether to use an insecure connection with an
                               S3-Compatible API.
      --s3.signature-version2  Whether to use S3 Signature Version 2; otherwise
                               Signature Version 4 will be used.
      --s3.encrypt-sse         Whether to use Server Side Encryption
      --gcs-backup-bucket=<bucket>  
                               Google Cloud Storage bucket name to backup blocks
                               on repair operations.
      --s3-backup-bucket=<bucket>  
                               S3 bucket name to backup blocks on repair
                               operations.

Subcommands:
  bucket verify [<flags>]
    verify all blocks in the bucket against specified issues

  bucket ls [<flags>]
    list all blocks in the bucket


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

verify all blocks in the bucket against specified issues

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
      --gcs-bucket=<bucket>    Google Cloud Storage bucket name for stored
                               blocks.
      --s3.bucket=<bucket>     S3-Compatible API bucket name for stored blocks.
      --s3.endpoint=<api-url>  S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>    Access key for an S3-Compatible API.
      --s3.insecure            Whether to use an insecure connection with an
                               S3-Compatible API.
      --s3.signature-version2  Whether to use S3 Signature Version 2; otherwise
                               Signature Version 4 will be used.
      --s3.encrypt-sse         Whether to use Server Side Encryption
      --gcs-backup-bucket=<bucket>  
                               Google Cloud Storage bucket name to backup blocks
                               on repair operations.
      --s3-backup-bucket=<bucket>  
                               S3 bucket name to backup blocks on repair
                               operations.
  -r, --repair                 attempt to repair blocks for which issues were
                               detected
  -i, --issues=index_issue... ...  
                               Issues to verify (and optionally repair).
                               Possible values: [overlapped_blocks
                               duplicated_compaction index_issue]
      --id-whitelist=ID-WHITELIST ...  
                               Block IDs to verify (and optionally repair) only.
                               If none is specified, all blocks will be
                               verified. Repeated field

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

list all blocks in the bucket

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
      --gcs-bucket=<bucket>    Google Cloud Storage bucket name for stored
                               blocks.
      --s3.bucket=<bucket>     S3-Compatible API bucket name for stored blocks.
      --s3.endpoint=<api-url>  S3-Compatible API endpoint for stored blocks.
      --s3.access-key=<key>    Access key for an S3-Compatible API.
      --s3.insecure            Whether to use an insecure connection with an
                               S3-Compatible API.
      --s3.signature-version2  Whether to use S3 Signature Version 2; otherwise
                               Signature Version 4 will be used.
      --s3.encrypt-sse         Whether to use Server Side Encryption
      --gcs-backup-bucket=<bucket>  
                               Google Cloud Storage bucket name to backup blocks
                               on repair operations.
      --s3-backup-bucket=<bucket>  
                               S3 bucket name to backup blocks on repair
                               operations.
  -o, --output=""              Format in which to print each block's
                               information. May be 'json' or custom template.

```

