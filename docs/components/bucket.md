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
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                              GCP project to send Google Cloud Trace tracings
                              to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                              How often we send traces (1/<sample-factor>). If 0
                              no trace will be sent periodically, unless forced
                              by baggage item. See `pkg/tracing/tracing.go` for
                              details.
      --objstore.type=<provider>  
                              Specify the provider for object store. If empty or
                              unsupported provider, Thanos won't read and store
                              any block to the object store. Now supported GCS /
                              S3.
      --objstore.bucket=<bucket>  
                              The bucket name for stored blocks.
      --objstore.endpoint=<api-url>  
                              The object store API endpoint for stored blocks.
                              Supported S3-Compatible API
      --objstore.access-key=<key>  
                              Access key for an object store API. Supported
                              S3-Compatible API
      --objstore.insecure     Whether to use an insecure connection with an
                              object store API. Supported S3-Compatible API
      --objstore.signature-version2  
                              Whether to use S3 Signature Version 2; otherwise
                              Signature Version 4 will be used
      --objstore.encrypt-sse  Whether to use Server Side Encryption

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
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                              GCP project to send Google Cloud Trace tracings
                              to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                              How often we send traces (1/<sample-factor>). If 0
                              no trace will be sent periodically, unless forced
                              by baggage item. See `pkg/tracing/tracing.go` for
                              details.
      --objstore.type=<provider>  
                              Specify the provider for object store. If empty or
                              unsupported provider, Thanos won't read and store
                              any block to the object store. Now supported GCS /
                              S3.
      --objstore.bucket=<bucket>  
                              The bucket name for stored blocks.
      --objstore.endpoint=<api-url>  
                              The object store API endpoint for stored blocks.
                              Supported S3-Compatible API
      --objstore.access-key=<key>  
                              Access key for an object store API. Supported
                              S3-Compatible API
      --objstore.insecure     Whether to use an insecure connection with an
                              object store API. Supported S3-Compatible API
      --objstore.signature-version2  
                              Whether to use S3 Signature Version 2; otherwise
                              Signature Version 4 will be used
      --objstore.encrypt-sse  Whether to use Server Side Encryption
  -r, --repair                attempt to repair blocks for which issues were
                              detected
      --objstore-backup.type=<provider>  
                              Specify the provider for object store. If empty or
                              unsupported provider, Thanos won't read and store
                              any block to the object store. Now supported GCS /
                              S3.
      --objstore-backup.bucket=<bucket>  
                              The bucket name for stored blocks.
      --objstore-backup.endpoint=<api-url>  
                              The object store API endpoint for stored blocks.
                              Supported S3-Compatible API
      --objstore-backup.access-key=<key>  
                              Access key for an object store API. Supported
                              S3-Compatible API
      --objstore-backup.insecure  
                              Whether to use an insecure connection with an
                              object store API. Supported S3-Compatible API
      --objstore-backup.signature-version2  
                              Whether to use S3 Signature Version 2; otherwise
                              Signature Version 4 will be used
      --objstore-backup.encrypt-sse  
                              Whether to use Server Side Encryption
  -i, --issues=index_issue... ...  
                              Issues to verify (and optionally repair). Possible
                              values: [duplicated_compaction index_issue
                              overlapped_blocks]
      --id-whitelist=ID-WHITELIST ...  
                              Block IDs to verify (and optionally repair) only.
                              If none is specified, all blocks will be verified.
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

list all blocks in the bucket

Flags:
  -h, --help                  Show context-sensitive help (also try --help-long
                              and --help-man).
      --version               Show application version.
      --log.level=info        Log filtering level.
      --gcloudtrace.project=GCLOUDTRACE.PROJECT  
                              GCP project to send Google Cloud Trace tracings
                              to. If empty, tracing will be disabled.
      --gcloudtrace.sample-factor=1  
                              How often we send traces (1/<sample-factor>). If 0
                              no trace will be sent periodically, unless forced
                              by baggage item. See `pkg/tracing/tracing.go` for
                              details.
      --objstore.type=<provider>  
                              Specify the provider for object store. If empty or
                              unsupported provider, Thanos won't read and store
                              any block to the object store. Now supported GCS /
                              S3.
      --objstore.bucket=<bucket>  
                              The bucket name for stored blocks.
      --objstore.endpoint=<api-url>  
                              The object store API endpoint for stored blocks.
                              Supported S3-Compatible API
      --objstore.access-key=<key>  
                              Access key for an object store API. Supported
                              S3-Compatible API
      --objstore.insecure     Whether to use an insecure connection with an
                              object store API. Supported S3-Compatible API
      --objstore.signature-version2  
                              Whether to use S3 Signature Version 2; otherwise
                              Signature Version 4 will be used
      --objstore.encrypt-sse  Whether to use Server Side Encryption
  -o, --output=""             Format in which to print each block's information.
                              May be 'json' or custom template.

```

