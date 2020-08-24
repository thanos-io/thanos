---
title: Object Storage
type: docs
menu: thanos
slug: /storage.md
---

# Object Storage

Thanos supports any object stores that can be implemented against Thanos [objstore.Bucket interface](/pkg/objstore/objstore.go)

All clients are configured using `--objstore.config-file` to reference to the configuration file or `--objstore.config` to put yaml config directly.

## How to use `config` flags?

You can either pass YAML file defined below in `--objstore.config-file` or pass the YAML content directly using `--objstore.config`.
We recommend the latter as it gives an explicit static view of configuration for each component. It also saves you the fuss of creating and managing additional file.

Don't be afraid of multiline flags!

In Kubernetes it is as easy as (on Thanos sidecar example):

```yaml
      - args:
        - sidecar
        - |
          --objstore.config=type: GCS
          config:
            bucket: <bucket>
        - --prometheus.url=http://localhost:9090
        - |
          --tracing.config=type: STACKDRIVER
          config:
            service_name: ""
            project_id: <project>
            sample_factor: 16
        - --tsdb.path=/prometheus-data
```

## How to add a new client?

1. Create new directory under `pkg/objstore/<provider>`
2. Implement [objstore.Bucket interface](/pkg/objstore/objstore.go)
3. Add `NewTestBucket` constructor for testing purposes, that creates and deletes temporary bucket.
4. Use created `NewTestBucket` in [ForeachStore method](/pkg/objstore/objtesting/foreach.go) to ensure we can run tests against new provider. (In PR)
5. RUN the [TestObjStoreAcceptanceTest](/pkg/objstore/objtesting/acceptance_e2e_test.go) against your provider to ensure it fits. Fix any found error until test passes. (In PR)
6. Add client implementation to the factory in [factory](/pkg/objstore/client/factory.go) code. (Using as small amount of flags as possible in every command)
7. Add client struct config to [bucketcfggen](/scripts/cfggen/main.go) to allow config auto generation.

At that point, anyone can use your provider by spec.

## Configuration

Current object storage client implementations:

| Provider             | Maturity | Auto-tested on CI | Maintainers |
|----------------------|-------------------|-----------|---------------|
| [Google Cloud Storage](./storage.md#gcs) | Stable  (production usage)             | yes       | @bwplotka   |
| [AWS/S3](./storage.md#s3) | Stable  (production usage)               | yes        | @bwplotka          |
| [Azure Storage Account](./storage.md#azure) | Stable  (production usage) | no       | @vglafirov   |
| [OpenStack Swift](./storage.md#openstack-swift)      | Beta  (working PoCs, testing usage)               | yes       | @sudhi-vm   |
| [Tencent COS](./storage.md#tencent-cos)          | Beta  (testing usage)                   | no        | @jojohappy          |
| [AliYun OSS](./storage.md#aliyun-oss)           | Beta  (testing usage)                   | no        | @shaulboozhiao,@wujinhu      |
| [Local Filesystem](./storage.md#filesystem) | Beta  (testing usage)             | yes       | @bwplotka   |

NOTE: Currently Thanos requires strong consistency (write-read) for object store implementation.

### S3

Thanos uses the [minio client](https://github.com/minio/minio-go) library to upload Prometheus data into AWS S3.

You can configure an S3 bucket as an object store with YAML, either by passing the configuration directly to the `--objstore.config` parameter, or (preferably) by passing the path to a configuration file to the `--objstore.config-file` option.

NOTE: Minio client was mainly for AWS S3, but it can be configured against other S3-compatible object storages e.g Ceph

[embedmd]:# (flags/config_bucket_s3.txt yaml)
```yaml
type: S3
config:
  bucket: ""
  endpoint: ""
  region: ""
  access_key: ""
  insecure: false
  signature_version2: false
  secret_key: ""
  put_user_metadata: {}
  http_config:
    idle_conn_timeout: 1m30s
    response_header_timeout: 2m
    insecure_skip_verify: false
  trace:
    enable: false
  part_size: 134217728
  sse_config:
    type: ""
    kms_key_id: ""
    kms_encryption_context: {}
    encryption_key: ""
```

At a minimum, you will need to provide a value for the `bucket`, `endpoint`, `access_key`, and `secret_key` keys. The rest of the keys are optional.

The AWS region to endpoint mapping can be found in this [link](https://docs.aws.amazon.com/general/latest/gr/s3.html).

Make sure you use a correct signature version. Currently AWS requires signature v4, so it needs `signature_version2: false`. If you don't specify it, you will get an `Access Denied` error. On the other hand, several S3 compatible APIs use `signature_version2: true`.

You can configure the timeout settings for the HTTP client by setting the `http_config.idle_conn_timeout` and `http_config.response_header_timeout` keys. As a rule of thumb, if you are seeing errors like `timeout awaiting response headers` in your logs, you may want to increase the value of `http_config.response_header_timeout`.

Please refer to the documentation of [the Transport type](https://golang.org/pkg/net/http/#Transport) in the `net/http` package for detailed information on what each option does.

`part_size` is specified in bytes and refers to the minimum file size used for multipart uploads, as some custom S3 implementations may have different requirements. A value of `0` means to use a default 128 MiB size.

For debug and testing purposes you can set

* `insecure: true` to switch to plain insecure HTTP instead of HTTPS

* `http_config.insecure_skip_verify: true` to disable TLS certificate verification (if your S3 based storage is using a self-signed certificate, for example)

* `trace.enable: true` to enable the minio client's verbose logging. Each request and response will be logged into the debug logger, so debug level logging must be enabled for this functionality.

#### S3 Server-Side Encryption

SSE can be configued using the `sse_config`. [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html), [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html), and [SSE-C](https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html) are supported.

* If type is set to `SSE-S3` you do not need to configure other options.

* If type is set to `SSE-KMS` you must set `kms_key_id`. The `kms_encryption_context` is optional, as [AWS provides a default encryption context](https://docs.aws.amazon.com/kms/latest/developerguide/services-s3.html#s3-encryption-context).

* If type is set to `SSE-C` you must provide a path to the encryption key using `encryption_key`.

If the SSE Config block is set but the `type` is not one of `SSE-S3`, `SSE-KMS`, or `SSE-C`, an error is raised.

You will also need to apply the following AWS IAM policy for the user to access the KMS key:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KMSAccess",
            "Effect": "Allow",
            "Action": [
                "kms:GenerateDataKey",
                "kms:Encrypt",
                "kms:Decrypt"
            ],
            "Resource": "arn:aws:kms:<region>:<account>:key/<KMS key id>"
        }
    ]
}
```

#### Credentials

By default Thanos will try to retrieve credentials from the following sources:

1. From config file if BOTH `access_key` and `secret_key` are present.
1. From the standard AWS environment variable - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
1. From `~/.aws/credentials`
1. IAM credentials retrieved from an instance profile.

NOTE: Getting access key from config file and secret key from other method (and vice versa) is not supported.

#### AWS Policies

Example working AWS IAM policy for user:

* For deployment (policy for Thanos services):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket>/*",
                "arn:aws:s3:::<bucket>"
            ]
        }
    ]
}
```

(No bucket policy)

To test the policy, set env vars for S3 access for *empty, not used* bucket as well as:

```
THANOS_TEST_OBJSTORE_SKIP=GCS,AZURE,SWIFT,COS,ALIYUNOSS
THANOS_ALLOW_EXISTING_BUCKET_USE=true
```

And run: `GOCACHE=off go test -v -run TestObjStore_AcceptanceTest_e2e ./pkg/...`

* For testing (policy to run e2e tests):

We need access to CreateBucket and DeleteBucket and access to all buckets:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:DeleteObject",
                "s3:PutObject",
                "s3:CreateBucket",
                "s3:DeleteBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket>/*",
                "arn:aws:s3:::<bucket>"
            ]
        }
    ]
}
```

With this policy you should be able to run set `THANOS_TEST_OBJSTORE_SKIP=GCS,AZURE,SWIFT,COS,ALIYUNOSS` and unset `S3_BUCKET` and run all tests using `make test`.

Details about AWS policies: https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html

### GCS

To configure Google Cloud Storage bucket as an object store you need to set `bucket` with GCS bucket name and configure Google Application credentials.

For example:

[embedmd]:# (flags/config_bucket_gcs.txt yaml)
```yaml
type: GCS
config:
  bucket: ""
  service_account: ""
```

#### Using GOOGLE_APPLICATION_CREDENTIALS

Application credentials are configured via JSON file and only the bucket needs to be specified,
the client looks for:

1. A JSON file whose path is specified by the
   `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
2. A JSON file in a location known to the gcloud command-line tool.
   On Windows, this is `%APPDATA%/gcloud/application_default_credentials.json`.
   On other systems, `$HOME/.config/gcloud/application_default_credentials.json`.
3. On Google App Engine it uses the `appengine.AccessToken` function.
4. On Google Compute Engine and Google App Engine Managed VMs, it fetches
   credentials from the metadata server.
   (In this final case any provided scopes are ignored.)

You can read more on how to get application credential json file in [https://cloud.google.com/docs/authentication/production](https://cloud.google.com/docs/authentication/production)

#### Using inline a Service Account

Another possibility is to inline the ServiceAccount into the Thanos configuration and only maintain one file.
This feature was added, so that the Prometheus Operator only needs to take care of one secret file.

```yaml
type: GCS
config:
  bucket: "thanos"
  service_account: |-
    {
      "type": "service_account",
      "project_id": "project",
      "private_key_id": "abcdefghijklmnopqrstuvwxyz12345678906666",
      "private_key": "-----BEGIN PRIVATE KEY-----\...\n-----END PRIVATE KEY-----\n",
      "client_email": "project@thanos.iam.gserviceaccount.com",
      "client_id": "123456789012345678901",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/thanos%40gitpods.iam.gserviceaccount.com"
    }
```

#### GCS Policies

__Note:__ GCS Policies should be applied at the project level, not at the bucket level

For deployment:

`Storage Object Creator` and `Storage Object Viewer`

For testing:

`Storage Object Admin` for ability to create and delete temporary buckets.

To test the policy is working as expected, exec into the sidecar container, eg:

```sh
kubectl exec -it -n <namespace> <prometheus with sidecar pod name> -c <sidecar container name> -- /bin/sh
```

Then test that you can at least list objects in the bucket, eg:

```sh
thanos bucket ls --objstore.config="${OBJSTORE_CONFIG}"
```

### Azure

To use Azure Storage as Thanos object store, you need to precreate storage account from Azure portal or using Azure CLI. Follow the instructions from Azure Storage Documentation: [https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal)

To configure Azure Storage account as an object store you need to provide a path to Azure storage config file in flag `--objstore.config-file`.

Config file format is the following:

[embedmd]:# (flags/config_bucket_azure.txt yaml)
```yaml
type: AZURE
config:
  storage_account: ""
  storage_account_key: ""
  container: ""
  endpoint: ""
  max_retries: 0
```

### OpenStack Swift

Thanos uses [gophercloud](http://gophercloud.io/) client to upload Prometheus data into [OpenStack Swift](https://docs.openstack.org/swift/latest/).

Below is an example configuration file for thanos to use OpenStack swift container as an object store.
Note that if the `name` of a user, project or tenant is used one must also specify its domain by ID or name.
Various examples for OpenStack authentication can be found in the [official documentation](https://developer.openstack.org/api-ref/identity/v3/index.html?expanded=password-authentication-with-scoped-authorization-detail#password-authentication-with-unscoped-authorization).

[embedmd]:# (flags/config_bucket_swift.txt yaml)
```yaml
type: SWIFT
config:
  auth_url: ""
  username: ""
  user_domain_name: ""
  user_domain_id: ""
  user_id: ""
  password: ""
  domain_id: ""
  domain_name: ""
  project_id: ""
  project_name: ""
  project_domain_id: ""
  project_domain_name: ""
  region_name: ""
  container_name: ""
```

### Tencent COS

To use Tencent COS as storage store, you should apply a Tencent Account to create an object storage bucket at first. Note that detailed from Tencent Cloud Documents: [https://cloud.tencent.com/document/product/436](https://cloud.tencent.com/document/product/436)

To configure Tencent Account to use COS as storage store you need to set these parameters in yaml format stored in a file:

[embedmd]:# (flags/config_bucket_cos.txt yaml)
```yaml
type: COS
config:
  bucket: ""
  region: ""
  app_id: ""
  secret_key: ""
  secret_id: ""
```

Set the flags `--objstore.config-file` to reference to the configuration file.

##  AliYun OSS
In order to use AliYun OSS object storage, you should first create a bucket with proper Storage Class , ACLs and get the access key on the AliYun cloud. Go to [https://www.alibabacloud.com/product/oss](https://www.alibabacloud.com/product/oss) for more detail.

To use AliYun OSS object storage, please specify following yaml configuration file in `objstore.config*` flag.

[embedmd]:# (flags/config_bucket_aliyunoss.txt yaml)
```yaml
type: ALIYUNOSS
config:
  endpoint: ""
  bucket: ""
  access_key_id: ""
  access_key_secret: ""
```

Use --objstore.config-file to reference to this configuration file.

### Filesystem

This storage type is used when user wants to store and access the bucket in the local filesystem.
We treat filesystem the same way we would treat object storage, so all optimization for remote bucket applies even though,
we might have the files locally.

NOTE: This storage type is experimental and might be inefficient. It is NOT advised to use it as the main storage for metrics
in production environment. Particularly there is no planned support for distributed filesystems like NFS.
This is mainly useful for testing and demos.

[embedmd]:# (flags/config_bucket_filesystem.txt yaml)
```yaml
type: FILESYSTEM
config:
  directory: ""
```
