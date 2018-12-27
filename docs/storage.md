# Object Storage

Thanos supports any object stores that can be implemented against Thanos [objstore.Bucket inteface](/pkg/objstore/objstore.go)

All clients are configured using `--objstore.config-file` to reference to the configuration file or `--objstore.config` to put yaml config directly.

## Implementations 

Current object storage client implementations:

| Provider             | Maturity | Auto-tested on CI | Maintainers |
|----------------------|-------------------|-----------|---------------|
| Google Cloud Storage | Stable  (production usage)             | yes       | @bwplotka   |
| AWS S3               | Beta  (working PoCs, testing usage)               | no        | @bwplotka          |
| Azure Storage Account | Alpha   | yes       | @vglafirov   |
| OpenStack Swift      | Beta  (working PoCs, testing usage)               | no        | @sudhi-vm   |

NOTE: Currently Thanos requires strong consistency (write-read) for object store implementation.

## How to add a new client?

1. Create new directory under `pkg/objstore/<provider>`
2. Implement [objstore.Bucket inteface](/pkg/objstore/objstore.go)
3. Add `NewTestBucket` constructor for testing purposes, that creates and deletes temporary bucket.
4. Use created `NewTestBucket` in [ForeachStore method](/pkg/objstore/objtesting/foreach.go) to ensure we can run tests against new provider. (In PR)
5. RUN the [TestObjStoreAcceptanceTest](/pkg/objstore/objtesting/acceptance_e2e_test.go) against your provider to ensure it fits. Fix any found error until test passes. (In PR)
6. Add client implementation to the factory in [factory](/pkg/objstore/client/factory.go) code. (Using as small amount of flags as possible in every command)
7. Add client struct config to [bucketcfggen](/scripts/bucketcfggen/main.go) to allow config auto generation.

At that point, anyone can use your provider by spec

## AWS S3 configuration

Thanos uses minio client to upload Prometheus data into AWS S3.

To configure S3 bucket as an object store you need to set these mandatory S3 variables in yaml format stored in a file:

[embedmd]:# (flags/config_s3.txt yaml)
```yaml
type: S3
config:
  bucket: ""
  endpoint: ""
  access_key: ""
  insecure: false
  signature_version2: false
  encrypt_sse: false
  secret_key: ""
  http_config:
    idle_conn_timeout: 0s
```

AWS region to endpoint mapping can be found in this [link](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)

Make sure you use a correct signature version.
Currently AWS require signature v4, so it needs `signature-version2: false`, otherwise, you will get Access Denied error, but several other S3 compatible use `signature-version2: true`

For debug purposes you can set `insecure: true` to switch to plain insecure HTTP instead of HTTPS

### Credentials
By default Thanos will try to retrieve credentials from the following sources:

1. From config file if BOTH `access_key` and `secret_key` are present.
1. IAM credentials retrieved from an instance profile.
1. From `~/.aws/credentials`
1. From the standard AWS environment variable - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

NOTE: Getting access key from config file and secret key from other method (and vice versa) is not supported. 

### AWS Policies

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
THANOS_SKIP_GCS_TESTS=true
THANOS_ALLOW_EXISTING_BUCKET_USE=true

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

With this policy you should be able to run set `THANOS_SKIP_GCS_TESTS=true` and unset `S3_BUCKET` and run all tests using `make test`.

Details about AWS policies: https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html

## GCS Configuration

To configure Google Cloud Storage bucket as an object store you need to set `bucket` with GCS bucket name and configure Google Application credentials.

For example:

[embedmd]:# (flags/config_gcs.txt yaml)
```yaml
type: GCS
config:
  bucket: ""
```

Application credentials are configured via JSON file, the client looks for:

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

### GCS Policies

For deployment:

`Storage Object Creator` and ` Storage Object Viewer`

For testing:

`Storage Object Admin` for ability to create and delete temporary buckets.

## Azure Configuration

To use Azure Storage as Thanos object store, you need to precreate storage account from Azure portal or using Azure CLI. Follow the instructions from Azure Storage Documentation: [https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=portal)

To configure Azure Storage account as an object store you need to provide a path to Azure storage config file in flag `--objstore.config-file`.

Config file format is the following:

[embedmd]:# (flags/config_azure.txt yaml)
```yaml
type: AZURE
config:
  storage_account: ""
  storage_account_key: ""
  container: ""
```

### OpenStack Swift Configuration
Thanos uses [gophercloud](http://gophercloud.io/) client to upload Prometheus data into [OpenStack Swift](https://docs.openstack.org/swift/latest/).

Below is an example configuration file for thanos to use OpenStack swift container as an object store. 

[embedmd]:# (flags/config_swift.txt yaml)
```yaml
type: SWIFT
config:
  storage_account: ""
  storage_account_key: ""
  container: ""
```

## Other minio supported S3 object storages

Minio client used for AWS S3 can be potentially configured against other S3-compatible object storages.

