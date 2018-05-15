# Object Storage

Thanos supports any object stores that can be implemented against Thanos [objstore.Bucket inteface](/pkg/objstore/objstore.go)

Current object storage client implementations:

| Provider             | Maturity | Auto-tested on CI | Maintainers |
|----------------------|-------------------|-----------|---------------|
| Google Cloud Storage | Stable  (production usage)             | yes       | @bplotka   |
| AWS S3               | Beta  (working PoCs, testing usage)               | no        | ?          |

NOTE: Currently Thanos requires strong consistency (write-read) for object store implementation.

## How to add a new client?

1. Create new directory under `pkg/objstore/<provider>`
2. Implement [objstore.Bucket inteface](/pkg/objstore/objstore.go)
3. Add `NewTestBucket` constructor for testing purposes, that creates and deletes temporary bucket.
4. Use created `NewTestBucket` in [ForeachStore method](/pkg/objstore/objtesting/foreach.go) to ensure we can run tests against new provider. (In PR)
5. RUN the [TestObjStoreAcceptanceTest](/pkg/objstore/objtesting/acceptance_test.go) against your provider to ensure it fits. Fix any found error until test passes. (In PR)
6. Add client implementation to the factory in [factory](/pkg/objstore/client/factory.go) code. (Using as small amount of flags as possible in every command)

At that point, anyone can use your provider!

## S3 configuration

Thanos uses minio client to upload Prometheus data into s3.

To configure S3 bucket as an object store you need to set these mandatory S3 flags:
- --s3.endpoint
- --s3.bucket
- --s3.access-key

and set `S3_SECRET_KEY` environment variable with AWS secret key.

Instead of using flags you can pass all the configuration via environment variables:
- `S3_BUCKET`
- `S3_ENDPOINT`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_INSECURE`
- `S3_SIGNATURE_VERSION2`

AWS region to endpoint mapping can be found in this [link](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)

Make sure you use a correct signature version with `--s3.signature-version2`, otherwise, you will get Access Denied error.

For debug purposes you can `--s3.insecure` to switch to plain insecure HTTP instead of HTTPS

## GCP Configuration 

To configure Google Cloud Storage bucket as an object store you need to set `--gcs.bucket` with GCS bucket name and configure Google Application credentials.

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

