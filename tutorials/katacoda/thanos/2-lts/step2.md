# Step 2 - Object Storage Configuration

In this step, we will configure the object store and change sidecar to upload to the object-store.

## Running Minio

Now, execute the command

```
mkdir -p /storage/thanos && docker run -d --name minio -v /storage:/data -p 9000:9000 -e "MINIO_ACCESS_KEY=minio" -e "MINIO_SECRET_KEY=minio123" minio/minio:RELEASE.2019-01-31T00-31-19Z server /data
```{{execute}}

## Verification

Now, you should have minio running well.

To check if the Minio is working as intended, let's check out [here](https://[[HOST_SUBDOMAIN]]-9000-[[KATACODA_HOST]].environments.katacoda.com/minio/)

Enter the credentials as mentioned below:

**Access Key** = `minio`
**Secret Key** = `minio123`

## Configuration :

The configuration file content :

Click `Copy To Editor` for config to propagate the configs to the file `bucket_storage.yml`:

<pre class="file" data-filename="bucket_storage.yml" data-target="replace">
type: S3
config:
  bucket: "thanos"
  endpoint: "127.0.0.1:9000"
  insecure: true
  signature_version2: true
  access_key: "minio"
  secret_key: "minio123"
</pre>

Before moving forward, we need to stop the `sidecar container` and we can do so by executing the following command:

```
docker stop prometheus-0-sidecar-eu1
```{{execute}}

Now, execute the following command :

```
docker run -d --net=host --rm \
    -v $(pwd)/bucket_storage.yml:/etc/prometheus/bucket_storage.yml \
    -v $(pwd)/test:/prometheus \
    --name sidecar \
    -u root \
    quay.io/thanos/thanos:v0.15.0 \
    sidecar \
    --tsdb.path                 /prometheus \
    --objstore.config-file      /etc/prometheus/bucket_storage.yml \
    --prometheus.url            http://127.0.0.1:9090 \
    --http-address              0.0.0.0:19090    \
    --grpc-address              0.0.0.0:19190 && echo "Store API exposed"
```{{execute}}

The flag `--objstore.config-file` loads all the required configuration from the file to ship the TSDB blocks to an object storage bucket, the storage endpoints, and the credentials used.

## Verification

We can check whether the data is uploaded into `thanos` bucket by visitng [Minio](https://[[HOST_SUBDOMAIN]]-9000-[[KATACODA_HOST]].environments.katacoda.com/minio/). The stored metrics will also be available in the object storage.