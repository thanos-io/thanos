## Long term retention!

### Step 3: Let's start object storage first.

In this step, we will configure the object store and change sidecar to upload to the
object-store.

## Running Minio

```
mkdir ${CURR_DIR}/minio && \
docker run -d --rm --name minio \
     -v ${CURR_DIR}/minio:/data \
     -p 9000:9000 -e "MINIO_ACCESS_KEY=rawkode" -e "MINIO_SECRET_KEY=rawkodeloveobs" \
     minio/minio:RELEASE.2019-01-31T00-31-19Z \
    server /data
```{{execute}}

Create `thanos` bucket:

```
mkdir ${CURR_DIR}/minio/thanos
```{{execute}}


To check if the Minio is working as intended, let's [open Minio server UI](https://[[HOST_SUBDOMAIN]]-9000-[[KATACODA_HOST]].environments.katacoda.com/minio/)

Enter the credentials as mentioned below:

**Access Key** = `rawkode`
**Secret Key** = `rawkodeloveobs`

### Step 4: Configure sidecar to upload blocks.

```
cat <<EOF > ${CURR_DIR}/minio-bucket.yaml
type: S3
config:
  bucket: "thanos"
  endpoint: "127.0.0.1:9000"
  insecure: true
  signature_version2: true
  access_key: "rawkode"
  secret_key: "rawkodeloveobs"
EOF
```{{execute}}

Before moving forward, we need to roll new sidecar configuration with new bucket config.

```
docker stop prom-eu1-0-sidecar
docker stop prom-eu1-1-sidecar
docker stop prom-us1-0-sidecar
```{{execute}}

Now, execute the following command :


* `cluster="eu1", replica="0"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/minio-bucket.yaml:/etc/thanos/minio-bucket.yaml \
    -v ${CURR_DIR}/prom-eu1-replica0:/prometheus \
    --name prom-eu1-0-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --tsdb.path /prometheus \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --shipper.upload-compacted \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_EU1_0_PORT}"
```{{execute}}

* `cluster="eu1", replica="1"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-eu1-replica1-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/minio-bucket.yaml:/etc/thanos/minio-bucket.yaml \
    -v ${CURR_DIR}/prom-eu1-replica1:/prometheus \
    --name prom-eu1-1-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --tsdb.path /prometheus \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --shipper.upload-compacted \
    --http-address 0.0.0.0:19092 \
    --grpc-address 0.0.0.0:19192 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_EU1_1_PORT}"
```{{execute}}

* `cluster="us1", replica="0"` sidecar:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/prom-us1-replica0-config.yaml:/etc/prometheus/prometheus.yml \
    -v ${CURR_DIR}/minio-bucket.yaml:/etc/thanos/minio-bucket.yaml \
    -v ${CURR_DIR}/prom-us1-replica0:/prometheus \
    --name prom-us1-0-sidecar \
    -u root \
    quay.io/thanos/thanos:v0.19.0 \
    sidecar \
    --tsdb.path /prometheus \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --shipper.upload-compacted \
    --http-address 0.0.0.0:19093 \
    --grpc-address 0.0.0.0:19193 \
    --reloader.config-file /etc/prometheus/prometheus.yml \
    --prometheus.url "http://127.0.0.1:${PROM_US1_0_PORT}"
```{{execute}}

We can check whether the data is uploaded into `thanos` bucket by visitng [Minio](https://[[HOST_SUBDOMAIN]]-9000-[[KATACODA_HOST]].environments.katacoda.com/minio/) (or `localhost:9000`) It will take a minute to synchronize all blocks. Note that sidecar by default uploads only "non compacted by Prometheus" blocks.

See [this](https://thanos.io/tip/components/sidecar.md/#upload-compacted-blocks) to read more about uploading old data already touched by Prometheus.

Once we see all ~40 blocks appear in the minio, we are sure our data is backed up. Awesome!

### Mhm, how to query those data now?

Let's run Store Gateway server:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/minio-bucket.yaml:/etc/thanos/minio-bucket.yaml \
    --name store-gateway \
    quay.io/thanos/thanos:v0.19.0 \
    store \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --http-address 0.0.0.0:19094 \
    --grpc-address 0.0.0.0:19194
```{{execute}}

### Let's point query to new StoreAPI!

```
docker stop querier && \
docker run -d --net=host --rm \
    --name querier \
    quay.io/thanos/thanos:v0.19.0 \
    query \
    --http-address 0.0.0.0:9090 \
    --grpc-address 0.0.0.0:19190 \
    --query.replica-label replica \
    --store 127.0.0.1:19191 \
    --store 127.0.0.1:19192 \
    --store 127.0.0.1:19193 \
    --store 127.0.0.1:19194
```{{execute}}

Visit https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com to see Thanos UI.

### Long term maintainance, retention, dedup and downsampling:

```
docker run -d --net=host --rm \
    -v ${CURR_DIR}/minio-bucket.yaml:/etc/thanos/minio-bucket.yaml \
    --name compactor \
    quay.io/thanos/thanos:v0.19.0 \
    compact \
    --wait --wait-interval 30s \
    --consistency-delay 0s \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --http-address 0.0.0.0:19095
```{{execute}}

Visit https://[[HOST_SUBDOMAIN]]-19095-[[KATACODA_HOST]].environments.katacoda.com/new/loaded to see Compactor Web UI.

### Data should be immdiately downsampled as well for smooth expierience!

Visit https://[[HOST_SUBDOMAIN]]-9090-[[KATACODA_HOST]].environments.katacoda.com to see Thanos UI and query for 1 year.

Check 5m downsampling vs raw data.

## Feel free to play around, more components will be added in future (:

<img src="https://i.imgflip.com/4mw3l5.jpg" title="yoda"/>

