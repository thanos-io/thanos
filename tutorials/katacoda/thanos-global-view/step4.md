# Step 4 - Adding Thanos Store

We're going to start Minio, an object storage server that implements AWS' S3 API that is compatible with Thanos. We'll then run the Thanos Store component that will download the metrics stored and expose them.

```
                        US1                                US2

               +------------+---------+           +------------+---------+
               | Prometheus | Sidecar |           | Prometheus | Sidecar |
               +------------+-+---+---+           +------------+----+--+-+
                              |   ^                                 ^  |
   +--------------------------+   |                                 |  |
   |                              |                                 |  |
   v                              |                                 |  |
+--+-+       +-------+            |     +-------+                   |  |
| S3 +<------+ Store +<-----------+-----+ Query +-------------------+  |
+--+-+       +-------+                  +-------+                      |
   ^                                                                   |
   |                                                                   |
   +-------------------------------------------------------------------+
```

## Minio

We'll start by launching minio with a predefined access key for simplification.

```
mkdir -p s3_data/thanos
docker run -d --net=host \
    --name minio \
    -v $(pwd)/s3_data:/data \
    -u root \
    -e "MINIO_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE" \
    -e "MINIO_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    minio/minio \
    server /data
```{{execute}}

Now that we have object storage, we can restart our sidecars with the right configuration to get them started writing metrics.

## Sidecar configuration

We'll write some YAML for the configuration (you could also provide inline YAML with the `objstore.config` flag):

<pre class="file" data-filename="thanos_sidecar.yml" data-target="replace">
type: S3
config:
  bucket: thanos
  endpoint: 127.0.0.1:9000
  access_key: AKIAIOSFODNN7EXAMPLE
  insecure: true
  secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

</pre>

We'll be reusing the same bucket for our data. Of course, you could also have a bucket per, say, datacenter and have dedicated stores instances for each buckets. For simplicity, we'll stay with one bucket and one store.

## Restarting the sidecar

Relaunch Thanos sidecar with our new parameters:

### US1

```
docker stop thanos-sidecar-us1 && docker rm thanos-sidecar-us1 
docker run -d --net=host \
    --name thanos-sidecar-us1 \
    -v $(pwd)/us1_data:/prometheus \
    -v $(pwd)/thanos_sidecar.yml:/thanos.yml \
    -u root \
    quay.io/thanos/thanos:v0.6.1 \
    sidecar \
    --tsdb.path /prometheus \
    --prometheus.url http://127.0.0.1:9090 \
    --objstore.config-file /thanos.yml
```{{execute}}

### EU1

```
docker stop thanos-sidecar-eu1 && docker rm thanos-sidecar-eu1 
docker run -d --net=host \
    --name thanos-sidecar-eu1 \
    -v $(pwd)/eu1_data:/prometheus \
    -v $(pwd)/thanos_sidecar.yml:/thanos.yml \
    -u root \
    quay.io/thanos/thanos:v0.6.1 \
    sidecar \
    --tsdb.path /prometheus \
    --prometheus.url http://127.0.0.1:9091 \
    --http-address="0.0.0.0:11002" \
    --grpc-address="0.0.0.0:11001" \
    --objstore.config-file /thanos.yml
```{{execute}}

## Store component

With the sidecars now writing data to S3 (TODO: explain 2h delay stuff)

We'll now start the Store component:

```
docker run -d --net=host \
    --name thanos-store \
    -v $(pwd)/thanos_sidecar.yml:/thanos.yml \
    -u root \
    quay.io/thanos/thanos:v0.6.1 \
    store \
    --http-address 0.0.0.0:12090 \
    --grpc-address 0.0.0.0:12091 \
    --objstore.config-file /thanos.yml
```{{execute}}

And restart the Query with the new store added:

```
docker stop thanos-query && docker rm thanos-query
docker run -d --net=host \
    --name thanos-query \
    quay.io/thanos/thanos:v0.6.1 \
    query \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19091 \
    --store 127.0.0.1:10901 \
    --store 127.0.0.1:11001 \
    --store 127.0.0.1:12091
```{{execute}}

If you look at the [Stores list](https://[[HOST_SUBDOMAIN]]-19090-[[KATACODA_HOST]].environments.katacoda.com/stores), you will see the new store component up and running.
