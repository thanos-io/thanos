# Step 3 - Adding Thanos sidecar and query components

We're going to launch a sidecar for each Prometheus we have, giving them access to Prometheus' API and data folder. And then we'll launch the query component giving us an overview of all our metrics. At the end of this step we'll have this:

```
         US1                                US2

+------------+---------+           +------------+---------+
| Prometheus | Sidecar |           | Prometheus | Sidecar |
+------------+-----+---+           +------------+----+----+
                   ^                                 ^
                   |                                 |
                   |                                 |
                   |                                 |
                   |     +-------+                   |
                   +-----+ Query +-------------------+
                         +-------+
```

## US1

```
docker run -d --net=host \
    --name thanos-sidecar-us1 \
    -v $(pwd)/us1_data:/prometheus \
    -u root \
    quay.io/thanos/thanos:v0.6.1 \
    sidecar \
    --tsdb.path /prometheus \
    --prometheus.url http://127.0.0.1:9090
```{{execute}}

## EU1

```
docker run -d --net=host \
    --name thanos-sidecar-eu1 \
    -v $(pwd)/eu1_data:/prometheus \
    -u root \
    quay.io/thanos/thanos:v0.6.1 \
    sidecar \
    --tsdb.path /prometheus \
    --prometheus.url http://127.0.0.1:9091 \
    --http-address="0.0.0.0:11002" \
    --grpc-address="0.0.0.0:11001"
```{{execute}}

## Query component

We'll now start the Query component and we'll provide our two endpoints to our sidecars:

```
docker run -d --net=host \
    --name thanos-query \
    quay.io/thanos/thanos:v0.6.1 \
    query \
    --http-address 0.0.0.0:19090 \
    --grpc-address 0.0.0.0:19091 \
    --store 127.0.0.1:10901 \
    --store 127.0.0.1:11001
```{{execute}}

You can now access [Query's UI.](https://[[HOST_SUBDOMAIN]]-19090-[[KATACODA_HOST]].environments.katacoda.com/)

If you look at the [Stores list](https://[[HOST_SUBDOMAIN]]-19090-[[KATACODA_HOST]].environments.katacoda.com/stores), you will see our two sidecars healthy and running.

And if you run a Prometheus Query such as [`rate(go_gc_duration_seconds_count[2m])`](https://[[HOST_SUBDOMAIN]]-19090-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1h&g0.expr=rate(go_gc_duration_seconds_count[2m])&g0.tab=1) you'll be able to see the metrics from US1 and EU1 at the same time. Pretty neat right ?

## Up next

We'll mock an object storage provider (S3) and use it to store our metrics forever, we'll then use the Store component to make those metrics available to the Query gateway.
