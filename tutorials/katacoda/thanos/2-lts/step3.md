# Step 3 - Fetching metrics from Bucket

In this step, we will learn about Thanos Store Gateway and how to deploy it.

## Thanos Components

Let's take a look at all the Thanos commands:

```docker run --rm quay.io/thanos/thanos:v0.16.0 --help```{{execute}}

You should see multiple commands that solve different purposes, block storage based long-term storage for Prometheus.

In this step we will focus on thanos `store gateway`:

```
  store [<flags>]
    Store node giving access to blocks in a bucket provider
```

## Store Gateway:

* This component implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need
significant amounts of local disk space.
* It keeps a small amount of information about all remote blocks on the local disk and keeps it in sync with the bucket.
This data is generally safe to delete across restarts at the cost of increased startup times.

You can read more about [Store](https://thanos.io/tip/components/store.md/) here.

### Deploying store for "EU1" Prometheus data

```
docker run -d --net=host --rm \
    -v /root/editor/bucket_storage.yaml:/etc/thanos/minio-bucket.yaml \
    --name store-gateway \
    quay.io/thanos/thanos:v0.16.0 \
    store \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --http-address 0.0.0.0:19091 \
    --grpc-address 0.0.0.0:19191
```{{execute}}

## How to query Thanos store data?

In this step, we will see how we can query Thanos store data which has access to historical data from the `thanos` bucket, and let's play with this setup a bit.

Click on the [Querier UI `Graph` page](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/graph) and try querying data for a year or two by inserting metrics [continuous_app_metric0](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/graph?g0.range_input=1y&g0.max_source_resolution=0s&g0.expr=continuous_app_metric0&g0.tab=0). Make sure `deduplication` is selected and you will be able to discover all the data fetched by Thanos store.

![](https://github.com/soniasingla/thanos/raw/master/tutorials/katacoda/thanos/2-lts/query.png)

Also, you can check all the active endpoints located by thanos-store by clicking on [Stores](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/stores).

We've added Thanos Query, a web and API frontend that can query a Prometheus instance and Thanos Store at the same time, which gives transparent access to the archived blocks and real-time metrics. The vanilla PromQL Prometheus engine used for evaluating the query deduces what time series and for what time ranges we need to fetch the data. Also, StoreAPIs propagate external labels and the time range they have data for, so we can do basic filtering on this. However, if you don't specify any of these in the query (only "up" series) the querier concurrently asks all the StoreAPI servers. It might cause a duplication of results between sidecar and store data.

Now, another interesting question here is how to ensure if we query the data from bucket only? We can check this by visitng the New UI, inseting `continuous_app_metric0` metrics again with 1 year time range of graph, and click on `Enable Store Filtering`. This allows us to filter stores and helps in debugging from where we are querying the data exactly.

## Question Time? 🤔

In an HA Prometheus setup with Thanos sidecars, would there be issues with multiple sidecars attempting to upload the same data blocks to object storage?

Think over this 😉

To see the answer to this question click `SHOW SOLUTION` below.

## Next

Voila! In the next step, we will talk about Thanos Compactor, it's retention capabilities, and how it improves query efficiency and reduce the required storage size.