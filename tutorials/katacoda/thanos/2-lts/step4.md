# Step 4 - Thanos Compactor

In this step, we will install Thanos Compactor which applies the compaction, retention, deletion and downsampling operations
on the TSDB block data object storage.

Before, moving forward, let's take a closer look at what the `Compactor` component does:

> Note: Thanos Compactor is currently designed to be run as a singleton. Unavailability (hours) is not problematic as it does not serve any live requests.
## Compactor

The `Compactor` is an essential component that operates on a single object storage bucket to compact, downsample, apply retention, to the TSDB blocks held inside,
thus, making queries on historical data more efficient. It creates aggregates of old metrics (based upon the rules).

It is also responsible for downsampling of data, performing 5m downsampling after 40 hours, and 1h downsampling after 10 days.

If you want to know more about Thanos Compactor, jump [here](https://thanos.io/tip/components/compact.md/).

> Note: Thanos Compactor is mandatory if you use object storage otherwise Thanos Store Gateway will be too slow without using a compactor.

## Deploying Thanos Compactor

Click below snippet to start the Compactor.

```
docker run -d --net=host --rm \
 -v /root/editor/bucket_storage.yaml:/etc/thanos/minio-bucket.yaml \
    --name thanos-compact \
    quay.io/thanos/thanos:v0.19.0 \
    compact \
    --wait --wait-interval 30s \
    --consistency-delay 0s \
    --objstore.config-file /etc/thanos/minio-bucket.yaml \
    --http-address 0.0.0.0:19095
```{{execute}}

The flag `wait` is used to make sure all compactions have been processed while `--wait-interval` is kept in 30s to perform all the compactions and downsampling very quickly. Also, this only works when when `--wait` flag is specified. Another flag `--consistency-delay` is basically used for buckets which are not consistent strongly. It is the minimum age of non-compacted blocks before they are being processed. Here, we kept the delay at 0s assuming the bucket is consistent.

## Setup Verification

To check if compactor works fine, we can look at the [Bucket View](https://[[HOST_SUBDOMAIN]]-19095-[[KATACODA_HOST]].environments.katacoda.com/new/loaded).

Now, if we click on the blocks, they will provide us all the metadata (Series, Samples, Resolution, Chunks, and many more things).

## Compaction and Downsampling

When we query large historical data there are millions of samples that we need to go through which makes the queries slower and slower as we retrieve year's worth of data.

Thus, Thanos uses the technique called downsampling (a process of reducing the sampling rate of the signal) to keep the queries responsive,
and no special configuration is required to perform this process.

The Compactor applies compaction to the bucket data and also completes the downsampling for historical data.

To expierience this, click on the [Querier](https://[[HOST_SUBDOMAIN]]-9091-[[KATACODA_HOST]].environments.katacoda.com/new/graph?g0.expr=&g0.tab=0&g0.stacked=0&g0.range_input=1h&g0.max_source_resolution=0s&g0.deduplicate=1&g0.partial_response=0&g0.store_matches=[]) and insert metrics `continuous_app_metric0` with 1 year time range of graph, and also, click on `Enable Store Filtering`.

Let's try querying `Max 5m downsampling` data, it uses 5m resolution and it will be faster than the raw data. Also, Downsampling is built on top of data, and never done on **young** data.

## Unlimited Retention - Not Challenging anymore?

Having a long time metric retention for Prometheus was always involving lots of complexity, disk space, and manual work. With Thanos, you can make Prometheus almost stateless, while having most of the data in durable and cheap object storage.

## Next

Awesome work! Feel free to play with the setup 🤗

Once Done, hit `Continue` for summary.