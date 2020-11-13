# Step 4 - Thanos Compactor

In this step, we will install Thanos Compactor which applies the compaction procedure of the Prometheus 2.0 storage engine to block data in object storage.

Before, moving forward, let's take a closer look at what the `Compactor` component does:

## Compactor

The `Compactor` is an essential component that operates on a single object storage bucket to compact, down-sample, apply retention, to the TSDB blocks held inside, thus, making queries on historical data more efficient. It creates aggregates of old metrics (based upon the rules).

It is also responsible for downsampling of data, performing 5m downsampling after 40 hours, and 1h downsampling after 10 days.

If you want to know more about Thanos Compactor, jump [here](https://thanos.io/tip/components/compact.md/).

**Note**: Thanos Compactor is mandatory if you use object storage otherwise Thanos Store Gateway will be too slow without using a compactor.

## Deploying Thanos Compactor

Click below snippet to start the Compactor.

```
docker run -d --net=host --rm \
 -v /root/editor/bucket_storage.yml:/etc/prometheus/bucket_storage.yml \
    --name thanos-compact \
    quay.io/thanos/thanos:v0.16.0 \
    compact \
    --wait --wait-interval 30s \
    --consistency-delay 0s \
    --objstore.config-file /etc/prometheus/bucket_storage.yml \
    --http-address 0.0.0.0:19095
```{{execute}}

The flag `wait` is used to make sure all compactions have been processed while `--wait-interval` is kept in 30s to perform all the compactions and downsampling very quickly. Also, this only works when when `--wait` flag is specified. Another flag `--consistency-delay` is basically used for buckets which are not consistent strongly. It is the minimum age of non-compacted blocks before they are being processed. Here, we kept the delay at 0s assuming the bucket is consistent.

## Unlimited Retention - Not Challenging anymore?

Having a long time metric retention for Prometheus was always involving lots of complexity, disk space, and manual work. With Thanos, you can make Prometheus almost stateless, while having most of the data in durable and cheap object storage.

## Next

Awesome work! Feel free to play with the setup 🤗

Once Done, hit `Continue` for summary.