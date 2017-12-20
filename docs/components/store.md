# Store

The store component of Thanos implements the Store API on top of historical data in an object storage bucket. It acts primarily as an API gateway and therefore does not need significant amounts of local disk space. It joins a Thanos cluster on startup and advertises the data it can access.
It keeps a small amount of information about all remote blocks on local disk and keeps it in sync with the bucket. This data is generally safe to delete across restarts at the cost of increased startup times.

```
$ thanos query \
    --tsdb.path        "/local/state/data/dir" \
    --gcs.bucket       "example-bucket" \
    --cluster.peers    "thanos-cluster.example.org"
```

In general about 1MB of local disk space is required per TSDB block stored in the object storage bucket.

## Deployment