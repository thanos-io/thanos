# Compact

The compactor component of Thanos applies the compaction procedure of the Prometheus 2.0 storage engine to block data stored in object storage.
It is generally not semantically concurrency safe and must be deployed as a singleton against a bucket.

Example:

```
$ thanos compact --gcs.bucket example-bucket --data-dir /tmp/thanos-compact
```

The compactor needs local disk space to store intermediate data for its processing. Generally, about 100GB are recommended for it to keep working as the compacted time ranges grow over time.
On-disk data is safe to delete between restarts and should be the first attempt to get crash-looping compactors unstuck.

NOTE: The compactor currently restricted to work with Google Cloud Storage buckets.
