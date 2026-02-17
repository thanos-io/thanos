### General Metrics

| Metric Name                               | Type    | Description                                                                                                                                                |
|-------------------------------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| thanos_build_info                         | gauge   | A metric with a constant '1' value labeled by version, revision, branch, goversion from which thanos was built, and the `goos` and `goarch` for the build. |
| thanos_status                             | gauge   | Represents status (0 indicates failure, 1 indicates success) of the component.                                                                             |
| thanos_consistency_delay_seconds          | gauge   | Configured consistency delay in seconds.                                                                                                                   |
| thanos_delete_delay_seconds               | gauge   | Configured delete delay in seconds.                                                                                                                        |
| thanos_proxy_store_empty_stream_responses | counter | Total number of empty responses received.                                                                                                                  |

### Metrics Exported By Store API

| Metric Name                             | Type      | Description                                                |
|-----------------------------------------|-----------|------------------------------------------------------------|
| thanos_store_api_query_duration_seconds | histogram | Duration of the Thanos Store API select phase for a query. |

### Metrics Exported By Compactor, Receiver, Ruler, Sidecar, and Store Gateway For Object Storage

| Metric Name                                          | Type      | Description                                                                                                                                                          |
|------------------------------------------------------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| thanos_objstore_bucket_last_successful_upload_time   | gauge     | Second timestamp of the last successful upload to the bucket.                                                                                                        |
| thanos_objstore_bucket_operation_duration_seconds    | histogram | Duration of successful operations against the bucket.                                                                                                                |
| thanos_objstore_bucket_operation_failures_total      | counter   | Total number of operations against a bucket that failed, but were not expected to fail in certain way from caller perspective. Those errors have to be investigated. |
| thanos_objstore_bucket_operations_total              | counter   | Total number of all attempted operations against a bucket.                                                                                                           |
| thanos_objstore_bucket_operation_fetched_bytes_total | counter   | Total number of bytes fetched from bucket, per operation.                                                                                                            |
| thanos_objstore_bucket_operation_transferred_bytes   | histogram | Number of bytes transferred from/to bucket per operation.                                                                                                            |
