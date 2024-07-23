### General Metrics

| Metric Name                                       | Type      | Description                                                                                                                                                |
|---------------------------------------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| thanos_build_info                                 | gauge     | A metric with a constant '1' value labeled by version, revision, branch, goversion from which thanos was built, and the `goos` and `goarch` for the build. |
| thanos_status                                     | gauge     | Represents status (0 indicates failure, 1 indicates success) of the component.                                                                             |
| thanos_proxy_store_empty_stream_responses         | counter   | Total number of empty responses received.                                                                                                                  |
| thanos_consistency_delay_seconds                  | gauge     | Configured consistency delay in seconds.                                                                                                                   |
| thanos_delete_delay_seconds                       | gauge     | Configured delete delay in seconds.                                                                                                                        |
| thanos_replicate_blocks_already_replicated_total  | counter   | Total number of blocks skipped due to already being replicated.                                                                                            |
| thanos_replicate_blocks_replicated_total          | counter   | Total number of blocks replicated.                                                                                                                         |
| thanos_replicate_objects_replicated_total         | counter   | Total number of objects replicated.                                                                                                                        |
| thanos_replicate_replication_runs_total           | counter   | The number of replication runs split by success and error.                                                                                                 |
| thanos_replicate_replication_run_duration_seconds | histogram | The Duration of replication runs split by success and error.                                                                                               |

### Metrics Exported By Store API

| Metric Name                             | Type      | Description                                                |
|-----------------------------------------|-----------|------------------------------------------------------------|
| thanos_store_api_query_duration_seconds | histogram | Duration of the Thanos Store API select phase for a query. |
