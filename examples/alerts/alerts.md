# Alerts

Here are some example alerts configured for Kubernetes environment.

## Compaction

[embedmd]:# (../tmp/thanos-compact.rules.yaml yaml)
```yaml
name: thanos-compact.rules
rules:
- alert: ThanosCompactMultipleRunning
  annotations:
    message: No more than one Thanos Compact instance should be running at once. There
      are {{ $value }}
  expr: sum(up{job=~"thanos-compact.*"}) > 1
  for: 5m
  labels:
    severity: warning
- alert: ThanosCompactHalted
  annotations:
    message: Thanos Compact {{$labels.job}} has failed to run and now is halted.
  expr: thanos_compactor_halted{job=~"thanos-compact.*"} == 1
  for: 5m
  labels:
    severity: warning
- alert: ThanosCompactHighCompactionFailures
  annotations:
    message: Thanos Compact {{$labels.job}} is failing to execute {{ $value | humanize
      }}% of compactions.
  expr: |
    (
      sum by (job) (rate(thanos_compact_group_compactions_failures_total{job=~"thanos-compact.*"}[5m]))
    /
      sum by (job) (rate(thanos_compact_group_compactions_total{job=~"thanos-compact.*"}[5m]))
    * 100 > 5
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosCompactBucketHighOperationFailures
  annotations:
    message: Thanos Compact {{$labels.job}} Bucket is failing to execute {{ $value
      | humanize }}% of operations.
  expr: |
    (
      sum by (job) (rate(thanos_objstore_bucket_operation_failures_total{job=~"thanos-compact.*"}[5m]))
    /
      sum by (job) (rate(thanos_objstore_bucket_operations_total{job=~"thanos-compact.*"}[5m]))
    * 100 > 5
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosCompactHasNotRun
  annotations:
    message: Thanos Compact {{$labels.job}} has not uploaded anything for 24 hours.
  expr: (time() - max(thanos_objstore_bucket_last_successful_upload_time{job=~"thanos-compact.*"}))
    / 60 / 60 > 24
  labels:
    severity: warning
```

## Ruler

For Thanos ruler we run some alerts in local Prometheus, to make sure that Thanos Rule is working:

[embedmd]:# (../tmp/thanos-rule.rules.yaml yaml)
```yaml
name: thanos-rule.rules
rules:
- alert: ThanosRuleQueueIsDroppingAlerts
  annotations:
    message: Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to queue alerts.
  expr: |
    sum by (job) (rate(thanos_alert_queue_alerts_dropped_total{job=~"thanos-rule.*"}[5m])) > 0
  for: 5m
  labels:
    severity: critical
- alert: ThanosRuleSenderIsFailingAlerts
  annotations:
    message: Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to send alerts
      to alertmanager.
  expr: |
    sum by (job) (rate(thanos_alert_sender_alerts_dropped_total{job=~"thanos-rule.*"}[5m])) > 0
  for: 5m
  labels:
    severity: critical
- alert: ThanosRuleHighRuleEvaluationFailures
  annotations:
    message: Thanos Rule {{$labels.job}} {{$labels.pod}} is failing to evaluate rules.
  expr: |
    (
      sum by (job) (rate(prometheus_rule_evaluation_failures_total{job=~"thanos-rule.*"}[5m]))
    /
      sum by (job) (rate(prometheus_rule_evaluations_total{job=~"thanos-rule.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosRuleHighRuleEvaluationWarnings
  annotations:
    message: Thanos Rule {{$labels.job}} {{$labels.pod}} has high number of evaluation
      warnings.
  expr: |
    sum by (job) (rate(thanos_rule_evaluation_with_warnings_total{job=~"thanos-rule.*"}[5m])) > 0
  for: 15m
  labels:
    severity: info
- alert: ThanosRuleRuleEvaluationLatencyHigh
  annotations:
    message: Thanos Rule {{$labels.job}}/{{$labels.pod}} has higher evaluation latency
      than interval for {{$labels.rule_group}}.
  expr: |
    (
      sum by (job, pod, rule_group) (prometheus_rule_group_last_duration_seconds{job=~"thanos-rule.*"})
    >
      sum by (job, pod, rule_group) (prometheus_rule_group_interval_seconds{job=~"thanos-rule.*"})
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosRuleGrpcErrorRate
  annotations:
    message: Thanos Rule {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", job=~"thanos-rule.*"}[5m]))
    /
      sum by (job) (rate(grpc_server_started_total{job=~"thanos-rule.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosRuleConfigReloadFailure
  annotations:
    message: Thanos Rule {{$labels.job}} has not been able to reload its configuration.
  expr: avg(thanos_rule_config_last_reload_successful{job=~"thanos-rule.*"}) by (job)
    != 1
  for: 5m
  labels:
    severity: info
- alert: ThanosRuleQueryHighDNSFailures
  annotations:
    message: Thanos Rule {{$labels.job}} have {{ $value | humanize }}% of failing
      DNS queries for query endpoints.
  expr: |
    (
      sum by (job) (rate(thanos_ruler_query_apis_dns_failures_total{job=~"thanos-rule.*"}[5m]))
    /
      sum by (job) (rate(thanos_ruler_query_apis_dns_lookups_total{job=~"thanos-rule.*"}[5m]))
    * 100 > 1
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosRuleAlertmanagerHighDNSFailures
  annotations:
    message: Thanos Rule {{$labels.job}} have {{ $value | humanize }}% of failing
      DNS queries for Alertmanager endpoints.
  expr: |
    (
      sum by (job) (rate(thanos_ruler_alertmanagers_dns_failures_total{job=~"thanos-rule.*"}[5m]))
    /
      sum by (job) (rate(thanos_ruler_alertmanagers_dns_lookups_total{job=~"thanos-rule.*"}[5m]))
    * 100 > 1
    )
  for: 15m
  labels:
    severity: warning
```

## Store Gateway

[embedmd]:# (../tmp/thanos-store.rules.yaml yaml)
```yaml
name: thanos-store.rules
rules:
- alert: ThanosStoreGrpcErrorRate
  annotations:
    message: Thanos Store {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", job=~"thanos-store.*"}[5m]))
    /
      sum by (job) (rate(grpc_server_started_total{job=~"thanos-store.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosStoreSeriesGateLatencyHigh
  annotations:
    message: Thanos Store {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for store series gate requests.
  expr: |
    (
      histogram_quantile(0.9, sum by (job, le) (rate(thanos_bucket_store_series_gate_duration_seconds_bucket{job=~"thanos-store.*"}[5m]))) > 2
    and
      sum by (job) (rate(thanos_bucket_store_series_gate_duration_seconds_count{job=~"thanos-store.*"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: warning
- alert: ThanosStoreBucketHighOperationFailures
  annotations:
    message: Thanos Store {{$labels.job}} Bucket is failing to execute {{ $value |
      humanize }}% of operations.
  expr: |
    (
      sum by (job) (rate(thanos_objstore_bucket_operation_failures_total{job=~"thanos-store.*"}[5m]))
    /
      sum by (job) (rate(thanos_objstore_bucket_operations_total{job=~"thanos-store.*"}[5m]))
    * 100 > 5
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosStoreObjstoreOperationLatencyHigh
  annotations:
    message: Thanos Store {{$labels.job}} Bucket has a 99th percentile latency of
      {{ $value }} seconds for the bucket operations.
  expr: |
    (
      histogram_quantile(0.9, sum by (job, le) (rate(thanos_objstore_bucket_operation_duration_seconds_bucket{job=~"thanos-store.*"}[5m]))) > 2
    and
      sum by (job) (rate(thanos_objstore_bucket_operation_duration_seconds_count{job=~"thanos-store.*"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: warning
```

## Sidecar

[embedmd]:# (../tmp/thanos-sidecar.rules.yaml yaml)
```yaml
name: thanos-sidecar.rules
rules:
- alert: ThanosSidecarPrometheusDown
  annotations:
    message: Thanos Sidecar {{$labels.job}} {{$labels.pod}} cannot connect to Prometheus.
  expr: |
    sum by (job, pod) (thanos_sidecar_prometheus_up{job=~"thanos-sidecar.*"} == 0)
  for: 5m
  labels:
    severity: critical
- alert: ThanosSidecarUnhealthy
  annotations:
    message: Thanos Sidecar {{$labels.job}} {{$labels.pod}} is unhealthy for {{ $value
      }} seconds.
  expr: |
    count(time() - max(thanos_sidecar_last_heartbeat_success_time_seconds{job=~"thanos-sidecar.*"}) by (job, pod) >= 300) > 0
  labels:
    severity: critical
```

## Query

[embedmd]:# (../tmp/thanos-query.rules.yaml yaml)
```yaml
name: thanos-query.rules
rules:
- alert: ThanosQueryHttpRequestQueryErrorRateHigh
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of "query" requests.
  expr: |
    (
      sum(rate(http_requests_total{code=~"5..", job=~"thanos-query.*", handler="query"}[5m]))
    /
      sum(rate(http_requests_total{job=~"thanos-query.*", handler="query"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: critical
- alert: ThanosQueryHttpRequestQueryRangeErrorRateHigh
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of "query_range" requests.
  expr: |
    (
      sum(rate(http_requests_total{code=~"5..", job=~"thanos-query.*", handler="query_range"}[5m]))
    /
      sum(rate(http_requests_total{job=~"thanos-query.*", handler="query_range"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: critical
- alert: ThanosQueryGrpcServerErrorRate
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", job=~"thanos-query.*"}[5m]))
    /
      sum by (job) (rate(grpc_server_started_total{job=~"thanos-query.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosQueryGrpcClientErrorRate
  annotations:
    message: Thanos Query {{$labels.job}} is failing to send {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_client_handled_total{grpc_code!="OK", job=~"thanos-query.*"}[5m]))
    /
      sum by (job) (rate(grpc_client_started_total{job=~"thanos-query.*"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: warning
- alert: ThanosQueryHighDNSFailures
  annotations:
    message: Thanos Query {{$labels.job}} have {{ $value | humanize }}% of failing
      DNS queries for store endpoints.
  expr: |
    (
      sum by (job) (rate(thanos_querier_store_apis_dns_failures_total{job=~"thanos-query.*"}[5m]))
    /
      sum by (job) (rate(thanos_querier_store_apis_dns_lookups_total{job=~"thanos-query.*"}[5m]))
    ) * 100 > 1
  for: 15m
  labels:
    severity: warning
- alert: ThanosQueryInstantLatencyHigh
  annotations:
    message: Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for instant queries.
  expr: |
    (
      histogram_quantile(0.99, sum by (job, le) (rate(http_request_duration_seconds_bucket{job=~"thanos-query.*", handler="query"}[5m]))) > 40
    and
      sum by (job) (rate(http_request_duration_seconds_bucket{job=~"thanos-query.*", handler="query"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: critical
- alert: ThanosQueryRangeLatencyHigh
  annotations:
    message: Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for range queries.
  expr: |
    (
      histogram_quantile(0.99, sum by (job, le) (rate(http_request_duration_seconds_bucket{job=~"thanos-query.*", handler="query_range"}[5m]))) > 90
    and
      sum by (job) (rate(http_request_duration_seconds_count{job=~"thanos-query.*", handler="query_range"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: critical
```

## Receive

[embedmd]:# (../tmp/thanos-receive.rules.yaml yaml)
```yaml
name: thanos-receive.rules
rules:
- alert: ThanosReceiveHttpRequestErrorRateHigh
  annotations:
    message: Thanos Receive {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum(rate(http_requests_total{code=~"5..", job=~"thanos-receive.*", handler="receive"}[5m]))
    /
      sum(rate(http_requests_total{job=~"thanos-receive.*", handler="receive"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: critical
- alert: ThanosReceiveHttpRequestLatencyHigh
  annotations:
    message: Thanos Receive {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for requests.
  expr: |
    (
      histogram_quantile(0.99, sum by (job, le) (rate(http_request_duration_seconds_bucket{job=~"thanos-receive.*", handler="receive"}[5m]))) > 10
    and
      sum by (job) (rate(http_request_duration_seconds_count{job=~"thanos-receive.*", handler="receive"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: critical
- alert: ThanosReceiveHighForwardRequestFailures
  annotations:
    message: Thanos Receive {{$labels.job}} is failing to forward {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(thanos_receive_forward_requests_total{result="error", job=~"thanos-receive.*"}[5m]))
    /
      sum by (job) (rate(thanos_receive_forward_requests_total{job=~"thanos-receive.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosReceiveHighHashringFileRefreshFailures
  annotations:
    message: Thanos Receive {{$labels.job}} is failing to refresh hashring file, {{
      $value | humanize }} of attempts failed.
  expr: |
    (
      sum by (job) (rate(thanos_receive_hashrings_file_errors_total{job=~"thanos-receive.*"}[5m]))
    /
      sum by (job) (rate(thanos_receive_hashrings_file_refreshes_total{job=~"thanos-receive.*"}[5m]))
    > 0
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosReceiveConfigReloadFailure
  annotations:
    message: Thanos Receive {{$labels.job}} has not been able to reload hashring configurations.
  expr: avg(thanos_receive_config_last_reload_successful{job=~"thanos-receive.*"})
    by (job) != 1
  for: 5m
  labels:
    severity: warning
```

## Replicate

[embedmd]:# (../tmp/thanos-bucket-replicate.rules.yaml yaml)
```yaml
name: thanos-bucket-replicate.rules
rules:
- alert: ThanosBucketReplicateIsDown
  annotations:
    message: Thanos Replicate has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-bucket-replicate.*"})
  for: 5m
  labels:
    severity: critical
- alert: ThanosBucketReplicateErrorRate
  annotations:
    message: Thanos Replicate failing to run, {{ $value | humanize }}% of attempts
      failed.
  expr: |
    (
      sum(rate(thanos_replicate_replication_runs_total{result="error", job=~"thanos-bucket-replicate.*"}[5m]))
    / on (namespace) group_left
      sum(rate(thanos_replicate_replication_runs_total{job=~"thanos-bucket-replicate.*"}[5m]))
    ) * 100 >= 10
  for: 5m
  labels:
    severity: critical
- alert: ThanosBucketReplicateRunLatency
  annotations:
    message: Thanos Replicate {{$labels.job}} has a 99th percentile latency of {{
      $value }} seconds for the replicate operations.
  expr: |
    (
      histogram_quantile(0.9, sum by (job, le) (rate(thanos_replicate_replication_run_duration_seconds_bucket{job=~"thanos-bucket-replicate.*"}[5m]))) > 20
    and
      sum by (job) (rate(thanos_replicate_replication_run_duration_seconds_bucket{job=~"thanos-bucket-replicate.*"}[5m])) > 0
    )
  for: 5m
  labels:
    severity: critical
```

## Extras

### Absent Rules

[embedmd]:# (../tmp/thanos-component-absent.rules.yaml yaml)
```yaml
name: thanos-component-absent.rules
rules:
- alert: ThanosCompactIsDown
  annotations:
    message: ThanosCompact has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-compact.*"} == 1)
  for: 5m
  labels:
    severity: critical
- alert: ThanosQueryIsDown
  annotations:
    message: ThanosQuery has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-query.*"} == 1)
  for: 5m
  labels:
    severity: critical
- alert: ThanosReceiveIsDown
  annotations:
    message: ThanosReceive has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-receive.*"} == 1)
  for: 5m
  labels:
    severity: critical
- alert: ThanosRuleIsDown
  annotations:
    message: ThanosRule has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-rule.*"} == 1)
  for: 5m
  labels:
    severity: critical
- alert: ThanosSidecarIsDown
  annotations:
    message: ThanosSidecar has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-sidecar.*"} == 1)
  for: 5m
  labels:
    severity: critical
- alert: ThanosStoreIsDown
  annotations:
    message: ThanosStore has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-store.*"} == 1)
  for: 5m
  labels:
    severity: critical
```
