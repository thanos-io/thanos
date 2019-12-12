# Alerts

Here are some example alerts configured for Kubernetes environment.

## Compaction

[embedmd]:# (../tmp/thanos-compact.rules.yaml yaml)
```yaml
name: thanos-compact.rules
rules:
- alert: ThanosCompactMultipleCompactsAreRunning
  annotations:
    message: You should never run more than one Thanos Compact at once. You have {{
      $value }}
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

[//]: # "TODO(kakkoyun): Generate rule rules using thanos-mixin."
<!-- [embedmd]:# (../tmp/thanos-rule.rules.yaml yaml) -->
```yaml
- alert: ThanosRuleIsDown
  expr: up{app="thanos-rule"} == 0 or absent(up{app="thanos-rule"})
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Rule is down
    impact: Alerts are not working
    action: 'check {{ $labels.kubernetes_pod_name }} pod in {{ $labels.kubernetes_namespace}} namespace'
    dashboard: RULE_DASHBOARD
- alert: ThanosRuleIsDroppingAlerts
  expr: rate(thanos_alert_queue_alerts_dropped_total{app="thanos-rule"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Rule is dropping alerts
    impact: Alerts are not working
    action: 'check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace'
    dashboard: RULE_DASHBOARD
- alert: ThanosRuleGrpcErrorRate
  expr: rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable",app="thanos-rule"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Rule is returning Internal/Unavailable errors
    impact: Recording Rules are not working
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: RULE_DASHBOARD
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
      histogram_quantile(0.9, sum by (job, le) (thanos_bucket_store_series_gate_duration_seconds_bucket{job=~"thanos-store.*"})) > 2
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
      histogram_quantile(0.9, sum by (job, le) (thanos_objstore_bucket_operation_duration_seconds_bucket{job=~"thanos-store.*"})) > 15
    and
      sum by (job) (rate(thanos_objstore_bucket_operation_duration_seconds_count{job=~"thanos-store.*"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: warning
```

## Sidecar

[//]: # "TODO(kakkoyun): Generate sidecar rules using thanos-mixin."
<!-- [embedmd]:# (../tmp/thanos-sidecar.rules.yaml yaml) -->
```yaml
- alert: ThanosSidecarPrometheusDown
  expr: thanos_sidecar_prometheus_up{name="prometheus"} == 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Sidecar cannot connect to Prometheus
    impact: Prometheus configuration is not being refreshed
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: SIDECAR_URL
- alert: ThanosSidecarBucketOperationsFailed
  expr: rate(thanos_objstore_bucket_operation_failures_total{name="prometheus"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Sidecar bucket operations are failing
    impact: We will lose metrics data if not fixed in 24h
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: SIDECAR_URL
- alert: ThanosSidecarGrpcErrorRate
  expr: rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable",name="prometheus"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Sidecar is returning Internal/Unavailable errors
    impact: Prometheus queries are failing
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: SIDECAR_URL
```

## Query

[embedmd]:# (../tmp/thanos-querier.rules.yaml yaml)
```yaml
name: thanos-querier.rules
rules:
- alert: ThanosQuerierHttpRequestQueryErrorRateHigh
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of "query" requests.
  expr: |
    (
      sum(rate(http_requests_total{code=~"5..", job=~"thanos-querier.*", handler="query"}[5m]))
    /
      sum(rate(http_requests_total{job=~"thanos-querier.*", handler="query"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: critical
- alert: ThanosQuerierHttpRequestQueryRangeErrorRateHigh
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of "query_range" requests.
  expr: |
    (
      sum(rate(http_requests_total{code=~"5..", job=~"thanos-querier.*", handler="query_range"}[5m]))
    /
      sum(rate(http_requests_total{job=~"thanos-querier.*", handler="query_range"}[5m]))
    ) * 100 > 5
  for: 5m
  labels:
    severity: critical
- alert: ThanosQuerierGrpcServerErrorRate
  annotations:
    message: Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", job=~"thanos-querier.*"}[5m]))
    /
      sum by (job) (rate(grpc_server_started_total{job=~"thanos-querier.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosQuerierGrpcClientErrorRate
  annotations:
    message: Thanos Query {{$labels.job}} is failing to send {{ $value | humanize
      }}% of requests.
  expr: |
    (
      sum by (job) (rate(grpc_client_handled_total{grpc_code!="OK", job=~"thanos-querier.*"}[5m]))
    /
      sum by (job) (rate(grpc_client_started_total{job=~"thanos-querier.*"}[5m]))
    * 100 > 5
    )
  for: 5m
  labels:
    severity: warning
- alert: ThanosQuerierHighDNSFailures
  annotations:
    message: Thanos Querys {{$labels.job}} have {{ $value }} of failing DNS queries.
  expr: |
    (
      sum by (job) (rate(thanos_query_store_apis_dns_failures_total{job=~"thanos-querier.*"}[5m]))
    /
      sum by (job) (rate(thanos_query_store_apis_dns_lookups_total{job=~"thanos-querier.*"}[5m]))
    > 1
    )
  for: 15m
  labels:
    severity: warning
- alert: ThanosQuerierInstantLatencyHigh
  annotations:
    message: Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for instant queries.
  expr: |
    (
      histogram_quantile(0.99, sum by (job, le) (http_request_duration_seconds_bucket{job=~"thanos-querier.*", handler="query"})) > 10
    and
      sum by (job) (rate(http_request_duration_seconds_bucket{job=~"thanos-querier.*", handler="query"}[5m])) > 0
    )
  for: 10m
  labels:
    severity: critical
- alert: ThanosQuerierRangeLatencyHigh
  annotations:
    message: Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value
      }} seconds for instant queries.
  expr: |
    (
      histogram_quantile(0.99, sum by (job, le) (http_request_duration_seconds_bucket{job=~"thanos-querier.*", handler="query_range"})) > 10
    and
      sum by (job) (rate(http_request_duration_seconds_count{job=~"thanos-querier.*", handler="query_range"}[5m])) > 0
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
      histogram_quantile(0.99, sum by (job, le) (http_request_duration_seconds_bucket{job=~"thanos-receive.*", handler="receive"})) > 10
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
    severity: critical
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
- alert: ThanosQuerierIsDown
  annotations:
    message: ThanosQuerier has disappeared from Prometheus target discovery.
  expr: |
    absent(up{job=~"thanos-querier.*"} == 1)
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
