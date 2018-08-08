# Alerts

Here are some example alerts configured for Kubernetes environment.

## Compaction

```
- alert: ThanosCompactHalted
  expr: thanos_compactor_halted{app="thanos-compact"} == 1
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos compaction has failed to run and now is halted
    impact: Long term storage queries will be slower 
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL
- alert: ThanosCompactCompactionsFailed
  expr: rate(prometheus_tsdb_compactions_failed_total{app="thanos-compact"}[5m]) > 0
  labels:
    team: TEAM
  annotations:
    summary: Thanos Compact is failing compaction
    impact: Long term storage queries will be slower 
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL
- alert: ThanosCompactBucketOperationsFailed
  expr: rate(thanos_objstore_bucket_operation_failures_total{app="thanos-compact"}[5m]) > 0
  labels:
    team: TEAM
  annotations:
    summary: Thanos Compact bucket operations are failing
    impact: Long term storage queries will be slower 
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL
- alert: ThanosCompactNotRunIn24Hours
  expr: (time() - max(thanos_objstore_bucket_last_successful_upload_time{app="thanos-compact"}) ) /60/60 > 24
  labels:
    team: TEAM
  annotations:
    summary: Thanos Compaction has not been run in 24 hours
    impact: Long term storage queries will be slower 
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL
- alert: ThanosComactionIsNotRunning
  expr: up{app="thanos-compact"} == 0 or absent({app="thanos-compact"})
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Compaction is not running
    impact: Long term storage queries will be slower 
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL
- alert: ThanosComactionMultipleCompactionsAreRunning
  expr: sum(up{app="thanos-compact"}) > 1
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Multiple replicas of Thanos compaction shouldn't be running.
    impact: Metrics in long term storage may be corrupted
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: COMPACTION_URL

```

## Ruler

For Thanos ruler we run some alerts in local Prometheus, to make sure that Thanos Rule is working:

```
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

```
- alert: ThanosStoreGrpcErrorRate
  expr: rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable",app="thanos-store"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Store is returning Internal/Unavailable errors 
    impact: Long Term Storage Prometheus queries are failing
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: GATEWAY_URL
- alert: ThanosStoreBucketOperationsFailed
  expr: rate(thanos_objstore_bucket_operation_failures_total{app="thanos-store"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Store is failing to do bucket operations
    impact: Long Term Storage Prometheus queries are failing
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: GATEWAY_URL
```

## Sidecar

```
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

```
- alert: ThanosQueryGrpcErrorRate
  expr: rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable",name="prometheus"}[5m]) > 0
  for: 5m
  labels:
    team: TEAM
  annotations:
    summary: Thanos Query is returning Internal/Unavailable errors 
    impact: Grafana is not showing metrics
    action: Check {{ $labels.kubernetes_pod_name }} pod logs in {{ $labels.kubernetes_namespace}} namespace
    dashboard: QUERY_URL
```
