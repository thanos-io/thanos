{
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-store.rules',
        rules: [
          {
            alert: 'ThanosStoreGrpcErrorRate',
            annotations: {
              message: 'Thanos Store {{$labels.job}} is failing to handle {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosStoreSelector)s}[5m]))
              /
                sum by (job) (rate(grpc_server_started_total{%(thanosStoreSelector)s}[5m]))
              * 100 > 5
              )
            ||| % $._config,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreSeriesGateLatencyHigh',
            annotations: {
              message: 'Thanos Store {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for store series gate requests.',
            },
            expr: |||
              (
                histogram_quantile(0.9, sum by (job, le) (thanos_bucket_store_series_gate_duration_seconds_bucket{%(thanosStoreSelector)s})) > 2
              and
                sum by (job) (rate(thanos_bucket_store_series_gate_duration_seconds_count{%(thanosStoreSelector)s}[5m])) > 0
              )
            ||| % $._config,
            'for': '10m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreBucketHighOperationFailures',
            annotations: {
              message: 'Thanos Store {{$labels.job}} Bucket is failing to execute {{ $value | humanize }}% of operations.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_objstore_bucket_operation_failures_total{%(thanosStoreSelector)s}[5m]))
              /
                sum by (job) (rate(thanos_objstore_bucket_operations_total{%(thanosStoreSelector)s}[5m]))
              * 100 > 5
              )
            ||| % $._config,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosStoreObjstoreOperationLatencyHigh',
            annotations: {
              message: 'Thanos Store {{$labels.job}} Bucket has a 99th percentile latency of {{ $value }} seconds for the bucket operations.',
            },
            expr: |||
              (
                histogram_quantile(0.9, sum by (job, le) (thanos_objstore_bucket_operation_duration_seconds_bucket{%(thanosStoreSelector)s})) > 15
              and
                sum by (job) (rate(thanos_objstore_bucket_operation_duration_seconds_count{%(thanosStoreSelector)s}[5m])) > 0
              )
            ||| % $._config,
            'for': '10m',
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
