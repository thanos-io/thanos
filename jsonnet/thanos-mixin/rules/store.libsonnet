{
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-store.rules',
        rules: [
          {
            record: ':grpc_server_failures_per_unary:sum_rate',
            expr: |||
              (
                sum(rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosStoreSelector)s, grpc_type="unary"}[5m]))
              /
                sum(rate(grpc_server_started_total{%(thanosStoreSelector)s, grpc_type="unary"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':grpc_server_failures_per_stream:sum_rate',
            expr: |||
              (
                sum(rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosStoreSelector)s, grpc_type="server_stream"}[5m]))
              /
                sum(rate(grpc_server_started_total{%(thanosStoreSelector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':thanos_objstore_bucket_failures_per_operation:sum_rate',
            expr: |||
              (
                sum(rate(thanos_objstore_bucket_operation_failures_total{%(thanosStoreSelector)s}[5m]))
              /
                sum(rate(thanos_objstore_bucket_operations_total{%(thanosStoreSelector)s}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':thanos_objstore_bucket_operation_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(thanos_objstore_bucket_operation_duration_seconds_bucket{%(thanosStoreSelector)s}) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':thanos_objstore_bucket_operation_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(thanos_objstore_bucket_operation_duration_seconds_bucket{%(thanosStoreSelector)s}[5m])) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
        ],
      },
    ],
  },
}
