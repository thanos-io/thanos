{
  local thanos = self,
  store+:: {
    selector: error 'must provide selector for Thanos Store recording rules',
  },
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-store.rules',
        rules: [
          {
            record: ':grpc_server_failures_per_unary:sum_rate',
            expr: |||
              (
                sum(rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="unary"}[5m]))
              /
                sum(rate(grpc_server_started_total{%(selector)s, grpc_type="unary"}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':grpc_server_failures_per_stream:sum_rate',
            expr: |||
              (
                sum(rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="server_stream"}[5m]))
              /
                sum(rate(grpc_server_started_total{%(selector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':thanos_objstore_bucket_failures_per_operation:sum_rate',
            expr: |||
              (
                sum(rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum(rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':thanos_objstore_bucket_operation_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(thanos_objstore_bucket_operation_duration_seconds_bucket{%(selector)s}[5m])) by (le)
              )
            ||| % thanos.store,
            labels: {
              quantile: '0.99',
            },
          },
        ],
      },
    ],
  },
}
