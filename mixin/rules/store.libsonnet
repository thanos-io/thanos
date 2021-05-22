{
  local thanos = self,
  store+:: {
    selector: error 'must provide selector for Thanos Store recording rules',
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job']),
  },
  prometheusRules+:: {
    groups+: if thanos.store == null then [] else [
      {
        name: 'thanos-store.rules',
        rules: [
          {
            record: ':grpc_server_failures_per_unary:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="unary"}[5m]))
              /
                sum by (%(dimensions)s) (rate(grpc_server_started_total{%(selector)s, grpc_type="unary"}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':grpc_server_failures_per_stream:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="server_stream"}[5m]))
              /
                sum by (%(dimensions)s) (rate(grpc_server_started_total{%(selector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':thanos_objstore_bucket_failures_per_operation:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              )
            ||| % thanos.store,
          },
          {
            record: ':thanos_objstore_bucket_operation_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum by (%(dimensions)s, le) (rate(thanos_objstore_bucket_operation_duration_seconds_bucket{%(selector)s}[5m]))
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
