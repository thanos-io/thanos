{
  local thanos = self,
  query+:: {
    selector: error 'must provide selector for Thanos Query recording rules',
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job']),
  },
  prometheusRules+:: {
    groups+: if thanos.query == null then [] else [
      {
        name: 'thanos-query.rules',
        rules: [
          {
            record: ':grpc_client_failures_per_unary:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="unary"}[5m]))
              /
                sum by (%(dimensions)s) (rate(grpc_client_started_total{%(selector)s, grpc_type="unary"}[5m]))
              )
            ||| % thanos.query,
          },
          {
            record: ':grpc_client_failures_per_stream:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s, grpc_type="server_stream"}[5m]))
              /
                sum by (%(dimensions)s) (rate(grpc_client_started_total{%(selector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % thanos.query,
          },
          {
            record: ':thanos_query_store_apis_dns_failures_per_lookup:sum_rate',
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_query_store_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_query_store_apis_dns_lookups_total{%(selector)s}[5m]))
              )
            ||| % thanos.query,
          },
          {
            record: ':query_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum by (%(dimensions)s, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query"}[5m]))
              )
            ||| % thanos.query,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':api_range_query_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum by (%(dimensions)s, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query_range"}[5m]))
              )
            ||| % thanos.query,
            labels: {
              quantile: '0.99',
            },
          },
        ],
      },
    ],
  },
}
