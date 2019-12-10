{
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-query.rules',
        rules: [
          {
            record: ':grpc_client_failures_per_unary:sum_rate',
            expr: |||
              (
                sum(rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerySelector)s, grpc_type="unary"}[5m]))
              /
                sum(rate(grpc_client_started_total{%(thanosQuerySelector)s, grpc_type="unary"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':grpc_client_failures_per_stream:sum_rate',
            expr: |||
              (
                sum(rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerySelector)s, grpc_type="server_stream"}[5m]))
              /
                sum(rate(grpc_client_started_total{%(thanosQuerySelector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':thanos_querier_store_apis_dns_failures_per_lookup:sum_rate',
            expr: |||
              (
                sum(rate(thanos_querier_store_apis_dns_failures_total{%(thanosQuerySelector)s}[5m]))
              /
                sum(rate(thanos_querier_store_apis_dns_lookups_total{%(thanosQuerySelector)s}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':query_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(http_request_duration_seconds_bucket{%(thanosQuerySelector)s, handler="query"}[5m])) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':api_range_query_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(http_request_duration_seconds_bucket{%(thanosQuerySelector)s, handler="query_range"}[5m])) by (le)
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
