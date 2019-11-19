{
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-querier.rules',
        rules: [
          {
            record: ':grpc_client_failures_per_unary:sum_rate',
            expr: |||
              (
                sum(rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerierSelector)s, grpc_type="unary"}[5m]))
              /
                sum(rate(grpc_client_started_total{%(thanosQuerierSelector)s, grpc_type="unary"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':grpc_client_failures_per_stream:sum_rate',
            expr: |||
              (
                sum(rate(grpc_client_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerierSelector)s, grpc_type="server_stream"}[5m]))
              /
                sum(rate(grpc_client_started_total{%(thanosQuerierSelector)s, grpc_type="server_stream"}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':thanos_querier_store_apis_dns_failures_per_lookup:sum_rate',
            expr: |||
              (
                sum(rate(thanos_querier_store_apis_dns_failures_total{%(thanosQuerierSelector)s}[5m]))
              /
                sum(rate(thanos_querier_store_apis_dns_lookups_total{%(thanosQuerierSelector)s}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':query_duration_seconds:p99:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(http_request_duration_seconds_bucket{%(thanosQuerierSelector)s, handler="query"}) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':query_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(http_request_duration_seconds_bucket{%(thanosQuerierSelector)s, handler="query"}[5m])) by (le)
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
                sum(http_request_duration_seconds_bucket{%(thanosQuerierSelector)s, handler="query_range"}) by (le)
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
                sum(rate(http_request_duration_seconds_bucket{%(thanosQuerierSelector)s, handler="query_range"}[5m])) by (le)
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
