{
  prometheusRules+:: {
    groups+: [
      {
        name: 'thanos-receive.rules',
        rules: [
          {
            record: ':grpc_server_failures_per_unary:sum_rate',
            expr: |||
              sum(
                rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerierSelector)s, grpc_type="unary"}[5m])
              /
                rate(grpc_server_started_total{%(thanosQuerierSelector)s, grpc_type="unary"}[5m])
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':grpc_server_failures_per_stream:sum_rate',
            expr: |||
              sum(
                rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(thanosQuerierSelector)s, grpc_type="server_stream"}[5m])
              /
                rate(grpc_server_started_total{%(thanosQuerierSelector)s, grpc_type="server_stream"}[5m])
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':http_failure_per_request:sum_rate',
            expr: |||
              sum(
                rate(http_requests_total{handler="receive", %(thanosReceiveSelector)s, code!~"5.."}[5m])
              /
                rate(http_requests_total{handler="receive", %(thanosReceiveSelector)s}[5m])
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':http_request_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(http_request_duration_seconds_bucket{handler="receive", %(thanosReceiveSelector)s}) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':http_request_duration_seconds:histogram_quantile',
            expr: |||
              histogram_quantile(0.99,
                sum(rate(http_request_duration_seconds_bucket{handler="receive", %(thanosReceiveSelector)s}[5m])) by (le)
              )
            ||| % $._config,
            labels: {
              quantile: '0.99',
            },
          },
          {
            record: ':thanos_receive_forward_failure_per_requests:sum_rate',
            expr: |||
              (
                sum(rate(thanos_receive_forward_requests_total{result="error", %(thanosReceiveSelector)s}[5m]))
              /
                sum(rate(thanos_receive_forward_requests_total{%(thanosReceiveSelector)s}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
          {
            record: ':thanos_receive_hashring_file_failure_per_refresh:sum_rate',
            expr: |||
              (
                sum(rate(thanos_receive_hashrings_file_errors_total{%(thanosReceiveSelector)s}[5m]))
              /
                sum(rate(thanos_receive_hashrings_file_refreshes_total{%(thanosReceiveSelector)s}[5m]))
              )
            ||| % $._config,
            labels: {
            },
          },
        ],
      },
    ],
  },
}
