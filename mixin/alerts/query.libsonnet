{
  local thanos = self,
  query+:: {
    selector: error 'must provide selector for Thanos Query alerts',
    httpErrorThreshold: 5,
    grpcErrorThreshold: 5,
    dnsErrorThreshold: 1,
    p99QueryLatencyThreshold: 40,
    p99QueryRangeLatencyThreshold: 90,
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-query.rules',
        rules: [
          {
            alert: 'ThanosQueryHttpRequestQueryErrorRateHigh',
            annotations: {
              message: 'Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize }}% of "query" requests.',
            },
            expr: |||
              (
                sum(rate(http_requests_total{code=~"5..", %(selector)s, handler="query"}[5m]))
              /
                sum(rate(http_requests_total{%(selector)s, handler="query"}[5m]))
              ) * 100 > %(httpErrorThreshold)s
            ||| % thanos.query,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosQueryHttpRequestQueryRangeErrorRateHigh',
            annotations: {
              message: 'Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize }}% of "query_range" requests.',
            },
            expr: |||
              (
                sum(rate(http_requests_total{code=~"5..", %(selector)s, handler="query_range"}[5m]))
              /
                sum(rate(http_requests_total{%(selector)s, handler="query_range"}[5m]))
              ) * 100 > %(httpErrorThreshold)s
            ||| % thanos.query,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosQueryGrpcServerErrorRate',
            annotations: {
              message: 'Thanos Query {{$labels.job}} is failing to handle {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (job) (rate(grpc_server_started_total{%(selector)s}[5m]))
              * 100 > %(grpcErrorThreshold)s
              )
            ||| % thanos.query,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosQueryGrpcClientErrorRate',
            annotations: {
              message: 'Thanos Query {{$labels.job}} is failing to send {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(grpc_client_handled_total{grpc_code!="OK", %(selector)s}[5m]))
              /
                sum by (job) (rate(grpc_client_started_total{%(selector)s}[5m]))
              ) * 100 > %(grpcErrorThreshold)s
            ||| % thanos.query,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosQueryHighDNSFailures',
            annotations: {
              message: 'Thanos Query {{$labels.job}} have {{ $value | humanize }}% of failing DNS queries for store endpoints.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_querier_store_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_querier_store_apis_dns_lookups_total{%(selector)s}[5m]))
              ) * 100 > %(dnsErrorThreshold)s
            ||| % thanos.query,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosQueryInstantLatencyHigh',
            annotations: {
              message: 'Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for instant queries.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (job, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query"}[5m]))) > %(p99QueryLatencyThreshold)s
              and
                sum by (job) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query"}[5m])) > 0
              )
            ||| % thanos.query,
            'for': '10m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosQueryRangeLatencyHigh',
            annotations: {
              message: 'Thanos Query {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for range queries.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (job, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query_range"}[5m]))) > %(p99QueryRangeLatencyThreshold)s
              and
                sum by (job) (rate(http_request_duration_seconds_count{%(selector)s, handler="query_range"}[5m])) > 0
              )
            ||| % thanos.query,
            'for': '10m',
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
