{
  local thanos = self,
  query+:: {
    selector: error 'must provide selector for Thanos Query alerts',
    httpErrorThreshold: 5,
    grpcErrorThreshold: 5,
    dnsErrorThreshold: 1,
    p99QueryLatencyThreshold: 40,
    p99QueryRangeLatencyThreshold: 90,
    aggregator: std.join(', ', std.objectFields(thanos.hierarcies) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.query == null then [] else [
      local location = if std.length(std.objectFields(thanos.hierarcies)) > 0 then ' in ' + std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.hierarcies)]) else ' ';
      {
        name: 'thanos-query',
        rules: [
          {
            alert: 'ThanosQueryHttpRequestQueryErrorRateHigh',
            annotations: {
              description: 'Thanos Query {{$labels.job}}%sis failing to handle {{$value | humanize}}%% of "query" requests.' % location,
              summary: 'Thanos Query is failing to handle requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(http_requests_total{code=~"5..", %(selector)s, handler="query"}[5m]))
              /
                sum by (%(aggregator)s) (rate(http_requests_total{%(selector)s, handler="query"}[5m]))
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
              description: 'Thanos Query {{$labels.job}}%sis failing to handle {{$value | humanize}}%% of "query_range" requests.' % location,
              summary: 'Thanos Query is failing to handle requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(http_requests_total{code=~"5..", %(selector)s, handler="query_range"}[5m]))
              /
                sum by (%(aggregator)s) (rate(http_requests_total{%(selector)s, handler="query_range"}[5m]))
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
              description: 'Thanos Query {{$labels.job}}%sis failing to handle {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Query is failing to handle requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(grpc_server_started_total{%(selector)s}[5m]))
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
              description: 'Thanos Query {{$labels.job}}%sis failing to send {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Query is failing to send requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(grpc_client_handled_total{grpc_code!="OK", %(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(grpc_client_started_total{%(selector)s}[5m]))
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
              description: 'Thanos Query {{$labels.job}}%shave {{$value | humanize}}%% of failing DNS queries for store endpoints.' % location,
              summary: 'Thanos Query is having high number of DNS failures.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(thanos_query_store_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(thanos_query_store_apis_dns_lookups_total{%(selector)s}[5m]))
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
              description: 'Thanos Query {{$labels.job}}%shas a 99th percentile latency of {{$value}} seconds for instant queries.' % location,
              summary: 'Thanos Query has high latency for queries.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(aggregator)s, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query"}[5m]))) > %(p99QueryLatencyThreshold)s
              and
                sum by (%(aggregator)s) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query"}[5m])) > 0
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
              description: 'Thanos Query {{$labels.job}}%shas a 99th percentile latency of {{$value}} seconds for range queries.' % location,
              summary: 'Thanos Query has high latency for queries.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(aggregator)s, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="query_range"}[5m]))) > %(p99QueryRangeLatencyThreshold)s
              and
                sum by (%(aggregator)s) (rate(http_request_duration_seconds_count{%(selector)s, handler="query_range"}[5m])) > 0
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
