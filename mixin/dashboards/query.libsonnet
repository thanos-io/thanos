local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  query+:: {
    selector: error 'must provide selector for Thanos Query dashboard',
    title: error 'must provide title for Thanos Query dashboard',
  },
  grafanaDashboards+:: {
    local selector = std.join(', ', thanos.dashboard.commonSelector + ['job="$job"']),
    local aggregator = std.join(', ', thanos.dashboard.commonSelector + ['job']),

    [if thanos.query != null then 'query.json']:
      g.dashboard(thanos.query.title)
      .addRow(
        g.row('Instant Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query for the given time.') +
          g.httpQpsPanel('http_requests_total', '%s, handler="query"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
          g.httpErrPanel('http_requests_total', '%s, handler="query"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%s, handler="query"' % selector, aggregator)
        )
      )
      .addRow(
        g.row('Range Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query_range for the given time range.') +
          g.httpQpsPanel('http_requests_total', '%s, handler="query_range"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
          g.httpErrPanel('http_requests_total', '%s, handler="query_range"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%s, handler="query_range"' % selector, aggregator)
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%s, grpc_type="unary"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%s, grpc_type="unary"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles.') +
          g.grpcLatencyPanel('client', '%s, grpc_type="unary"' % selector, aggregator)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%s, grpc_type="server_stream"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%s, grpc_type="server_stream"' % selector, aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles') +
          g.grpcLatencyPanel('client', '%s, grpc_type="server_stream"' % selector, aggregator)
        )
      )
      .addRow(
        g.row('DNS')
        .addPanel(
          g.panel('Rate', 'Shows rate of DNS lookups to discover stores.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_query_store_apis_dns_lookups_total{%s}[$interval]))' % [aggregator, selector],
            'lookups {{job}}'
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of failures compared to the the total number of executed DNS lookups.') +
          g.qpsErrTotalPanel(
            'thanos_query_store_apis_dns_failures_total{%s}' % selector,
            'thanos_query_store_apis_dns_lookups_total{%s}' % selector,
            aggregator
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow(selector)
      ),

    __overviewRows__+:: [
      g.row('Instant Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query for the given time.') +
        g.httpQpsPanel('http_requests_total', '%s, handler="query"' % selector, aggregator) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
        g.httpErrPanel('http_requests_total', '%s, handler="query"' % selector, aggregator) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%s, handler="query"}' % selector,
          aggregator,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),

      g.row('Range Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query_range for the given time range.') +
        g.httpQpsPanel('http_requests_total', '%s, handler="query_range"' % selector, aggregator) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
        g.httpErrPanel('http_requests_total', '%s, handler="query_range"' % selector, aggregator) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%s, handler="query_range"}' % selector,
          aggregator,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),
    ],
  },
}
