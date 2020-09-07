local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  query+:: {
    jobPrefix: error 'must provide job prefix for Thanos Query dashboard',
    selector: error 'must provide selector for Thanos Query dashboard',
    title: error 'must provide title for Thanos Query dashboard',
    namespaceLabel: error 'must provide namespace label', 
  },
  grafanaDashboards+:: {
    'thanos-query.json':
      g.dashboard(thanos.query.title)
      .addRow(
        g.row('Instant Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query for the given time.') +
          g.httpQpsPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query"' % thanos.query)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
          g.httpErrPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query"' % thanos.query)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query"' % thanos.query)
        )
      )
      .addRow(
        g.row('Range Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query_range for the given time range.') +
          g.httpQpsPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"' % thanos.query)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
          g.httpErrPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"' % thanos.query)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"' % thanos.query)
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="unary"' % thanos.query)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="unary"' % thanos.query)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles.') +
          g.grpcLatencyPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="unary"' % thanos.query)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="server_stream"' % thanos.query)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="server_stream"' % thanos.query)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles') +
          g.grpcLatencyPanel('client', '%(namespaceLabel)s="$namespace",%(selector)s,grpc_type="server_stream"' % thanos.query)
        )
      )
      .addRow(
        g.row('DNS')
        .addPanel(
          g.panel('Rate', 'Shows rate of DNS lookups to discover stores.') +
          g.queryPanel(
            'sum(rate(thanos_querier_store_apis_dns_lookups_total{%(namespaceLabel)s="$namespace",%(selector)s}[$interval])) by (job)' % thanos.query,
            'lookups {{job}}'
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of failures compared to the the total number of executed DNS lookups.') +
          g.qpsErrTotalPanel(
            'thanos_querier_store_apis_dns_failures_total{%(namespaceLabel)s="$namespace",%(selector)s}' % thanos.query,
            'thanos_querier_store_apis_dns_lookups_total{%(namespaceLabel)s="$namespace",%(selector)s}' % thanos.query,
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', thanos.dashboard.namespaceMetric) +
      g.template('job', 'up', '%(namespaceLabel)s="$namespace",%(selector)s' % thanos.query, true, '%(jobPrefix)s.*' % thanos.query) +
      g.template('pod', 'kube_pod_info', '%(namespaceLabel)s="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.query, true, '.*'),

    __overviewRows__+:: [
      g.row('Instant Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query for the given time.') +
        g.httpQpsPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query"' % thanos.query) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
        g.httpErrPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query"' % thanos.query) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%(namespaceLabel)s="$namespace",%(selector)s,handler="query"}' % thanos.query,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),

      g.row('Range Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query_range for the given time range.') +
        g.httpQpsPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"' % thanos.query) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
        g.httpErrPanel('http_requests_total', '%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"' % thanos.query) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%(namespaceLabel)s="$namespace",%(selector)s,handler="query_range"}' % thanos.query,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),
    ],
  },
}
