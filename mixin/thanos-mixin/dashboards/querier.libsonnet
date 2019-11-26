local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  grafanaDashboards+:: {
    'querier.json':
      g.dashboard($._config.grafanaThanos.dashboardQuerierTitle)
      .addRow(
        g.row('Instant Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query for the given time.') +
          g.httpQpsPanel('http_requests_total', 'namespace="$namespace",job=~"$job",handler="query"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
          g.httpErrPanel('http_requests_total', 'namespace="$namespace",job=~"$job",handler="query"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', 'namespace="$namespace",job=~"$job",handler="query"')
        )
      )
      .addRow(
        g.row('Range Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query_range for the given time range.') +
          g.httpQpsPanel('http_requests_total', 'namespace="$namespace",job=~"$job",handler="query_range"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
          g.httpErrPanel('http_requests_total', 'namespace="$namespace",job=~"$job",handler="query_range"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', 'namespace="$namespace",job=~"$job",handler="query_range"')
        )
      )
      .addRow(
        g.row('Query Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query for the given time, with handlers and codes.') +
          g.httpQpsPanelDetailed('http_requests_total', 'namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests, in more detail.') +
          g.httpErrDetailsPanel('http_requests_total', 'namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.httpLatencyDetailsPanel('http_request_duration_seconds', 'namespace="$namespace",job=~"$job"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles.') +
          g.grpcLatencyPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests, with grpc methods and codes from other queriers.') +
          g.grpcQpsPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrDetailsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles.') +
          g.grpcLatencyPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles') +
          g.grpcLatencyPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests, with grpc methods and codes.') +
          g.grpcQpsPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests.') +
          g.grpcErrDetailsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.grpcLatencyPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('DNS')
        .addPanel(
          g.panel('Rate', 'Shows rate of DNS lookups to discover stores.') +
          g.queryPanel(
            'sum(rate(thanos_querier_store_apis_dns_lookups_total{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
            'lookups {{job}}'
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of failures compared to the the total number of executed DNS lookups.') +
          g.qpsErrTotalPanel(
            'thanos_querier_store_apis_dns_failures_total{namespace="$namespace",job=~"$job"}',
            'thanos_querier_store_apis_dns_lookups_total{namespace="$namespace",job=~"$job"}',
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(thanosQuerierSelector)s' % $._config, true, '%(thanosQuerierJobPrefix)s.*' % $._config) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(thanosQuerierJobPrefix)s.*"' % $._config, true, '.*'),

    __overviewRows__+:: [
      g.row('Instant Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query for the given time.') +
        g.httpQpsPanel('http_requests_total', 'namespace="$namespace",%(thanosQuerierSelector)s,handler="query"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
        g.httpErrPanel('http_requests_total', 'namespace="$namespace",%(thanosQuerierSelector)s,handler="query"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{namespace="$namespace",%(thanosQuerierSelector)s,handler="query"}' % $._config,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      ),

      g.row('Range Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query_range for the given time range.') +
        g.httpQpsPanel('http_requests_total', 'namespace="$namespace",%(thanosQuerierSelector)s,handler="query_range"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
        g.httpErrPanel('http_requests_total', 'namespace="$namespace",%(thanosQuerierSelector)s,handler="query_range"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{namespace="$namespace",%(thanosQuerierSelector)s,handler="query_range"}' % $._config,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardQuerierTitle)
      ),
    ],
  },
}
