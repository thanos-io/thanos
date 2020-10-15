local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  query+:: {
    jobPrefix: error 'must provide job prefix for Thanos Query dashboard',
    selector: error 'must provide selector for Thanos Query dashboard',
    title: error 'must provide title for Thanos Query dashboard',
  },
  local cluster = '%s="$cluster"' % thanos.dashboard.clusterLabel,
  local namespace = 'namespace="$namespace"',
  local clusterNamespace = '%s,%s' % [cluster, namespace],
  local clusterNamespaceJob = '%s,job=~"$job"' % [clusterNamespace],
  local clusterNamespaceSelector = '%s,%s' % [clusterNamespace, thanos.query.selector],
  grafanaDashboards+:: {
    'query.json':
      g.dashboard(thanos.query.title)
      .addRow(
        g.row('Instant Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query for the given time.') +
          g.httpQpsPanel('http_requests_total', '%s,handler="query"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
          g.httpErrPanel('http_requests_total', '%s,handler="query"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%s,handler="query"' % clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('Range Query API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query_range for the given time range.') +
          g.httpQpsPanel('http_requests_total', '%s,handler="query_range"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
          g.httpErrPanel('http_requests_total', '%s,handler="query_range"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%s,handler="query_range"' % clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%s,grpc_type="unary"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%s,grpc_type="unary"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles.') +
          g.grpcLatencyPanel('client', '%s,grpc_type="unary"' % clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from other queriers.') +
          g.grpcQpsPanel('client', '%s,grpc_type="server_stream"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests from other queriers.') +
          g.grpcErrorsPanel('client', '%s,grpc_type="server_stream"' % clusterNamespaceJob)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from other queriers, in quantiles') +
          g.grpcLatencyPanel('client', '%s,grpc_type="server_stream"' % clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('DNS')
        .addPanel(
          g.panel('Rate', 'Shows rate of DNS lookups to discover stores.') +
          g.queryPanel(
            'sum(rate(thanos_querier_store_apis_dns_lookups_total{%s}[$interval])) by (job)' % clusterNamespaceJob,
            'lookups {{job}}'
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of failures compared to the the total number of executed DNS lookups.') +
          g.qpsErrTotalPanel(
            'thanos_querier_store_apis_dns_failures_total{%s}' % clusterNamespaceJob,
            'thanos_querier_store_apis_dns_lookups_total{%s}' % clusterNamespaceJob,
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template(thanos.dashboard.clusterLabel, 'up', '', false, '', if thanos.dashboard.showMultiCluster then '' else '2') +
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', clusterNamespaceSelector, true, '%(jobPrefix)s.*' % thanos.query) +
      g.template('pod', 'kube_pod_info', '%s,created_by_name=~"%s.*"' % [clusterNamespace, thanos.query.jobPrefix], true, '.*'),

    __overviewRows__+:: [
      g.row('Instant Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query for the given time.') +
        g.httpQpsPanel('http_requests_total', '%s,handler="query"' % clusterNamespaceSelector) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query.') +
        g.httpErrPanel('http_requests_total', '%s,handler="query"' % clusterNamespaceSelector) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%s,handler="query"}' % clusterNamespaceSelector,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),

      g.row('Range Query')
      .addPanel(
        g.panel('Requests Rate', 'Shows rate of requests against /query_range for the given time range.') +
        g.httpQpsPanel('http_requests_total', '%s,handler="query_range"' % clusterNamespaceSelector) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.panel('Requests Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_range.') +
        g.httpErrPanel('http_requests_total', '%s,handler="query_range"' % clusterNamespaceSelector) +
        g.addDashboardLink(thanos.query.title)
      )
      .addPanel(
        g.sloLatency(
          'Latency 99th Percentile',
          'Shows how long has it taken to handle requests.',
          'http_request_duration_seconds_bucket{%s,handler="query_range"}' % clusterNamespaceSelector,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.query.title)
      ),
    ],
  },
}
