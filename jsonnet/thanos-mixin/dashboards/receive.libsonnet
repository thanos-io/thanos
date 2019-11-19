local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  grafanaDashboards+:: {
    'receive.json':
      g.dashboard($._config.grafanaThanos.dashboardReceiveTitle)
      .addRow(
        g.row('Incoming Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of incoming requests.') +
          g.httpQpsPanel('http_requests_total', 'handler="receive",namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
          g.httpErrPanel('http_requests_total', 'handler="receive",namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle incoming requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', 'handler="receive",namespace="$namespace",job=~"$job"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of incoming requests.') +
          g.httpQpsPanelDetailed('http_requests_total', 'handler="receive",namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
          g.httpErrDetailsPanel('http_requests_total', 'handler="receive",namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle incoming requests in quantiles.') +
          g.httpLatencyDetailsPanel('http_request_duration_seconds', 'handler="receive",namespace="$namespace",job=~"$job"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('Forward Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of forwarded requests to other receive nodes.') +
          g.queryPanel(
            'sum(rate(thanos_receive_forward_requests_total{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of forwareded requests to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_forward_requests_total{namespace="$namespace",job=~"$job",result="error"}',
            'thanos_receive_forward_requests_total{namespace="$namespace",job=~"$job"}',
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
      )
      .addRow(
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        ) +
        g.collapse
      )
      .addRow(
        g.row('Last Updated')
        .addPanel(
          g.panel('Successful Upload', 'Shows the relative time of last successful upload to the object-store bucket.') +
          g.tablePanel(
            ['time() - max(thanos_objstore_bucket_last_successful_upload_time{namespace="$namespace",job=~"$job"}) by (job, bucket)'],
            {
              Value: {
                alias: 'Uploaded Ago',
                unit: 's',
                type: 'number',
              },
            },
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(thanosReceiveSelector)s' % $._config, true, '%(thanosReceiveJobPrefix)s.*' % $._config) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(thanosReceiveJobPrefix)s.*"' % $._config, true, '.*'),

    __overviewRows__+:: [
      g.row('Receive')
      .addPanel(
        g.panel('Incoming Requests Rate', 'Shows rate of incoming requests.') +
        g.httpQpsPanel('http_requests_total', 'handler="receive",namespace="$namespace",%(thanosReceiveSelector)s' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardReceiveTitle)
      )
      .addPanel(
        g.panel('Incoming Requests Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
        g.httpErrPanel('http_requests_total', 'handler="receive",namespace="$namespace",%(thanosReceiveSelector)s' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardReceiveTitle)
      )
      .addPanel(
        g.sloLatency(
          'Incoming Requests Latency 99th Percentile',
          'Shows how long has it taken to handle incoming requests.',
          'http_request_duration_seconds_bucket{handler="receive",namespace="$namespace",%(thanosReceiveSelector)s}' % $._config,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardReceiveTitle)
      ),
    ],
  },
}
