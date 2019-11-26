local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  grafanaDashboards+:: {
    'sidecar.json':
      g.dashboard($._config.grafanaThanos.dashboardSidecarTitle)
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
          g.panel('Errors') +
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
          g.grpcQpsPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrDetailsPanel('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanelDetailed('client', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
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
        g.row('Bucket Operations')
        .addPanel(
          g.panel('Rate') +
          g.queryPanel(
            'sum(rate(thanos_objstore_bucket_operations_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, operation)',
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors') +
          g.qpsErrTotalPanel(
            'thanos_objstore_bucket_operation_failures_total{namespace="$namespace",job=~"$job"}',
            'thanos_objstore_bucket_operations_total{namespace="$namespace",job=~"$job"}',
          )
        )
        .addPanel(
          g.panel('Duration') +
          g.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', 'namespace="$namespace",job=~"$job"')
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(thanosSidecarSelector)s' % $._config, true, '%(thanosSidecarJobPrefix)s.*' % $._config) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(thanosSidecarJobPrefix)s.*"' % $._config, true, '.*'),

    __overviewRows__+:: [
      g.row('Sidecar')
      .addPanel(
        g.panel('gPRC (Unary) Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
        g.grpcQpsPanel('server', 'namespace="$namespace",%(thanosSidecarSelector)s,grpc_type="unary"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardSidecarTitle)
      )
      .addPanel(
        g.panel('gPRC (Unary) Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
        g.grpcErrorsPanel('server', 'namespace="$namespace",%(thanosSidecarSelector)s,grpc_type="unary"' % $._config) +
        g.addDashboardLink($._config.grafanaThanos.dashboardSidecarTitle)
      )
      .addPanel(
        g.sloLatency(
          'gPRC (Unary) Latency 99th Percentile',
          'Shows how long has it taken to handle requests from queriers, in quantiles.',
          'grpc_server_handling_seconds_bucket{grpc_type="unary",namespace="$namespace",%(thanosSidecarSelector)s}' % $._config,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardSidecarTitle)
      ),
    ],
  },
}
