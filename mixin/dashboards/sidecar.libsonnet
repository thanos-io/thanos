local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  sidecar+:: {
    jobPrefix: error 'must provide job prefix for Thanos Sidecar dashboard',
    selector: error 'must provide selector for Thanos Sidecar dashboard',
    title: error 'must provide title for Thanos Sidecar dashboard',
  },
  grafanaDashboards+:: {
    'sidecar.json':
      g.dashboard(thanos.sidecar.title)
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
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.sidecar, true, '%(jobPrefix)s.*' % thanos.sidecar) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.sidecar, true, '.*'),

    __overviewRows__+:: [
      g.row('Sidecar')
      .addPanel(
        g.panel('gPRC (Unary) Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
        g.grpcQpsPanel('server', 'namespace="$namespace",%(selector)s,grpc_type="unary"' % thanos.sidecar) +
        g.addDashboardLink(thanos.sidecar.title)
      )
      .addPanel(
        g.panel('gPRC (Unary) Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
        g.grpcErrorsPanel('server', 'namespace="$namespace",%(selector)s,grpc_type="unary"' % thanos.sidecar) +
        g.addDashboardLink(thanos.sidecar.title)
      )
      .addPanel(
        g.sloLatency(
          'gPRC (Unary) Latency 99th Percentile',
          'Shows how long has it taken to handle requests from queriers, in quantiles.',
          'grpc_server_handling_seconds_bucket{grpc_type="unary",namespace="$namespace",%(selector)s}' % thanos.sidecar,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.sidecar.title)
      ),
    ],
  },
}
