local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  sidecar+:: {
    selector: error 'must provide selector for Thanos Sidecar dashboard',
    title: error 'must provide title for Thanos Sidecar dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job="$job"']),
      aggregator: std.join(', ', thanos.dashboard.aggregator + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.sidecar != null then 'sidecar.json']:
      g.dashboard(thanos.sidecar.title)
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', '%s, grpc_type="unary"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', '%s, grpc_type="server_stream"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('Last Updated')
        .addPanel(
          g.panel('Successful Upload', 'Shows the relative time of last successful upload to the object-store bucket.') +
          g.tablePanel(
            ['time() - max by (%s, bucket) (thanos_objstore_bucket_last_successful_upload_time{%s})' % [thanos.sidecar.dashboard.aggregator, thanos.sidecar.dashboard.selector]],
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
            'sum by (%s, operation) (rate(thanos_objstore_bucket_operations_total{%s}[$interval]))' % [thanos.sidecar.dashboard.aggregator, thanos.sidecar.dashboard.selector],
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors') +
          g.qpsErrTotalPanel(
            'thanos_objstore_bucket_operation_failures_total{%s}' % thanos.sidecar.dashboard.selector,
            'thanos_objstore_bucket_operations_total{%s}' % thanos.sidecar.dashboard.selector,
            thanos.sidecar.dashboard.aggregator
          )
        )
        .addPanel(
          g.panel('Duration') +
          g.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator)
      ),

    __overviewRows__+:: [
      g.row('Sidecar')
      .addPanel(
        g.panel('gPRC (Unary) Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
        g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator) +
        g.addDashboardLink(thanos.sidecar.title)
      )
      .addPanel(
        g.panel('gPRC (Unary) Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
        g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.sidecar.dashboard.selector, thanos.sidecar.dashboard.aggregator) +
        g.addDashboardLink(thanos.sidecar.title)
      )
      .addPanel(
        g.sloLatency(
          'gPRC (Unary) Latency 99th Percentile',
          'Shows how long has it taken to handle requests from queriers, in quantiles.',
          'grpc_server_handling_seconds_bucket{%s, grpc_type="unary"}' % thanos.sidecar.dashboard.selector,
          thanos.sidecar.dashboard.aggregator,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.sidecar.title)
      ),
    ],
  },
}
