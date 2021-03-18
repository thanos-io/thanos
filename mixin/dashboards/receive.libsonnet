local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  receive+:: {
    selector: error 'must provide selector for Thanos Receive dashboard',
    title: error 'must provide title for Thanos Receive dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job="$job"']),
      aggregator: std.join(', ', thanos.dashboard.aggregator + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.receive != null then 'receive.json']:
      g.dashboard(thanos.receive.title)
      .addRow(
        g.row('WRITE - Incoming Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of incoming requests.') +
          g.httpQpsPanel('http_requests_total', '%s, handler="receive"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
          g.httpErrPanel('http_requests_total', '%s, handler="receive"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle incoming requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', '%s, handler="receive"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('WRITE - Replication')
        .addPanel(
          g.panel('Rate', 'Shows rate of replications to other receive nodes.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_receive_replications_total{%s}[$interval]))' % [thanos.receive.dashboard.aggregator, thanos.receive.dashboard.selector],
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of replications to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_replications_total{%s, result="error"}' % thanos.receive.dashboard.selector,
            'thanos_receive_replications_total{%s}' % thanos.receive.dashboard.selector,
            thanos.receive.dashboard.aggregator
          )
        )
      )
      .addRow(
        g.row('WRITE - Forward Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of forwarded requests to other receive nodes.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_receive_forward_requests_total{%s}[$interval]))' % [thanos.receive.dashboard.aggregator, thanos.receive.dashboard.selector],
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of forwareded requests to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_forward_requests_total{%s, result="error"}' % thanos.receive.dashboard.selector,
            'thanos_receive_forward_requests_total{%s}' % thanos.receive.dashboard.selector,
            thanos.receive.dashboard.aggregator
          )
        )
      )
      .addRow(
        g.row('WRITE - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="unary", grpc_method="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="unary", grpc_method="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', '%s, grpc_type="unary", grpc_method="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('READ - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="unary", grpc_method!="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="unary", grpc_method!="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', '%s, grpc_type="unary", grpc_method!="RemoteWrite"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('READ - gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', '%s, grpc_type="server_stream"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('Last Updated')
        .addPanel(
          g.panel('Successful Upload', 'Shows the relative time of last successful upload to the object-store bucket.') +
          g.tablePanel(
            ['time() - max by (%s, bucket) (thanos_objstore_bucket_last_successful_upload_time{%s})' % [thanos.receive.dashboard.aggregator, thanos.receive.dashboard.selector]],
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
        g.resourceUtilizationRow(thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator)
      ),

    __overviewRows__+:: [
      g.row('Receive')
      .addPanel(
        g.panel('Incoming Requests Rate', 'Shows rate of incoming requests.') +
        g.httpQpsPanel('http_requests_total', '%s, handler="receive"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.panel('Incoming Requests Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
        g.httpErrPanel('http_requests_total', '%s, handler="receive"' % thanos.receive.dashboard.selector, thanos.receive.dashboard.aggregator) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.sloLatency(
          'Incoming Requests Latency 99th Percentile',
          'Shows how long has it taken to handle incoming requests.',
          'http_request_duration_seconds_bucket{%s, handler="receive"}' % thanos.receive.dashboard.selector,
          thanos.receive.dashboard.aggregator,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.receive.title)
      ),
    ],
  },
}
