local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  receive+:: {
    jobPrefix: error 'must provide job prefix for Thanos Receive dashboard',
    selector: error 'must provide selector for Thanos Receive dashboard',
    title: error 'must provide title for Thanos Receive dashboard',
  },
  grafanaDashboards+:: {
    'receive.json':
      g.dashboard(thanos.receive.title)
      .addRow(
        g.row('WRITE - Incoming Request')
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
        g.row('WRITE - Replication')
        .addPanel(
          g.panel('Rate', 'Shows rate of replications to other receive nodes.') +
          g.queryPanel(
            'sum(rate(thanos_receive_replications_total{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of replications to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_replications_total{namespace="$namespace",job=~"$job",result="error"}',
            'thanos_receive_replications_total{namespace="$namespace",job=~"$job"}',
          )
        )
      )
      .addRow(
        g.row('WRITE - Forward Request')
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
        g.row('WRITE - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method="RemoteWrite"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method="RemoteWrite"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method="RemoteWrite"')
        )
      )
      .addRow(
        g.row('READ - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method!="RemoteWrite"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method!="RemoteWrite"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary",grpc_method!="RemoteWrite"')
        )
      )
      .addRow(
        g.row('READ - gRPC (Stream)')
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
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.receive, true, '%(jobPrefix)s.*' % thanos.receive) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.receive, true, '.*'),

    __overviewRows__+:: [
      g.row('Receive')
      .addPanel(
        g.panel('Incoming Requests Rate', 'Shows rate of incoming requests.') +
        g.httpQpsPanel('http_requests_total', 'handler="receive",namespace="$namespace",%(selector)s' % thanos.receive) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.panel('Incoming Requests Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
        g.httpErrPanel('http_requests_total', 'handler="receive",namespace="$namespace",%(selector)s' % thanos.receive) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.sloLatency(
          'Incoming Requests Latency 99th Percentile',
          'Shows how long has it taken to handle incoming requests.',
          'http_request_duration_seconds_bucket{handler="receive",namespace="$namespace",%(selector)s}' % thanos.receive,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.receive.title)
      ),
    ],
  },
}
