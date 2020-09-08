local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  receive+:: {
    jobPrefix: error 'must provide job prefix for Thanos Receive dashboard',
    selector: error 'must provide selector for Thanos Receive dashboard',
    title: error 'must provide title for Thanos Receive dashboard',
    namespaceLabel: error 'must provide namespace label', 
  },
  grafanaDashboards+:: {
    'thanos-receive.json':
      g.dashboard(thanos.receive.title)
      .addRow(
        g.row('WRITE - Incoming Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of incoming requests.') +
          g.httpQpsPanel('http_requests_total', 'handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
          g.httpErrPanel('http_requests_total', 'handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle incoming requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', 'handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive)
        )
      )
      .addRow(
        g.row('WRITE - Replication')
        .addPanel(
          g.panel('Rate', 'Shows rate of replications to other receive nodes.') +
          g.queryPanel(
            'sum(rate(thanos_receive_replications_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}[$interval])) by (job)' % thanos.receive,
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of replications to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_replications_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,result="error"}' % thanos.receive,
            'thanos_receive_replications_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}' % thanos.receive,
          )
        )
      )
      .addRow(
        g.row('WRITE - Forward Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of forwarded requests to other receive nodes.') +
          g.queryPanel(
            'sum(rate(thanos_receive_forward_requests_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}[$interval])) by (job)' % thanos.receive,
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of forwareded requests to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_forward_requests_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,result="error"}' % thanos.receive,
            'thanos_receive_forward_requests_total{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}' % thanos.receive,
          )
        )
      )
      .addRow(
        g.row('WRITE - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method="RemoteWrite"' % thanos.receive)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method="RemoteWrite"' % thanos.receive)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method="RemoteWrite"' % thanos.receive)
        )
      )
      .addRow(
        g.row('READ - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method!="RemoteWrite"' % thanos.receive)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method!="RemoteWrite"' % thanos.receive)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="unary",grpc_method!="RemoteWrite"' % thanos.receive)
        )
      )
      .addRow(
        g.row('READ - gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcQpsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="server_stream"' % thanos.receive)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="server_stream"' % thanos.receive)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s,grpc_type="server_stream"' % thanos.receive)
        )
      )
      .addRow(
        g.row('Last Updated')
        .addPanel(
          g.panel('Successful Upload', 'Shows the relative time of last successful upload to the object-store bucket.') +
          g.tablePanel(
            ['time() - max(thanos_objstore_bucket_last_successful_upload_time{%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}) by (job, bucket)'] % thanos.receive,
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
        g.resourceUtilizationRow(thanos.receive.namespaceLabel, thanos.receive.selector)
      ) +
      g.template('namespace', thanos.dashboard.namespaceMetric) +
      g.template('job', 'up', '%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive, true, '%(jobPrefix)s.*' % thanos.receive) +
      g.template('pod', 'kube_pod_info', '%(namespaceLabel)s="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.receive, true, '.*'),

    __overviewRows__+:: [
      g.row('Receive')
      .addPanel(
        g.panel('Incoming Requests Rate', 'Shows rate of incoming requests.') +
        g.httpQpsPanel('http_requests_total', 'handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.panel('Incoming Requests Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
        g.httpErrPanel('http_requests_total', 'handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s' % thanos.receive) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.sloLatency(
          'Incoming Requests Latency 99th Percentile',
          'Shows how long has it taken to handle incoming requests.',
          'http_request_duration_seconds_bucket{handler="receive",%(namespaceLabel)s="$namespace",job=~"$job",%(selector)s}' % thanos.receive,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.receive.title)
      ),
    ],
  },
}
