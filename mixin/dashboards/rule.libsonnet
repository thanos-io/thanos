local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  rule+:: {
    jobPrefix: error 'must provide job prefix for Thanos Rule dashboard',
    selector: error 'must provide selector for Thanos Rule dashboard',
    title: error 'must provide title for Thanos Rule dashboard',
  },
  grafanaDashboards+:: {
    'rule.json':
      g.dashboard(thanos.rule.title)
      .addRow(
        g.row('Alert Sent')
        .addPanel(
          g.panel('Dropped Rate', 'Shows rate of dropped alerts.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_dropped_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)',
            '{{job}} {{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_sent_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)',
            '{{job}} {{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{namespace="$namespace",job=~"$job"}',
            'thanos_alert_sender_alerts_sent_total{namespace="$namespace",job=~"$job"}',
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', 'namespace="$namespace",job=~"$job"'),
        )
      )
      .addRow(
        g.row('Alert Queue')
        .addPanel(
          g.panel('Push Rate', 'Shows rate of queued alerts.') +
          g.queryPanel(
            'sum(rate(thanos_alert_queue_alerts_dropped_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, pod)',
            '{{job}} {{pod}}'
          )
        )
        .addPanel(
          g.panel('Drop Ratio', 'Shows ratio of dropped alerts compared to the total number of queued alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_queue_alerts_dropped_total{namespace="$namespace",job=~"$job"}',
            'thanos_alert_queue_alerts_pushed_total{namespace="$namespace",job=~"$job"}',
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.rule, true, '%(jobPrefix)s.*' % thanos.rule) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.rule, true, '.*'),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum(rate(thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(selector)s}[$interval])) by (job, alertmanager)' % thanos.rule,
          '{{job}} {{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.rule.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{namespace="$namespace",%(selector)s}' % thanos.rule,
          'thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(selector)s}' % thanos.rule,
        ) +
        g.addDashboardLink(thanos.rule.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{namespace="$namespace",%(selector)s}' % thanos.rule,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.rule.title)
      ) +
      g.collapse,
    ],
  },
}
