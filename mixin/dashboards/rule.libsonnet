local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  rule+:: {
    jobPrefix: error 'must provide job prefix for Thanos Rule dashboard',
    selector: error 'must provide selector for Thanos Rule dashboard',
    title: error 'must provide title for Thanos Rule dashboard',
    namespaceLabel: error 'must provide namespace label', 
  },
  grafanaDashboards+:: {
    'thanos-rule.json':
      g.dashboard(thanos.rule.title)
      .addRow(
        g.row('Rule Group Evaluations')
        .addPanel(
          g.panel('Rule Group Evaluations') +
          g.queryPanel(
            |||
              sum by (strategy) (rate(prometheus_rule_evaluations_total{%(namespaceLabel)s="$namespace",%(selector)s}[$interval]))
            ||| % thanos.rule,
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evaluations Missed') +
          g.queryPanel(
            |||
              sum by (strategy) (increase(prometheus_rule_group_iterations_missed_total{%(namespaceLabel)s="$namespace",%(selector)s}[$interval]))
            ||| % thanos.rule,
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evlauations Too Slow') +
          g.queryPanel(
            |||
              (
                max by(rule_group) (prometheus_rule_group_last_duration_seconds{%(namespaceLabel)s="$namespace",%(selector)s})
                >
                sum by(rule_group) (prometheus_rule_group_interval_seconds{%(namespaceLabel)s="$namespace",%(selector)s})
              )
            ||| % thanos.rule,
            '{{ rule_group }}',
          )
        )
      )
      .addRow(
        g.row('Alert Sent')
        .addPanel(
          g.panel('Dropped Rate', 'Shows rate of dropped alerts.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_dropped_total{%(namespaceLabel)s="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)' % thanos.rule,
            '{{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum(rate(thanos_alert_sender_alerts_sent_total{%(namespaceLabel)s="$namespace",job=~"$job"}[$interval])) by (job, alertmanager)' % thanos.rule,
            '{{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{%(namespaceLabel)s="$namespace",job=~"$job"}' % thanos.rule,
            'thanos_alert_sender_alerts_sent_total{%(namespaceLabel)s="$namespace",job=~"$job"}' % thanos.rule,
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', '%(namespaceLabel)s="$namespace",job=~"$job"' % thanos.rule),
        )
      )
      .addRow(
        g.row('Alert Queue')
        .addPanel(
          g.panel('Push Rate', 'Shows rate of queued alerts.') +
          g.queryPanel(
            'sum(rate(thanos_alert_queue_alerts_dropped_total{%(namespaceLabel)s="$namespace",job=~"$job"}[$interval])) by (job, pod)' % thanos.rule,
            '{{pod}}'
          )
        )
        .addPanel(
          g.panel('Drop Ratio', 'Shows ratio of dropped alerts compared to the total number of queued alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_queue_alerts_dropped_total{%(namespaceLabel)s="$namespace",job=~"$job"}' % thanos.rule,
            'thanos_alert_queue_alerts_pushed_total{%(namespaceLabel)s="$namespace",job=~"$job"}' % thanos.rule,
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="unary"') % thanos.rule
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="unary"' % thanos.rule)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="unary"' % thanos.rule)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="server_stream"' % thanos.rule)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="server_stream"' % thanos.rule)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanel('server', '%(namespaceLabel)s="$namespace",job=~"$job",grpc_type="server_stream"' % thanos.rule)
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', thanos.dashboard.namespaceMetric) +
      g.template('job', 'up', '%(namespaceLabel)s="$namespace",%(selector)s' % thanos.rule, true, '%(jobPrefix)s.*' % thanos.rule) +
      g.template('pod', 'kube_pod_info', '%(namespaceLabel)s="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.rule, true, '.*'),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum(rate(thanos_alert_sender_alerts_sent_total{%(namespaceLabel)s="$namespace",%(selector)s}[$interval])) by (job, alertmanager)' % thanos.rule,
          '{{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.rule.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{%(namespaceLabel)s="$namespace",%(selector)s}' % thanos.rule,
          'thanos_alert_sender_alerts_sent_total{%(namespaceLabel)s="$namespace",%(selector)s}' % thanos.rule,
        ) +
        g.addDashboardLink(thanos.rule.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{%(namespaceLabel)s="$namespace",%(selector)s}' % thanos.rule,
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
