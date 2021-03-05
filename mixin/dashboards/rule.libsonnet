local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  rule+:: {
    selector: error 'must provide selector for Thanos Rule dashboard',
    title: error 'must provide title for Thanos Rule dashboard',
  },
  grafanaDashboards+:: {
    local selector = std.join(', ', thanos.dashboard.commonSelector + ['job="$job"']),
    [if thanos.rule != null then 'rule.json']:
      g.dashboard(thanos.rule.title)
      .addRow(
        g.row('Rule Group Evaluations')
        .addPanel(
          g.panel('Rule Group Evaluations') +
          g.queryPanel(
            'sum by (strategy) (rate(prometheus_rule_evaluations_total{%s}[$interval]))' % selector,
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evaluations Missed') +
          g.queryPanel(
            'sum by (strategy) (increase(prometheus_rule_group_iterations_missed_total{%s}[$interval]))' % selector,
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evlauations Too Slow') +
          g.queryPanel(
            |||
              (
                max by(rule_group) (prometheus_rule_group_last_duration_seconds{%s})
                >
                sum by(rule_group) (prometheus_rule_group_interval_seconds{%s})
              )
            ||| % [selector, selector],
            '{{ rule_group }}',
          )
        )
      )
      .addRow(
        g.row('Alert Sent')
        .addPanel(
          g.panel('Dropped Rate', 'Shows rate of dropped alerts.') +
          g.queryPanel(
            'sum by (job, alertmanager) (rate(thanos_alert_sender_alerts_dropped_total{%s}[$interval]))' % selector,
            '{{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum by (job, alertmanager) (rate(thanos_alert_sender_alerts_sent_total{%s}[$interval]))' % selector,
            '{{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{%s}' % selector,
            'thanos_alert_sender_alerts_sent_total{%s}' % selector,
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', selector),
        )
      )
      .addRow(
        g.row('Alert Queue')
        .addPanel(
          g.panel('Push Rate', 'Shows rate of queued alerts.') +
          g.queryPanel(
            'sum  by (job) (rate(thanos_alert_queue_alerts_dropped_total{%s}[$interval]))' % selector,
            '{{job}}'
          )
        )
        .addPanel(
          g.panel('Drop Ratio', 'Shows ratio of dropped alerts compared to the total number of queued alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_queue_alerts_dropped_total{%s}' % selector,
            'thanos_alert_queue_alerts_pushed_total{%s}' % selector,
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanel('server', '%s, grpc_type="unary"' % selector)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', '%s, grpc_type="unary"' % selector)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanel('server', '%s, grpc_type="unary"' % selector)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanel('server', '%s, grpc_type="server_stream"' % selector)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('server', '%s, grpc_type="server_stream"' % selector)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanel('server', '%s, grpc_type="server_stream"' % selector)
        )
      )
      .addRow(
        g.resourceUtilizationRow(selector)
      ),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum by (job, alertmanager) (rate(thanos_alert_sender_alerts_sent_total{%s}[$interval]))' % selector,
          '{{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.rule.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{%s}' % selector,
          'thanos_alert_sender_alerts_sent_total{%s}' % selector,
        ) +
        g.addDashboardLink(thanos.rule.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{%s}' % selector,
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
