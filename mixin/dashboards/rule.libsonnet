local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  rule+:: {
    selector: error 'must provide selector for Thanos Rule dashboard',
    title: error 'must provide title for Thanos Rule dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job="$job"']),
      aggregator: std.join(', ', thanos.dashboard.aggregator + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.rule != null then 'rule.json']:
      g.dashboard(thanos.rule.title)
      .addRow(
        g.row('Rule Group Evaluations')
        .addPanel(
          g.panel('Rule Group Evaluations') +
          g.queryPanel(
            'sum by (%s, strategy) (rate(prometheus_rule_evaluations_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evaluations Missed') +
          g.queryPanel(
            'sum by (%s, strategy) (increase(prometheus_rule_group_iterations_missed_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evlauations Too Slow') +
          g.queryPanel(
            |||
              (
                max by(%(aggregator)s, rule_group) (prometheus_rule_group_last_duration_seconds{%(selector)s})
                >
                sum by(%(aggregator)s, rule_group) (prometheus_rule_group_interval_seconds{%(selector)s})
              )
            ||| % thanos.rule.dashboard,
            '{{ rule_group }}',
          )
        )
      )
      .addRow(
        g.row('Alert Sent')
        .addPanel(
          g.panel('Dropped Rate', 'Shows rate of dropped alerts.') +
          g.queryPanel(
            'sum by (%(aggregator)s, alertmanager) (rate(thanos_alert_sender_alerts_dropped_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
            '{{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum by (%(aggregator)s, alertmanager) (rate(thanos_alert_sender_alerts_sent_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
            '{{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{%s}' % thanos.rule.dashboard.selector,
            'thanos_alert_sender_alerts_sent_total{%s}' % thanos.rule.dashboard.selector,
            thanos.rule.dashboard.aggregator
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator),
        )
      )
      .addRow(
        g.row('Alert Queue')
        .addPanel(
          g.panel('Push Rate', 'Shows rate of queued alerts.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_alert_queue_alerts_dropped_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
            '{{job}}'
          )
        )
        .addPanel(
          g.panel('Drop Ratio', 'Shows ratio of dropped alerts compared to the total number of queued alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_queue_alerts_dropped_total{%s}' % thanos.rule.dashboard.selector,
            'thanos_alert_queue_alerts_pushed_total{%s}' % thanos.rule.dashboard.selector,
            thanos.rule.dashboard.aggregator
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="unary"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds_bucket', '%s, grpc_type="unary"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcRequestsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('grpc_server_handled_total', '%s, grpc_type="server_stream"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.latencyPanel('grpc_server_handling_seconds_bucket', '%s, grpc_type="server_stream"' % thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.rule.dashboard.selector, thanos.rule.dashboard.aggregator)
      ),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum by (%s, alertmanager) (rate(thanos_alert_sender_alerts_sent_total{%s}[$interval]))' % [thanos.rule.dashboard.aggregator, thanos.rule.dashboard.selector],
          '{{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.rule.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{%s}' % thanos.rule.dashboard.selector,
          'thanos_alert_sender_alerts_sent_total{%s}' % thanos.rule.dashboard.selector,
          thanos.rule.dashboard.aggregator
        ) +
        g.addDashboardLink(thanos.rule.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{%s}' % thanos.rule.dashboard.selector,
          thanos.rule.dashboard.aggregator,
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
