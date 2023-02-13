local g = import '../lib/thanos-grafana-builder/builder.libsonnet';
local utils = import '../lib/utils.libsonnet';

{
  local thanos = self,
  rule+:: {
    selector: error 'must provide selector for Thanos Rule dashboard',
    title: error 'must provide title for Thanos Rule dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job=~"$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.rule != null then 'rule.json']:
      local grpcUnarySelector = utils.joinLabels([thanos.rule.dashboard.selector, 'grpc_type="unary"']);
      local grpcServerStreamSelector = utils.joinLabels([thanos.rule.dashboard.selector, 'grpc_type="server_stream"']);

      g.dashboard(thanos.rule.title)
      .addRow(
        g.row('Rule Group Evaluations')
        .addPanel(
          g.panel('Rule Group Evaluations') +
          g.queryPanel(
            'sum by (%s) (rate(prometheus_rule_evaluations_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.rule.dashboard.dimensions, 'strategy']), thanos.rule.dashboard.selector],
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evaluations Missed') +
          g.queryPanel(
            'sum by (%s) (increase(prometheus_rule_group_iterations_missed_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.rule.dashboard.dimensions, 'strategy']), thanos.rule.dashboard.selector],
            '{{ strategy }}',
          )
        )
        .addPanel(
          g.panel('Rule Group Evlauations Too Slow') +
          g.queryPanel(
            |||
              (
                sum by(%(dimensions)s, rule_group) (prometheus_rule_group_last_duration_seconds{%(selector)s})
                >
                sum by(%(dimensions)s, rule_group) (prometheus_rule_group_interval_seconds{%(selector)s})
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
            'sum by (%(dimensions)s, alertmanager) (rate(thanos_alert_sender_alerts_dropped_total{%s}[$__rate_interval]))' % [thanos.rule.dashboard.dimensions, thanos.rule.dashboard.selector],
            '{{alertmanager}}'
          )
        )
        .addPanel(
          g.panel('Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
          g.queryPanel(
            'sum by (%(dimensions)s, alertmanager) (rate(thanos_alert_sender_alerts_sent_total{%s}[$__rate_interval]))' % [thanos.rule.dashboard.dimensions, thanos.rule.dashboard.selector],
            '{{alertmanager}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_sender_errors_total{%s}' % thanos.rule.dashboard.selector,
            'thanos_alert_sender_alerts_sent_total{%s}' % thanos.rule.dashboard.selector,
            thanos.rule.dashboard.dimensions
          )
        )
        .addPanel(
          g.panel('Sent Duration', 'Shows how long has it taken to send alerts to alert manager.') +
          g.latencyPanel('thanos_alert_sender_latency_seconds', thanos.rule.dashboard.selector, thanos.rule.dashboard.dimensions),
        )
      )
      .addRow(
        g.row('Alert Queue')
        .addPanel(
          g.panel('Push Rate', 'Shows rate of queued alerts.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_alert_queue_alerts_dropped_total{%s}[$__rate_interval]))' % [thanos.rule.dashboard.dimensions, thanos.rule.dashboard.selector],
            '{{job}}'
          )
        )
        .addPanel(
          g.panel('Drop Ratio', 'Shows ratio of dropped alerts compared to the total number of queued alerts.') +
          g.qpsErrTotalPanel(
            'thanos_alert_queue_alerts_dropped_total{%s}' % thanos.rule.dashboard.selector,
            'thanos_alert_queue_alerts_pushed_total{%s}' % thanos.rule.dashboard.selector,
            thanos.rule.dashboard.dimensions
          )
        )
      )
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcUnarySelector, thanos.rule.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcUnarySelector, thanos.rule.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcUnarySelector, thanos.rule.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.rule.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.rule.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.latencyPanel('grpc_server_handling_seconds', grpcServerStreamSelector, thanos.rule.dashboard.dimensions)
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.rule.dashboard.selector, thanos.rule.dashboard.dimensions)
      ),

    __overviewRows__+:: if thanos.rule == null then [] else [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum by (%s) (rate(thanos_alert_sender_alerts_sent_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.dashboard.overview.dimensions, 'alertmanager']), thanos.dashboard.overview.selector],
          '{{alertmanager}}'
        ) +
        g.addDashboardLink(thanos.rule.title) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{%s}' % thanos.dashboard.overview.selector,
          'thanos_alert_sender_alerts_sent_total{%s}' % thanos.dashboard.overview.selector,
          thanos.dashboard.overview.dimensions
        ) +
        g.addDashboardLink(thanos.rule.title)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{%s}' % thanos.dashboard.overview.selector,
          thanos.dashboard.overview.dimensions,
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
