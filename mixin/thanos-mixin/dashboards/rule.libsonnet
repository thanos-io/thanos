local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  grafanaDashboards+:: {
    'rule.json':
      g.dashboard($._config.grafanaThanos.dashboardRuleTitle)
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
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles.') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        ) +
        g.collapse
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
        g.row('Detailed')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests.') +
          g.grpcQpsPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests.') +
          g.grpcErrDetailsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests, in quantiles') +
          g.grpcLatencyPanelDetailed('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        ) +
        g.collapse
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(thanosRuleSelector)s' % $._config, true, '%(thanosRuleJobPrefix)s.*' % $._config) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(thanosRuleJobPrefix)s.*"' % $._config, true, '.*'),

    __overviewRows__+:: [
      g.row('Rule')
      .addPanel(
        g.panel('Alert Sent Rate', 'Shows rate of alerts that successfully sent to alert manager.') +
        g.queryPanel(
          'sum(rate(thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(thanosRuleSelector)s}[$interval])) by (job, alertmanager)' % $._config,
          '{{job}} {{alertmanager}}'
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardRuleTitle) +
        g.stack
      )
      .addPanel(
        g.panel('Alert Sent Errors', 'Shows ratio of errors compared to the total number of sent alerts.') +
        g.qpsErrTotalPanel(
          'thanos_alert_sender_errors_total{namespace="$namespace",%(thanosRuleSelector)s}' % $._config,
          'thanos_alert_sender_alerts_sent_total{namespace="$namespace",%(thanosRuleSelector)s}' % $._config,
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardRuleTitle)
      )
      .addPanel(
        g.sloLatency(
          'Alert Sent Duration',
          'Shows how long has it taken to send alerts to alert manager.',
          'thanos_alert_sender_latency_seconds_bucket{namespace="$namespace",%(thanosRuleSelector)s}' % $._config,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink($._config.grafanaThanos.dashboardRuleTitle)
      ) +
      g.collapse,
    ],
  },
}
