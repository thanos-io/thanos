{
  local thanos = self,
  rule+:: {
    selector: error 'must provide selector for Thanos Rule alerts',
    grpcErrorThreshold: 5,
    rulerDnsErrorThreshold: 1,
    alertManagerDnsErrorThreshold: 1,
    evalErrorThreshold: 5,
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job', 'instance']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.rule == null then [] else [
      local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '';
      {
        name: 'thanos-rule',
        rules: [
          {
            alert: 'ThanosRuleQueueIsDroppingAlerts',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s is failing to queue alerts.' % location,
              summary: 'Thanos Rule is failing to queue alerts.',
            },
            expr: |||
              sum by (%(dimensions)s) (rate(thanos_alert_queue_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleSenderIsFailingAlerts',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s is failing to send alerts to alertmanager.' % location,
              summary: 'Thanos Rule is failing to send alerts to alertmanager.',
            },
            expr: |||
              sum by (%(dimensions)s) (rate(thanos_alert_sender_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleHighRuleEvaluationFailures',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s is failing to evaluate rules.' % location,
              summary: 'Thanos Rule is failing to evaluate rules.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(prometheus_rule_evaluation_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(prometheus_rule_evaluations_total{%(selector)s}[5m]))
              * 100 > %(evalErrorThreshold)s
              )
            ||| % thanos.rule,

            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleHighRuleEvaluationWarnings',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s has high number of evaluation warnings.' % location,
              summary: 'Thanos Rule has high number of evaluation warnings.',
            },
            expr: |||
              sum by (%(dimensions)s) (rate(thanos_rule_evaluation_with_warnings_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,

            'for': '15m',
            labels: {
              severity: 'info',
            },
          },
          {
            alert: 'ThanosRuleRuleEvaluationLatencyHigh',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s has higher evaluation latency than interval for {{$labels.rule_group}}.' % location,
              summary: 'Thanos Rule has high rule evaluation latency.',
            },
            expr: |||
              (
                sum by (%(dimensions)s, rule_group) (prometheus_rule_group_last_duration_seconds{%(selector)s})
              >
                sum by (%(dimensions)s, rule_group) (prometheus_rule_group_interval_seconds{%(selector)s})
              )
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleGrpcErrorRate',
            annotations: {
              description: 'Thanos Rule {{$labels.job}}%s is failing to handle {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Rule is failing to handle grpc requests.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(grpc_server_started_total{%(selector)s}[5m]))
              * 100 > %(grpcErrorThreshold)s
              )
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleConfigReloadFailure',
            annotations: {
              description: 'Thanos Rule {{$labels.job}}%s has not been able to reload its configuration.' % location,
              summary: 'Thanos Rule has not been able to reload configuration.',
            },
            expr: 'avg by (%(dimensions)s) (thanos_rule_config_last_reload_successful{%(selector)s}) != 1' % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'info',
            },
          },
          {
            alert: 'ThanosRuleQueryHighDNSFailures',
            annotations: {
              description: 'Thanos Rule {{$labels.job}}%s has {{$value | humanize}}%% of failing DNS queries for query endpoints.' % location,
              summary: 'Thanos Rule is having high number of DNS failures.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_rule_query_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_rule_query_apis_dns_lookups_total{%(selector)s}[5m]))
              * 100 > %(rulerDnsErrorThreshold)s
              )
            ||| % thanos.rule,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosRuleAlertmanagerHighDNSFailures',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s has {{$value | humanize}}%% of failing DNS queries for Alertmanager endpoints.' % location,
              summary: 'Thanos Rule is having high number of DNS failures.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_rule_alertmanagers_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_rule_alertmanagers_dns_lookups_total{%(selector)s}[5m]))
              * 100 > %(alertManagerDnsErrorThreshold)s
              )
            ||| % thanos.rule,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            // NOTE: This alert will give false positive if no rules are configured.
            alert: 'ThanosRuleNoEvaluationFor10Intervals',
            annotations: {
              description: 'Thanos Rule {{$labels.job}}%s has rule groups that did not evaluate for at least 10x of their expected interval.' % location,
              summary: 'Thanos Rule has rule groups that did not evaluate for 10 intervals.',
            },
            expr: |||
              time() -  max by (%(dimensions)s, group) (prometheus_rule_group_last_evaluation_timestamp_seconds{%(selector)s})
              >
              10 * max by (%(dimensions)s, group) (prometheus_rule_group_interval_seconds{%(selector)s})
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              // TODO(bwplotka): Move to critical once we gain more confidence in this, it's not trivial as it looks.
              severity: 'info',
            },
          },
          {
            alert: 'ThanosNoRuleEvaluations',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%s did not perform any rule evaluations in the past 10 minutes.' % location,
              summary: 'Thanos Rule did not perform any rule evaluations.',
            },
            expr: |||
              sum by (%(dimensions)s) (rate(prometheus_rule_evaluations_total{%(selector)s}[5m])) <= 0
                and
              sum by (%(dimensions)s) (thanos_rule_loaded_rules{%(selector)s}) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
