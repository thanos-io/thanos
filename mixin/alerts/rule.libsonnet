{
  local thanos = self,
  rule+:: {
    selector: error 'must provide selector for Thanos Rule alerts',
    grpcErrorThreshold: 5,
    rulerDnsErrorThreshold: 1,
    alertManagerDnsErrorThreshold: 1,
    evalErrorThreshold: 5,
    aggregator: std.join(', ', std.objectFields(thanos.hierarcies) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.rule == null then [] else [
      local location = if std.length(std.objectFields(thanos.hierarcies)) > 0 then ' in ' + std.join('/', ['{{labels.%s}}' % level for level in std.objectFields(thanos.hierarcies)]) else ' ';
      {
        name: 'thanos-rule',
        rules: [
          {
            alert: 'ThanosRuleQueueIsDroppingAlerts',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%sis failing to queue alerts.' % location,
              summary: 'Thanos Rule is failing to queue alerts.',
            },
            expr: |||
              sum by (%(aggregator)s) (rate(thanos_alert_queue_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleSenderIsFailingAlerts',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%sis failing to send alerts to alertmanager.' % location,
              summary: 'Thanos Rule is failing to send alerts to alertmanager.',
            },
            expr: |||
              sum by (%(aggregator)s) (rate(thanos_alert_sender_alerts_dropped_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosRuleHighRuleEvaluationFailures',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%sis failing to evaluate rules.' % location,
              summary: 'Thanos Rule is failing to evaluate rules.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(prometheus_rule_evaluation_failures_total{%(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(prometheus_rule_evaluations_total{%(selector)s}[5m]))
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
              description: 'Thanos Rule {{$labels.instance}}%shas high number of evaluation warnings.' % location,
              summary: 'Thanos Rule has high number of evaluation warnings.',
            },
            expr: |||
              sum by (%(aggregator)s) (rate(thanos_rule_evaluation_with_warnings_total{%(selector)s}[5m])) > 0
            ||| % thanos.rule,

            'for': '15m',
            labels: {
              severity: 'info',
            },
          },
          {
            alert: 'ThanosRuleRuleEvaluationLatencyHigh',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%shas higher evaluation latency than interval for {{$labels.rule_group}}.' % location,
              summary: 'Thanos Rule has high rule evaluation latency.',
            },
            expr: |||
              (
                sum by (%(aggregator)s, instance, rule_group) (prometheus_rule_group_last_duration_seconds{%(selector)s})
              >
                sum by (%(aggregator)s, instance, rule_group) (prometheus_rule_group_interval_seconds{%(selector)s})
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
              description: 'Thanos Rule {{$labels.instance}}%sis failing to handle {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Rule is failing to handle grpc requests.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(grpc_server_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss|DeadlineExceeded", %(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(grpc_server_started_total{%(selector)s}[5m]))
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
              description: 'Thanos Rule {{$labels.instance}}%shas not been able to reload its configuration.' % location,
              summary: 'Thanos Rule has not been able to reload configuration.',
            },
            expr: 'avg by (%(aggregator)s) (thanos_rule_config_last_reload_successful{%(selector)s}) != 1' % thanos.rule,
            'for': '5m',
            labels: {
              severity: 'info',
            },
          },
          {
            alert: 'ThanosRuleQueryHighDNSFailures',
            annotations: {
              description: 'Thanos Rule {{$labels.instance}}%shas {{$value | humanize}}%% of failing DNS queries for query endpoints.' % location,
              summary: 'Thanos Rule is having high number of DNS failures.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(thanos_rule_query_apis_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(thanos_rule_query_apis_dns_lookups_total{%(selector)s}[5m]))
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
              description: 'Thanos Rule {{$labels.instance}}%shas {{$value | humanize}}%% of failing DNS queries for Alertmanager endpoints.' % location,
              summary: 'Thanos Rule is having high number of DNS failures.',
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(thanos_rule_alertmanagers_dns_failures_total{%(selector)s}[5m]))
              /
                sum by (%(aggregator)s) (rate(thanos_rule_alertmanagers_dns_lookups_total{%(selector)s}[5m]))
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
              description: 'Thanos Rule {{$labels.instance}}%shas {{$value | humanize}}%% rule groups that did not evaluate for at least 10x of their expected interval.' % location,
              summary: 'Thanos Rule has rule groups that did not evaluate for 10 intervals.',
            },
            expr: |||
              time() -  max by (%(aggregator)s, group) (prometheus_rule_group_last_evaluation_timestamp_seconds{%(selector)s})
              >
              10 * max by (%(aggregator)s, group) (prometheus_rule_group_interval_seconds{%(selector)s})
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
              description: 'Thanos Rule {{$labels.instance}}%sdid not perform any rule evaluations in the past 2 minutes.' % location,
              summary: 'Thanos Rule did not perform any rule evaluations.',
            },
            expr: |||
              sum by (%(aggregator)s) (rate(prometheus_rule_evaluations_total{%(selector)s}[2m])) <= 0
                and
              sum by (%(aggregator)s) (thanos_rule_loaded_rules{%(selector)s}) > 0
            ||| % thanos.rule,
            'for': '3m',
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
