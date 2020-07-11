{
  local thanos = self,
  compact+:: {
    selector: error 'must provide selector for Thanos Compact alerts',
    compactionErrorThreshold: 5,
    bucketOpsErrorThreshold: 5,
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-compact.rules',
        rules: [
          {
            alert: 'ThanosCompactMultipleRunning',
            annotations: {
              message: 'No more than one Thanos Compact instance should be running at once. There are {{ $value }}',
            },
            expr: 'sum(up{%(selector)s}) > 1' % thanos.compact,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHalted',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} has failed to run and now is halted.',
            },
            expr: 'thanos_compactor_halted{%(selector)s} == 1' % thanos.compact,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHighCompactionFailures',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} is failing to execute {{ $value | humanize }}% of compactions.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_compact_group_compactions_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_compact_group_compactions_total{%(selector)s}[5m]))
              * 100 > %(compactionErrorThreshold)s
              )
            ||| % thanos.compact,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactBucketHighOperationFailures',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} Bucket is failing to execute {{ $value | humanize }}% of operations.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              * 100 > %(bucketOpsErrorThreshold)s
              )
            ||| % thanos.compact,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactHasNotRun',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} has not uploaded anything for 24 hours.',
            },
            expr: '(time() - max(max_over_time(thanos_objstore_bucket_last_successful_upload_time{%(selector)s}[24h]))) / 60 / 60 > 24' % thanos.compact,
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
