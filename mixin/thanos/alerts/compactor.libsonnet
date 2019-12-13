{
  local thanos = self,
  compactor+:: {
    jobPrefix: error 'must provide job prefix for Thanos Compact alerts',
    selector: error 'must provide selector for Thanos Compact alerts',
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-compactor.rules',
        rules: [
          {
            alert: 'ThanosCompactorMultipleCompactsAreRunning',
            annotations: {
              message: 'You should never run more than one Thanos Compact at once. You have {{ $value }}',
            },
            expr: 'sum(up{%(selector)s}) > 1' % thanos.compactor,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactorHalted',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} has failed to run and now is halted.',
            },
            expr: 'thanos_compactor_halted{%(selector)s} == 1' % thanos.compactor,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactorHighCompactionFailures',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} is failing to execute {{ $value | humanize }}% of compactions.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_compact_group_compactions_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_compact_group_compactions_total{%(selector)s}[5m]))
              * 100 > 5
              )
            ||| % thanos.compactor,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactorBucketHighOperationFailures',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} Bucket is failing to execute {{ $value | humanize }}% of operations.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[5m]))
              * 100 > 5
              )
            ||| % thanos.compactor,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosCompactorHasNotRun',
            annotations: {
              message: 'Thanos Compact {{$labels.job}} has not uploaded anything for 24 hours.',
            },
            expr: '(time() - max(thanos_objstore_bucket_last_successful_upload_time{%(selector)s})) / 60 / 60 > 24' % thanos.compactor,
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
