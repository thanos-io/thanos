{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    errorThreshold: 10,
    p99LatencyThreshold: 20,
    aggregator: std.join(', ', std.objectFields(thanos.hierarcies) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.bucket_replicate == null then [] else [
      local location = if std.length(std.objectFields(thanos.hierarcies)) > 0 then ' in' + std.join('/', ['{{labels.%s}}' % level for level in std.objectFields(thanos.hierarcies)]) else ' ';
      {
        name: 'thanos-bucket-replicate',
        rules: [
          {
            alert: 'ThanosBucketReplicateErrorRate',
            annotations: {
              description: 'Thanos Replicate is failing to run%s, {{$value | humanize}}%% of attempts failed.' % location,
              summary: 'Thanose Replicate is failing to run in %s.' % location,
            },
            expr: |||
              (
                sum by (%(aggregator)s) (rate(thanos_replicate_replication_runs_total{result="error", %(selector)s}[5m]))
              / on (%(aggregator)s) group_left
                sum by (%(aggregator)s) (rate(thanos_replicate_replication_runs_total{%(selector)s}[5m]))
              ) * 100 >= %(errorThreshold)s
            ||| % thanos.bucket_replicate,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosBucketReplicateRunLatency',
            annotations: {
              description: 'Thanos Replicate {{$labels.job}}%shas a 99th percentile latency of {{$value}} seconds for the replicate operations.' % location,
              summary: 'Thanos Replicate has a high latency for replicate operations.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(aggregator)s) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m]))) > %(p99LatencyThreshold)s
              and
                sum by (%(aggregator)s) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m])) > 0
              )
            ||| % thanos.bucket_replicate,
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
