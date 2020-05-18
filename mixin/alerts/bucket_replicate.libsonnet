{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    errorThreshold: 10,
    p99LatencyThreshold: 20,
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-bucket-replicate.rules',
        rules: [
          {
            alert: 'ThanosBucketReplicateIsDown',
            expr: |||
              absent(up{%(selector)s})
            ||| % thanos.bucket_replicate,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: 'Thanos Replicate has disappeared from Prometheus target discovery.',
            },
          },
          {
            alert: 'ThanosBucketReplicateErrorRate',
            annotations: {
              message: 'Thanos Replicate failing to run, {{ $value | humanize }}% of attempts failed.',
            },
            expr: |||
              (
                sum(rate(thanos_replicate_replication_runs_total{result="error", %(selector)s}[5m]))
              / on (namespace) group_left
                sum(rate(thanos_replicate_replication_runs_total{%(selector)s}[5m]))
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
              message: 'Thanos Replicate {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for the replicate operations.',
            },
            expr: |||
              (
                histogram_quantile(0.9, sum by (job, le) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m]))) > %(p99LatencyThreshold)s
              and
                sum by (job) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m])) > 0
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
