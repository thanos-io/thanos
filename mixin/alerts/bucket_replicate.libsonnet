{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    errorThreshold: 10,
    p99LatencyThreshold: 20,
  },
  prometheusAlerts+:: {
    groups+: if thanos.bucket_replicate == null then [] else [
      {
        name: 'thanos-bucket-replicate',
        rules: [
          {
            alert: 'ThanosBucketReplicateErrorRate',
            annotations: {
              description: 'Thanos Replicate in {{$labels.namespace}} failing to run, {{$value | humanize}}% of attempts failed.',
              summary: 'Thanose Replicate in {{$labels.namespace}} is failing to run.',
            },
            expr: |||
              (
                sum by (namespace, job) (rate(thanos_replicate_replication_runs_total{result="error", %(selector)s}[5m]))
              / on (namespace) group_left
                sum by (namespace, job) (rate(thanos_replicate_replication_runs_total{%(selector)s}[5m]))
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
              description: 'Thanos Replicate {{$labels.namespace}}/{{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for the replicate operations.',
              summary: 'Thanos Replicate has a high latency for replicate operations.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (namespace, job, le) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m]))) > %(p99LatencyThreshold)s
              and
                sum by (namespace, job) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m])) > 0
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
