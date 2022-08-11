local utils = import '../lib/utils.libsonnet';

{
  local thanos = self,
  bucketReplicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    errorThreshold: 10,
    p99LatencyThreshold: 20,
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.bucketReplicate == null then [] else [
      local location = utils.location(thanos.targetGroups);
      local labels = utils.labelsTemplate(thanos.bucketReplicate.dimensions, thanos.targetGroups);

      {
        name: 'thanos-bucket-replicate',
        rules: [
          {
            alert: 'ThanosBucketReplicateErrorRate',
            annotations: {
              description: 'Thanos Replicate %s%s is failing to run, {{$value | humanize}}%% of attempts failed.' % [labels, location],
              summary: 'Thanos Replicate is failing to run%s.' % location,
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_replicate_replication_runs_total{result="error", %(selector)s}[5m]))
              / on (%(dimensions)s) group_left
                sum by (%(dimensions)s) (rate(thanos_replicate_replication_runs_total{%(selector)s}[5m]))
              ) * 100 >= %(errorThreshold)s
            ||| % thanos.bucketReplicate,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosBucketReplicateRunLatency',
            annotations: {
              description: 'Thanos Replicate %s%s has a 99th percentile latency of {{$value}} seconds for the replicate operations.' % [labels, location],
              summary: 'Thanos Replicate has a high latency for replicate operations.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(dimensions)s) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m]))) > %(p99LatencyThreshold)s
              and
                sum by (%(dimensions)s) (rate(thanos_replicate_replication_run_duration_seconds_bucket{%(selector)s}[5m])) > 0
              )
            ||| % thanos.bucketReplicate,
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
