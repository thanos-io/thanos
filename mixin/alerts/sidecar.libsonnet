{
  local thanos = self,
  sidecar+:: {
    selector: error 'must provide selector for Thanos Sidecar alerts',
    thanosPrometheusCommonDimensions: error 'must provide commonDimensions between Thanos and Prometheus metrics for Sidecar alerts',
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job', 'instance']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.sidecar == null then [] else [
      local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '';
      {
        name: 'thanos-sidecar',
        rules: [
          {
            alert: 'ThanosSidecarBucketOperationsFailed',
            annotations: {
              description: 'Thanos Sidecar {{$labels.instance}}%s bucket operations are failing' % location,
              summary: 'Thanos Sidecar bucket operations are failing',
            },
            expr: |||
              sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m])) > 0
            ||| % thanos.sidecar,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosSidecarNoConnectionToStartedPrometheus',
            annotations: {
              description: 'Thanos Sidecar {{$labels.instance}}%s is unhealthy.' % location,
              summary: 'Thanos Sidecar cannot access Prometheus, even though Prometheus seems healthy and has reloaded WAL.',
            },
            expr: |||
              thanos_sidecar_prometheus_up{%(selector)s} == 0
              AND on (%(thanosPrometheusCommonDimensions)s)
              prometheus_tsdb_data_replay_duration_seconds != 0
            ||| % thanos.sidecar,
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
