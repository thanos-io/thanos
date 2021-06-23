{
  local thanos = self,
  sidecar+:: {
    selector: error 'must provide selector for Thanos Sidecar alerts',
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job', 'instance']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.sidecar == null then [] else [
      local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '';
      {
        name: 'thanos-sidecar',
        rules: [
          {
            alert: 'ThanosSidecarPrometheusDown',
            annotations: {
              description: 'Thanos Sidecar {{$labels.instance}}%s cannot connect to Prometheus.' % location,
              summary: 'Thanos Sidecar cannot connect to Prometheus',
            },
            expr: |||
              thanos_sidecar_prometheus_up{%(selector)s} == 0
            ||| % thanos.sidecar,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
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
            alert: 'ThanosSidecarUnhealthy',
            annotations: {
              description: 'Thanos Sidecar {{$labels.instance}}%s is unhealthy for more than {{$value}} seconds.' % location,
              summary: 'Thanos Sidecar is unhealthy.',
            },
            expr: |||
              time() - max by (%(dimensions)s) (thanos_sidecar_last_heartbeat_success_time_seconds{%(selector)s}) >= 240
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
