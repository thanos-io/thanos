{
  local thanos = self,
  sidecar+:: {
    selector: error 'must provide selector for Thanos Sidecar alerts',
  },
  prometheusAlerts+:: {
    groups+: if thanos.sidecar == null then [] else [
      {
        name: 'thanos-sidecar',
        rules: [
          {
            alert: 'ThanosSidecarPrometheusDown',
            annotations: {
              description: 'Thanos Sidecar {{$labels.job}} {{$labels.instance}} cannot connect to Prometheus.',
              summary: 'Thanos Sidecar cannot connect to Prometheus',
            },
            expr: |||
              sum by (job, instance) (thanos_sidecar_prometheus_up{%(selector)s} == 0)
            ||| % thanos.sidecar,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosSidecarBucketOperationsFailed',
            annotations: {
              description: 'Thanos Sidecar {{$labels.job}} {{$labels.instance}} bucket operations are failing',
              summary: 'Thanos Sidecar bucket operations are failing',
            },
            expr: |||
              rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[5m]) > 0
            ||| % thanos.sidecar,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosSidecarUnhealthy',
            annotations: {
              description: 'Thanos Sidecar {{$labels.job}} {{$labels.instance}} is unhealthy for {{ $value }} seconds.',
              summary: 'Thanos Sidecar is unhealthy.',
            },
            expr: |||
              time() - max(thanos_sidecar_last_heartbeat_success_time_seconds{%(selector)s}) by (job, instance) >= 600
            ||| % thanos.sidecar,
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
