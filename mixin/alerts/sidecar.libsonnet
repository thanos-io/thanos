{
  local thanos = self,
  sidecar+:: {
    selector: error 'must provide selector for Thanos Sidecar alerts',
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-sidecar.rules',
        rules: [
          {
            alert: 'ThanosSidecarPrometheusDown',
            annotations: {
              description: 'Thanos Sidecar {{$labels.job}} {{$labels.pod}} cannot connect to Prometheus.',
              summary: 'Thanos Sidecar cannot connect to Prometheus',
            },
            expr: |||
              sum by (job, pod) (thanos_sidecar_prometheus_up{%(selector)s} == 0)
            ||| % thanos.sidecar,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosSidecarUnhealthy',
            annotations: {
              description: 'Thanos Sidecar {{$labels.job}} {{$labels.pod}} is unhealthy for {{ $value }} seconds.',
              summary: 'Thanos Sidecar is unhealthy.',
            },
            expr: |||
              time() - max(thanos_sidecar_last_heartbeat_success_time_seconds{%(selector)s}) by (job, pod) >= 600
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
