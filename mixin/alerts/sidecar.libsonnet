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
              message: 'Thanos Sidecar {{$labels.job}} {{$labels.pod}} cannot connect to Prometheus.',
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
              message: 'Thanos Sidecar {{$labels.job}} {{$labels.pod}} is unhealthy for {{ $value }} seconds.',
            },
            expr: |||
              count(time() - max(thanos_sidecar_last_heartbeat_success_time_seconds{%(selector)s}) by (job, pod) >= 300) > 0
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
