{
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-sidecar.rules',
        rules: [
          {
            alert: 'ThanosSidecarUnhealthy',
            annotations: {
              message: 'Thanos Sidecar {{$labels.job}} {{$labels.pod}} is unhealthy for {{ $value }} seconds.',
            },
            expr: |||
              count(time() - max(thanos_sidecar_last_heartbeat_success_time_seconds{%(thanosSidecarSelector)s}) by (job, pod) >= 300) > 0
            ||| % $._config,
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
