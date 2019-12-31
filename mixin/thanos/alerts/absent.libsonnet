{
  local thanos = self,

  // We build alerts for the presence of all these jobs.
  jobs:: {
    ThanosQuerier: thanos.querier.selector,
    ThanosStore: thanos.store.selector,
    ThanosReceiver: thanos.receiver.selector,
    ThanosRuler: thanos.ruler.selector,
    ThanosCompactor: thanos.compactor.selector,
    ThanosSidecar: thanos.sidecar.selector,
  },

  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-component-absent.rules',
        rules: [
          {
            alert: '%sIsDown' % name,
            expr: |||
              absent(up{%s} == 1)
            ||| % thanos.jobs[name],
            'for': '5m',
            labels: {
              severity: 'critical',
            },
            annotations: {
              message: '%s has disappeared from Prometheus target discovery.' % name,
            },
          }
          for name in std.objectFields(thanos.jobs)
        ],
      },
    ],
  },
}
