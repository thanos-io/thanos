{
  local thanos = self,

  // We build alerts for the presence of all these jobs.
  jobs:: {
    ThanosQuery: thanos.query.selector,
    ThanosStore: thanos.store.selector,
    ThanosReceive: thanos.receive.selector,
    ThanosRule: thanos.rule.selector,
    ThanosCompact: thanos.compact.selector,
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
              description: '%s has disappeared from Prometheus target discovery.' % name,
              summary: 'thanos component has disappeared from Prometheus target discovery.',
            },
          }
          for name in std.objectFields(thanos.jobs)
        ],
      },
    ],
  },
}
