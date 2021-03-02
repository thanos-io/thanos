local capitalize(str) = std.asciiUpper(std.substr(str, 0, 1)) + std.asciiLower(std.substr(str, 1, std.length(str)));
local titlize(str) = std.join('', std.map(capitalize, std.split(str, '_')));
{
  local thanos = self,

  // We build alerts for the presence of all these jobs.
  jobs:: {
    ['Thanos%s' % titlize(component)]: thanos[component].selector
    for component in std.objectFieldsAll(thanos)
    if component != 'jobs' && std.type(thanos[component]) == 'object' && std.objectHas(thanos[component], 'selector')
  },

  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-component-absent',
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
              description: '%s has disappeared from {{$labels.namespace}}. Prometheus target for the component cannot be discovered.' % name,
              summary: 'Thanos component has disappeared from {{$labels.namespace}}.',
            },
          }
          for name in std.objectFields(thanos.jobs)
        ],
      },
    ],
  },
}
