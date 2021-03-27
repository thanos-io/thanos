local capitalize(str) = std.asciiUpper(std.substr(str, 0, 1)) + std.asciiLower(std.substr(str, 1, std.length(str)));
local titlize(str) = std.join('', std.map(capitalize, std.split(str, '_')));

local components = ['query', 'receive', 'rule', 'compact', 'store', 'bucket_replicate', 'sidecar'];
{
  local thanos = self,

  // We build alerts for the presence of all these jobs.
  jobs:: {
    ['Thanos%s' % titlize(component)]: thanos[component].selector
    for component in std.objectFieldsAll(thanos)
    if component != 'jobs' && std.type(thanos[component]) == 'object' && std.member(components, component)
  },

  prometheusAlerts+:: {
    local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' from %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '',
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
              description: '%s has disappeared%s. Prometheus target for the component cannot be discovered.' % [name, location],
              summary: 'Thanos component has disappeared%s.' % location,
            },
          }
          for name in std.objectFields(thanos.jobs)
        ],
      },
    ],
  },
}
