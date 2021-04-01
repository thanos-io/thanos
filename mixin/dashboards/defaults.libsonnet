{
  local thanos = self,
  local grafanaDashboards = super.grafanaDashboards,
  local grafana = import 'grafonnet/grafana.libsonnet',
  local template = grafana.template,

  dashboard:: {
    prefix: 'Thanos / ',
    tags: error 'must provide dashboard tags',
    timezone: 'UTC',
  },

  // Automatically add a uid to each dashboard based on the base64 encoding
  // of the file name and set the timezone to be 'default'.
  grafanaDashboards:: {
    local component = std.split(filename, '.')[0],
    [filename]: grafanaDashboards[filename] {
      uid: std.md5(filename),
      timezone: thanos.dashboard.timezone,
      tags: thanos.dashboard.tags,

      // Modify tooltip to only show a single value
      rows: [
        row {
          panels: [
            panel {
              tooltip+: {
                shared: false,
              },
            }
            for panel in super.panels
          ],
        }
        for row in super.rows
      ],

      templating+: {
        list+: [
          template.interval(
            'interval',
            '5m,10m,30m,1h,6h,12h,auto',
            label='interval',
            current='5m',
          ),
        ],
      },
    } {
      templating+: {
        list+: [
          template.new(
            level,
            '$datasource',
            'label_values(%s, %s)' % [thanos.targetGroups[level], level],
            label=level,
            refresh=1,
            sort=2,
          )
          for level in std.objectFields(thanos.targetGroups)
        ],
      },
    } + if std.objectHas(thanos[component], 'selector') then {
      templating+: {
        local name = 'job',
        local selector = std.join(', ', thanos.dashboard.selector + [thanos[component].selector]),
        list+: [
          template.new(
            name,
            '$datasource',
            'label_values(up{%s}, %s)' % [selector, name],
            label=name,
            refresh=1,
            sort=2,
            current='all',
            allValues=null,
            includeAll=true
          ),
        ],
      },
    } else {}
    for filename in std.objectFields(grafanaDashboards)
  },
}
