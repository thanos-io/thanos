local utils = import '../lib/utils.libsonnet';
{
  local thanos = self,
  local grafanaDashboards = super.grafanaDashboards,

  dashboard:: {
    prefix: 'Thanos / ',
    tags: error 'must provide dashboard tags',
    timezone: 'UTC',
    instance_name_filter: '',
  },

  // Automatically add a uid to each dashboard based on the base64 encoding
  // of the file name and set the timezone to be 'default'.
  grafanaDashboards:: {
    local component = utils.sanitizeComponentName(std.split(filename, '.')[0]),

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
        // Add optional filter to the datasource template variable
        list: [
          if variable.name == 'datasource'
          then variable { regex: thanos.dashboard.instance_name_filter }
          else variable
          for variable in super.list
        ] + [
          utils.intervalVariable('interval', '5m,10m,30m,1h,6h,12h,auto', '5m'),
        ],
      },
    } {
      templating+: {
        list+: [
          utils.queryVariable(level, 'label_values(%s, %s)' % [thanos.targetGroups[level], level])
          for level in std.objectFields(thanos.targetGroups)
        ],
      },
    } + if std.objectHas(thanos[component], 'selector') then {
      templating+: {
        local name = 'job',
        local selector = std.join(', ', thanos.dashboard.selector + [thanos[component].selector]),
        list+: [
          utils.queryVariable(name, 'label_values(up{%s}, %s)' % [selector, name], current='all'),
        ],
      },
    } else {}
    for filename in std.objectFields(grafanaDashboards)
  },
}
