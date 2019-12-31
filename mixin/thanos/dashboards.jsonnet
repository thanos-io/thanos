local dashboards = (
  (import 'mixin.libsonnet') +
  (import 'defaults.libsonnet')
).grafanaDashboards;

{
  [name]: dashboards[name]
  for name in std.objectFields(dashboards)
}
