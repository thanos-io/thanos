{
  [group.name]: group
  for group in (
    (import 'mixin.libsonnet') +
    (import 'defaults.libsonnet')
  ).prometheusAlerts.groups
}
