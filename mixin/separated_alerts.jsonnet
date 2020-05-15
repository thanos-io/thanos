{
  [group.name]: group
  for group in
    (
      import 'mixin.libsonnet'
    ).prometheusAlerts.groups
}
