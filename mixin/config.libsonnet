{
  local thanos = self,
  // TargetGroups is a way to help mixin users to add high level target grouping to their alerts and dashboards.
  // With the help of TargetGroups you can use a single observability stack to monitor several Thanos instances.
  // The key in the key-value pair will be used as "label name" in the alerts and variable name in the dashboards.
  // The value in the key-value pair will be used as a query to fetch available values for the given label name.
  targetGroups+:: {
    // For example for given following groups,
    // namespace: 'thanos_status',
    // cluster: 'find_mi_cluster_bitte',
    // zone: 'an_i_in_da_zone',
    // region: 'losing_my_region',
    // will generate queriers for the alerts as follows:
    //  (
    //     sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_failures_total{job=~"thanos-compact.*"}[5m]))
    //   /
    //     sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_total{job=~"thanos-compact.*"}[5m]))
    //   * 100 > 5
    //   )
    //
    // AND for the dashborads:
    //
    // sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_failures_total{cluster=\"$cluster\", namespace=\"$namespace\", region=\"$region\", zone=\"$zone\", job=\"$job\"}[$interval]))
    // /
    // sum by (cluster, namespace, region, zone, job) (rate(thanos_compact_group_compactions_total{cluster=\"$cluster\", namespace=\"$namespace\", region=\"$region\", zone=\"$zone\", job=\"$job\"}[$interval]))
  },
  query+:: {
    selector: 'job=~".*thanos-query.*"',
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  queryFrontend+:: {
    selector: 'job=~".*thanos-query-frontend.*"',
    title: '%(prefix)sQuery Frontend' % $.dashboard.prefix,
  },
  store+:: {
    selector: 'job=~".*thanos-store.*"',
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receive+:: {
    selector: 'job=~".*thanos-receive.*"',
    title: '%(prefix)sReceive' % $.dashboard.prefix,
  },
  rule+:: {
    selector: 'job=~".*thanos-rule.*"',
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compact+:: {
    selector: 'job=~".*thanos-compact.*"',
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    selector: 'job=~".*thanos-sidecar.*"',
    thanosPrometheusCommonDimensions: 'namespace, pod',
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  bucketReplicate+:: {
    selector: 'job=~".*thanos-bucket-replicate.*"',
    title: '%(prefix)sBucketReplicate' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    timezone: 'UTC',
    selector: ['%s="$%s"' % [level, level] for level in std.objectFields(thanos.targetGroups)],
    dimensions: ['%s' % level for level in std.objectFields(thanos.targetGroups)],

    overview+:: {
      title: '%(prefix)sOverview' % $.dashboard.prefix,
      selector: std.join(', ', thanos.dashboard.selector),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
}
