{
  local thanos = self,
  // Hierarcies is a way to help mixin users to add high level hierarcies to their alerts and dashboards.
  // The key in the key-value pair will be used as label name in the alerts and variable name in the dashboards.
  // The value in the key-value pair will be used as a query to fetch available values for the given label name.
  hierarcies+:: {
    // For example:
    // cluster: 'find_mi_cluster_bitte',
    // namespace: 'thanos_status',
  },
  query+:: {
    selector: 'job=~"thanos-query.*"',
    title: '%(prefix)sQuery' % $.dashboard.prefix,
  },
  store+:: {
    selector: 'job=~"thanos-store.*"',
    title: '%(prefix)sStore' % $.dashboard.prefix,
  },
  receive+:: {
    selector: 'job=~"thanos-receive.*"',
    title: '%(prefix)sReceive' % $.dashboard.prefix,
  },
  rule+:: {
    selector: 'job=~"thanos-rule.*"',
    title: '%(prefix)sRule' % $.dashboard.prefix,
  },
  compact+:: {
    selector: 'job=~"thanos-compact.*"',
    title: '%(prefix)sCompact' % $.dashboard.prefix,
  },
  sidecar+:: {
    selector: 'job=~"thanos-sidecar.*"',
    title: '%(prefix)sSidecar' % $.dashboard.prefix,
  },
  bucket_replicate+:: {
    selector: 'job=~"thanos-bucket-replicate.*"',
    title: '%(prefix)sBucketReplicate' % $.dashboard.prefix,
  },
  dashboard+:: {
    prefix: 'Thanos / ',
    tags: ['thanos-mixin'],
    selector: ['%s="$%s"' % [level, level] for level in std.objectFields(thanos.hierarcies)],
    aggregator: ['%s' % level for level in std.objectFields(thanos.hierarcies)],

    overview+:: {
      title: '%(prefix)sOverview' % $.dashboard.prefix,
      selector: std.join(', ', thanos.dashboard.selector),
      aggregator: std.join(', ', thanos.dashboard.aggregator + ['job']),
    },
  },
}
