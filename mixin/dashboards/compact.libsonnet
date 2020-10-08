local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  compact+:: {
    jobPrefix: error 'must provide job prefix for Thanos Compact dashboard',
    selector: error 'must provide selector for Thanos Compact dashboard',
    title: error 'must provide title for Thanos Compact dashboard',
  },
  local clusterNamespaceJob = '%s="$cluster",namespace="$namespace",job=~"$job"' % thanos.dashboard.clusterLabel,
  local clusterNamespaceCompactSelector = '%s="$cluster",namespace="$namespace",%s' % [thanos.dashboard.clusterLabel, thanos.compact.selector],
  grafanaDashboards+:: {
    'compact.json':
      g.dashboard(thanos.compact.title)
      .addRow(
        g.row('Group Compaction')
        .addPanel(
          g.panel(
            'Rate',
            'Shows rate of execution for compactions against blocks that are stored in the bucket by compaction group.'
          ) +
          g.queryPanel(
            'sum(rate(thanos_compact_group_compactions_total{%s}[$interval])) by (job, group)' % clusterNamespaceJob,
            'compaction {{job}} {{group}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel(
            'Errors',
            'Shows ratio of errors compared to the total number of executed compactions against blocks that are stored in the bucket.'
          ) +
          g.qpsErrTotalPanel(
            'thanos_compact_group_compactions_failures_total{%s}' % clusterNamespaceJob,
            'thanos_compact_group_compactions_total{%s}' % clusterNamespaceJob,
          )
        )
      )
      .addRow(
        g.row('Downsample')
        .addPanel(
          g.panel(
            'Rate',
            'Shows rate of execution for downsampling against blocks that are stored in the bucket by compaction group.'
          ) +
          g.queryPanel(
            'sum(rate(thanos_compact_downsample_total{%s}[$interval])) by (job, group)' % clusterNamespaceJob,
            'downsample {{job}} {{group}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed downsampling against blocks that are stored in the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_compact_downsample_failed_total{%s}' % clusterNamespaceJob,
            'thanos_compact_downsample_total{%s}' % clusterNamespaceJob,
          )
        )
      )
      .addRow(
        g.row('Garbage Collection')
        .addPanel(
          g.panel(
            'Rate',
            'Shows rate of execution for removals of blocks if their data is available as part of a block with a higher compaction level.'
          ) +
          g.queryPanel(
            'sum(rate(thanos_compact_garbage_collection_total{%s}[$interval])) by (job)' % clusterNamespaceJob,
            'garbage collection {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed garbage collections.') +
          g.qpsErrTotalPanel(
            'thanos_compact_garbage_collection_failures_total{%s}' % clusterNamespaceJob,
            'thanos_compact_garbage_collection_total{%s}' % clusterNamespaceJob,
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute garbage collection in quantiles.') +
          g.latencyPanel('thanos_compact_garbage_collection_duration_seconds', clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('Sync Meta')
        .addPanel(
          g.panel(
            'Rate',
            'Shows rate of execution for all meta files from blocks in the bucket into the memory.'
          ) +
          g.queryPanel(
            'sum(rate(thanos_blocks_meta_syncs_total{%s}[$interval])) by (job)' % clusterNamespaceJob,
            'sync {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed meta file sync.') +
          g.qpsErrTotalPanel(
            'thanos_blocks_meta_sync_failures_total{%s}' % clusterNamespaceJob,
            'thanos_blocks_meta_syncs_total{%s}' % clusterNamespaceJob,
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute meta file sync, in quantiles.') +
          g.latencyPanel('thanos_blocks_meta_sync_duration_seconds', clusterNamespaceJob)
        )
      )
      .addRow(
        g.row('Object Store Operations')
        .addPanel(
          g.panel('Rate', 'Shows rate of execution for operations against the bucket.') +
          g.queryPanel(
            'sum(rate(thanos_objstore_bucket_operations_total{%s}[$interval])) by (job, operation)' % clusterNamespaceJob,
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed operations against the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_objstore_bucket_operation_failures_total{%s}' % clusterNamespaceJob,
            'thanos_objstore_bucket_operations_total{%s}' % clusterNamespaceJob,
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute operations against the bucket, in quantiles.') +
          g.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', clusterNamespaceJob)
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template(thanos.dashboard.clusterLabel, 'up', '', false, '', if thanos.dashboard.showMultiCluster then '' else '2') +
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', clusterNamespaceCompactSelector, true, '%(jobPrefix)s.*' % thanos.compact) +
      g.template('pod', 'kube_pod_info', '%(clusterLabel)s="$cluster",namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % [thanos.dashboard.clusterLabel, thanos.compact.jobPrefix], true, '.*'),

    __overviewRows__+:: [
      g.row('Compact')
      .addPanel(
        g.panel(
          'Compaction Rate',
          'Shows rate of execution for compactions against blocks that are stored in the bucket by compaction group.'
        ) +
        g.queryPanel(
          'sum(rate(thanos_compact_group_compactions_total{%s}[$interval])) by (job)' % clusterNamespaceCompactSelector,
          'compaction {{job}}'
        ) +
        g.stack +
        g.addDashboardLink(thanos.compact.title)
      )
      .addPanel(
        g.panel(
          'Compaction Errors',
          'Shows ratio of errors compared to the total number of executed compactions against blocks that are stored in the bucket.'
        ) +
        g.qpsErrTotalPanel(
          'thanos_compact_group_compactions_failures_total{%s}' % clusterNamespaceCompactSelector,
          'thanos_compact_group_compactions_total{%s}' % clusterNamespaceCompactSelector,
        ) +
        g.addDashboardLink(thanos.compact.title)
      ) +
      g.collapse,
    ],
  },
}
