local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  compact+:: {
    selector: error 'must provide selector for Thanos Compact dashboard',
    title: error 'must provide title for Thanos Compact dashboard',
  },
  grafanaDashboards+:: {
    local selector = std.join(', ', thanos.dashboard.commonSelector + ['job="$job"']),
    local aggregator = std.join(', ', thanos.dashboard.commonSelector + ['job']),

    [if thanos.compact != null then 'compact.json']:
      g.dashboard(thanos.compact.title)
      .addRow(
        g.row('Group Compaction')
        .addPanel(
          g.panel(
            'Rate',
            'Shows rate of execution for compactions against blocks that are stored in the bucket by compaction group.'
          ) +
          g.queryPanel(
            'sum by (%s, group) (rate(thanos_compact_group_compactions_total{%s}[$interval]))' % [aggregator, selector],
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
            'thanos_compact_group_compactions_failures_total{%s}' % selector,
            'thanos_compact_group_compactions_total{%s}' % selector,
            aggregator
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
            'sum by (%s, group) (rate(thanos_compact_downsample_total{%s}[$interval]))' % [aggregator, selector],
            'downsample {{job}} {{group}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed downsampling against blocks that are stored in the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_compact_downsample_failed_total{%s}' % selector,
            'thanos_compact_downsample_total{%s}' % selector,
            aggregator
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
            'sum by (%s) (rate(thanos_compact_garbage_collection_total{%s}[$interval]))' % [aggregator, selector],
            'garbage collection {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed garbage collections.') +
          g.qpsErrTotalPanel(
            'thanos_compact_garbage_collection_failures_total{%s}' % selector,
            'thanos_compact_garbage_collection_total{%s}' % selector,
            aggregator
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute garbage collection in quantiles.') +
          g.latencyPanel('thanos_compact_garbage_collection_duration_seconds', selector, aggregator)
        )
      )
      .addRow(
        g.row('Blocks deletion')
        .addPanel(
          g.panel(
            'Deletion Rate',
            'Shows deletion rate of blocks already marked for deletion.'
          ) +
          g.queryPanel(
            'sum by (%s) (rate(thanos_compact_blocks_cleaned_total{%s}[$interval]))' % [aggregator, selector],
            'Blocks cleanup {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel(
            'Deletion Error Rate',
            'Shows deletion failures rate of blocks already marked for deletion.'
          ) +
          g.queryPanel(
            'sum by (%s) (rate(thanos_compact_block_cleanup_failures_total{%s}[$interval]))' % [aggregator, selector],
            'Blocks cleanup failures {{job}}'
          )
        )
        .addPanel(
          g.panel(
            'Marking Rate',
            'Shows rate at which blocks are marked for deletion (from GC and retention policy).'
          ) +
          g.queryPanel(
            'sum by (%s) (rate(thanos_compact_blocks_marked_for_deletion_total{%s}[$interval]))' % [aggregator, selector],
            'Blocks marked {{job}}'
          )
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
            'sum by (%s) (rate(thanos_blocks_meta_syncs_total{%s}[$interval]))' % [aggregator, selector],
            'sync {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed meta file sync.') +
          g.qpsErrTotalPanel(
            'thanos_blocks_meta_sync_failures_total{%s}' % selector,
            'thanos_blocks_meta_syncs_total{%s}' % selector,
            aggregator
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute meta file sync, in quantiles.') +
          g.latencyPanel('thanos_blocks_meta_sync_duration_seconds', selector, aggregator)
        )
      )
      .addRow(
        g.row('Object Store Operations')
        .addPanel(
          g.panel('Rate', 'Shows rate of execution for operations against the bucket.') +
          g.queryPanel(
            'sum by (%s, operation) (rate(thanos_objstore_bucket_operations_total{%s}[$interval]))' % [aggregator, selector],
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed operations against the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_objstore_bucket_operation_failures_total{%s}' % selector,
            'thanos_objstore_bucket_operations_total{%s}' % selector,
            aggregator
          )
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute operations against the bucket, in quantiles.') +
          g.latencyPanel('thanos_objstore_bucket_operation_duration_seconds', selector, aggregator)
        )
      )
      .addRow(
        g.resourceUtilizationRow(selector, aggregator)
      ),

    __overviewRows__+:: [
      g.row('Compact')
      .addPanel(
        g.panel(
          'Compaction Rate',
          'Shows rate of execution for compactions against blocks that are stored in the bucket by compaction group.'
        ) +
        g.queryPanel(
          'sum by (%s) (rate(thanos_compact_group_compactions_total{%s}[$interval]))' % [aggregator, selector],
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
          'thanos_compact_group_compactions_failures_total{%s}' % selector,
          'thanos_compact_group_compactions_total{%s}' % selector,
          aggregator
        ) +
        g.addDashboardLink(thanos.compact.title)
      ) +
      g.collapse,
    ],
  },
}
