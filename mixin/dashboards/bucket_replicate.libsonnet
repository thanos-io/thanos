local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    title: error 'must provide title for Thanos Bucket Replicate dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job="$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.bucket_replicate != null then 'bucket_replicate.json']:
      g.dashboard(thanos.bucket_replicate.title)
      .addRow(
        g.row('Bucket Replicate Runs')
        .addPanel(
          g.panel('Rate') +
          g.qpsErrTotalPanel(
            'thanos_replicate_replication_runs_total{result="error", %s}' % thanos.bucket_replicate.dashboard.selector,
            'thanos_replicate_replication_runs_total{%s}' % thanos.bucket_replicate.dashboard.selector,
            thanos.rule.dashboard.dimensions
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows rate of errors.') +
          g.queryPanel(
            'sum by (%(dimensions)s, result) (rate(thanos_replicate_replication_runs_total{result="error", %(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
            '{{result}}'
          ) +
          { yaxes: g.yaxes('percentunit') } +
          g.stack
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to run a replication cycle.') +
          g.latencyPanel(
            'thanos_replicate_replication_run_duration_seconds',
            'result="success",  %s' % thanos.bucket_replicate.dashboard.selector,
            thanos.rule.dashboard.dimensions
          )
        )
      )
      .addRow(
        g.row('Bucket Replication')
        .addPanel(
          g.panel('Metrics') +
          g.queryPanel(
            [
              'sum by (%(dimensions)s) (rate(blocks_meta_synced{state="loaded", %(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
              'sum by (%(dimensions)s) (rate(blocks_meta_synced{state="failed", %(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
              'sum by (%(dimensions)s) (rate(thanos_replicate_blocks_already_replicated_total{%(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
              'sum by (%(dimensions)s) (rate(thanos_replicate_blocks_replicated_total{%(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
              'sum by (%(dimensions)s) (rate(thanos_replicate_objects_replicated_total{%(selector)s}[$interval]))' % thanos.bucket_replicate.dashboard,
            ],
            ['meta loads', 'partial meta reads', 'already replicated blocks', 'replicated blocks', 'replicated objects']
          )
        )
      ),
  },
}
