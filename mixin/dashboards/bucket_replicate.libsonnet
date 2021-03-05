local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  bucket_replicate+:: {
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    title: error 'must provide title for Thanos Bucket Replicate dashboard',
  },
  grafanaDashboards+:: {
    local selector = std.join(', ', thanos.dashboard.commonSelector + ['job="$job"']),

    [if thanos.bucket_replicate != null then 'bucket_replicate.json']:
      g.dashboard(thanos.bucket_replicate.title)
      .addRow(
        g.row('Bucket Replicate Runs')
        .addPanel(
          g.panel('Rate') +
          g.qpsErrTotalPanel(
            'thanos_replicate_replication_runs_total{result="error", %s}' % selector,
            'thanos_replicate_replication_runs_total{%s}' % selector,
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows rate of errors.') +
          g.queryPanel(
            'sum by (result) (rate(thanos_replicate_replication_runs_total{result="error", %s}[$interval]))' % selector,
            '{{result}}'
          ) +
          { yaxes: g.yaxes('percentunit') } +
          g.stack
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to run a replication cycle.') +
          g.latencyPanel('thanos_replicate_replication_run_duration_seconds', 'result="success",  %s' % selector)
        )
      )
      .addRow(
        g.row('Bucket Replication')
        .addPanel(
          g.panel('Metrics') +
          g.queryPanel(
            [
              'sum(rate(blocks_meta_synced{state="loaded", %s}[$interval]))' % selector,
              'sum(rate(blocks_meta_synced{state="failed", %s}[$interval]))' % selector,
              'sum(rate(thanos_replicate_blocks_already_replicated_total{%s}[$interval]))' % selector,
              'sum(rate(thanos_replicate_blocks_replicated_total{%s}[$interval]))' % selector,
              'sum(rate(thanos_replicate_objects_replicated_total{%s}[$interval]))' % selector,
            ],
            ['meta loads', 'partial meta reads', 'already replicated blocks', 'replicated blocks', 'replicated objects']
          )
        )
      ),
  },
}
