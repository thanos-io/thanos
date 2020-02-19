local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  replicate+:: {
    jobPrefix: error 'must provide job prefix for Thanos Replicate dashboard',
    selector: error 'must provide selector for Thanos Replicate dashboard',
    title: error 'must provide title for Thanos Replicate dashboard',
  },
  grafanaDashboards+:: {
    'replicate.json':
      g.dashboard(thanos.replicate.title)
      .addRow(
        g.row('Replicate Runs')
        .addPanel(
          g.panel('Rate') +
          g.qpsErrTotalPanel(
            'thanos_replicate_replication_runs_total{result="error", namespace="$namespace",%(selector)s}' % thanos.replicate,
            'thanos_replicate_replication_runs_total{namespace="$namespace",%(selector)s}' % thanos.replicate,
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows rate of errors.') +
          g.queryPanel(
            'sum(rate(thanos_replicate_replication_runs_total{result="error", namespace="$namespace",%(selector)s}[$interval])) by (result)' % thanos.replicate,
            '{{result}}'
          ) +
          { yaxes: g.yaxes('percentunit') } +
          g.stack
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to run a replication cycle.') +
          g.latencyPanel('thanos_replicate_replication_run_duration_seconds', 'result="success", namespace="$namespace",%(selector)s' % thanos.replicate)
        )
      )
      .addRow(
        g.row('Replication')
        .addPanel(
          g.panel('Metrics') +
          g.queryPanel(
            [
              'sum(rate(thanos_replicate_origin_iterations_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
              'sum(rate(thanos_replicate_origin_meta_loads_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
              'sum(rate(thanos_replicate_origin_partial_meta_reads_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
              'sum(rate(thanos_replicate_blocks_already_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
              'sum(rate(thanos_replicate_blocks_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
              'sum(rate(thanos_replicate_objects_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.replicate,
            ],
            ['iterations', 'meta loads', 'partial meta reads', 'already replicated blocks', 'replicated blocks', 'replicated objects']
          )
        )
      )
      +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.replicate, true, '%(jobPrefix)s.*' % thanos.replicate),
  },
}
