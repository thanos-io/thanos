local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  bucket_replicate+:: {
    jobPrefix: error 'must provide job prefix for Thanos Bucket Replicate dashboard',
    selector: error 'must provide selector for Thanos Bucket Replicate dashboard',
    title: error 'must provide title for Thanos Bucket Replicate dashboard',
  },
  grafanaDashboards+:: {
    'bucket_replicate.json':
      g.dashboard(thanos.bucket_replicate.title)
      .addRow(
        g.row('Bucket Replicate Runs')
        .addPanel(
          g.panel('Rate') +
          g.qpsErrTotalPanel(
            'thanos_replicate_replication_runs_total{result="error", namespace="$namespace",%(selector)s}' % thanos.bucket_replicate,
            'thanos_replicate_replication_runs_total{namespace="$namespace",%(selector)s}' % thanos.bucket_replicate,
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows rate of errors.') +
          g.queryPanel(
            'sum(rate(thanos_replicate_replication_runs_total{result="error", namespace="$namespace",%(selector)s}[$interval])) by (result)' % thanos.bucket_replicate,
            '{{result}}'
          ) +
          { yaxes: g.yaxes('percentunit') } +
          g.stack
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to run a replication cycle.') +
          g.latencyPanel('thanos_replicate_replication_run_duration_seconds', 'result="success", namespace="$namespace",%(selector)s' % thanos.bucket_replicate)
        )
      )
      .addRow(
        g.row('Bucket Replication')
        .addPanel(
          g.panel('Metrics') +
          g.queryPanel(
            [
              'sum(rate(thanos_replicate_origin_iterations_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
              'sum(rate(thanos_replicate_origin_meta_loads_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
              'sum(rate(thanos_replicate_origin_partial_meta_reads_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
              'sum(rate(thanos_replicate_blocks_already_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
              'sum(rate(thanos_replicate_blocks_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
              'sum(rate(thanos_replicate_objects_replicated_total{namespace="$namespace",%(selector)s}[$interval]))' % thanos.bucket_replicate,
            ],
            ['iterations', 'meta loads', 'partial meta reads', 'already replicated blocks', 'replicated blocks', 'replicated objects']
          )
        )
      )
      +
      g.template('namespace', 'kube_pod_info') +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.bucket_replicate, true, '%(jobPrefix)s.*' % thanos.bucket_replicate),
  },
}
