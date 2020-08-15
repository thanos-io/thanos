local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  store+:: {
    jobPrefix: error 'must provide job prefix for Thanos Store dashboard',
    selector: error 'must provide selector for Thanos Store dashboard',
    title: error 'must provide title for Thanos Store dashboard',
  },
  grafanaDashboards+:: {
    'store.json':
      g.dashboard(thanos.store.title)
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="unary"')
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcQpsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.grpcLatencyPanel('server', 'namespace="$namespace",job=~"$job",grpc_type="server_stream"')
        )
      )
      .addRow(
        g.row('Bucket Operations')
        .addPanel(
          g.panel('Rate', 'Shows rate of execution for operations against the bucket.') +
          g.queryPanel(
            'sum(rate(thanos_objstore_bucket_operations_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, operation)',
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed operations against the bucket.') +
          g.queryPanel(
            'sum by (job, operation) (rate(thanos_objstore_bucket_operation_failures_total{namespace="$namespace",job=~"$job"}[$interval])) / sum by (job, operation) (rate(thanos_objstore_bucket_operations_total{namespace="$namespace",job=~"$job"}[$interval]))',
            '{{job}} {{operation}}'
          ) +
          { yaxes: g.yaxes({ format: 'percentunit' }) } +
          g.stack,
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute operations against the bucket, in quantiles.') +
          $.latencyByOperationPanel('thanos_objstore_bucket_operation_duration_seconds', 'namespace="$namespace",job=~"$job"')
        )
      )
      .addRow(
        g.row('Block Operations')
        .addPanel(
          g.panel('Block Load Rate', 'Shows rate of block loads from the bucket.') +
          g.queryPanel(
            'sum(rate(thanos_bucket_store_block_loads_total{namespace="$namespace",job=~"$job"}[$interval]))',
            'block loads'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Block Load Errors', 'Shows ratio of errors compared to the total number of block loads from the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_bucket_store_block_load_failures_total{namespace="$namespace",job=~"$job"}',
            'thanos_bucket_store_block_loads_total{namespace="$namespace",job=~"$job"}',
          )
        )
        .addPanel(
          g.panel('Block Drop Rate', 'Shows rate of block drops.') +
          g.queryPanel(
            'sum(rate(thanos_bucket_store_block_drops_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, operation)',
            'block drops {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Block Drop Errors', 'Shows ratio of errors compared to the total number of block drops.') +
          g.qpsErrTotalPanel(
            'thanos_bucket_store_block_drop_failures_total{namespace="$namespace",job=~"$job"}',
            'thanos_bucket_store_block_drops_total{namespace="$namespace",job=~"$job"}',
          )
        )
      )
      .addRow(
        g.row('Cache Operations')
        .addPanel(
          g.panel('Requests', 'Show rate of cache requests.') +
          g.queryPanel(
            'sum(rate(thanos_store_index_cache_requests_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, item_type)',
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Hits', 'Shows ratio of errors compared to the total number of cache hits.') +
          g.queryPanel(
            'sum(rate(thanos_store_index_cache_hits_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, item_type)',
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Added', 'Show rate of added items to cache.') +
          g.queryPanel(
            'sum(rate(thanos_store_index_cache_items_added_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, item_type)',
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Evicted', 'Show rate of evicted items from cache.') +
          g.queryPanel(
            'sum(rate(thanos_store_index_cache_items_evicted_total{namespace="$namespace",job=~"$job"}[$interval])) by (job, item_type)',
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
      )
      .addRow(
        g.row('Store Sent')
        .addPanel(
          g.panel('Chunk Size', 'Shows size of chunks that have sent to the bucket.') +
          g.queryPanel(
            [
              'histogram_quantile(0.99, sum(rate(thanos_bucket_store_sent_chunk_size_bytes_bucket{namespace="$namespace",job=~"$job"}[$interval])) by (job, le))',
              'sum(rate(thanos_bucket_store_sent_chunk_size_bytes_sum{namespace="$namespace",job=~"$job"}[$interval])) by (job) / sum(rate(thanos_bucket_store_sent_chunk_size_bytes_count{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
              'histogram_quantile(0.99, sum(rate(thanos_bucket_store_sent_chunk_size_bytes_bucket{namespace="$namespace",job=~"$job"}[$interval])) by (job, le))',
            ],
            [
              'P99',
              'mean',
              'P50',
            ],
          ) +
          { yaxes: g.yaxes('bytes') }
        ),
      )
      .addRow(
        g.row('Series Operations')
        .addPanel(
          g.panel('Block queried') +
          g.queryPanel(
            [
              'thanos_bucket_store_series_blocks_queried{namespace="$namespace",job=~"$job",quantile="0.99"}',
              'sum(rate(thanos_bucket_store_series_blocks_queried_sum{namespace="$namespace",job=~"$job"}[$interval])) by (job) / sum(rate(thanos_bucket_store_series_blocks_queried_count{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
              'thanos_bucket_store_series_blocks_queried{namespace="$namespace",job=~"$job",quantile="0.50"}',
            ], [
              'P99',
              'mean {{job}}',
              'P50',
            ],
          )
        )
        .addPanel(
          g.panel('Data Fetched', 'Show the size of data fetched') +
          g.queryPanel(
            [
              'thanos_bucket_store_series_data_fetched{namespace="$namespace",job=~"$job",quantile="0.99"}',
              'sum(rate(thanos_bucket_store_series_data_fetched_sum{namespace="$namespace",job=~"$job"}[$interval])) by (job) / sum(rate(thanos_bucket_store_series_data_fetched_count{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
              'thanos_bucket_store_series_data_fetched{namespace="$namespace",job=~"$job",quantile="0.50"}',
            ], [
              'P99',
              'mean {{job}}',
              'P50',
            ],
          ) +
          { yaxes: g.yaxes('bytes') }
        )
        .addPanel(
          g.panel('Result series') +
          g.queryPanel(
            [
              'thanos_bucket_store_series_result_series{namespace="$namespace",job=~"$job",quantile="0.99"}',
              'sum(rate(thanos_bucket_store_series_result_series_sum{namespace="$namespace",job=~"$job"}[$interval])) by (job) / sum(rate(thanos_bucket_store_series_result_series_count{namespace="$namespace",job=~"$job"}[$interval])) by (job)',
              'thanos_bucket_store_series_result_series{namespace="$namespace",job=~"$job",quantile="0.50"}',
            ], [
              'P99',
              'mean {{job}}',
              'P50',
            ],
          )
        )
      )
      .addRow(
        g.row('Series Operation Durations')
        .addPanel(
          g.panel('Get All', 'Shows how long has it taken to get all series.') +
          g.latencyPanel('thanos_bucket_store_series_get_all_duration_seconds', 'namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Merge', 'Shows how long has it taken to merge series.') +
          g.latencyPanel('thanos_bucket_store_series_merge_duration_seconds', 'namespace="$namespace",job=~"$job"')
        )
        .addPanel(
          g.panel('Gate', 'Shows how long has it taken for a series to wait at the gate.') +
          g.latencyPanel('thanos_bucket_store_series_gate_duration_seconds', 'namespace="$namespace",job=~"$job"')
        )
      )
      .addRow(
        g.resourceUtilizationRow()
      ) +
      g.template('namespace', thanos.dashboard.namespaceQuery) +
      g.template('job', 'up', 'namespace="$namespace",%(selector)s' % thanos.store, true, '%(jobPrefix)s.*' % thanos.store) +
      g.template('pod', 'kube_pod_info', 'namespace="$namespace",created_by_name=~"%(jobPrefix)s.*"' % thanos.store, true, '.*'),

    __overviewRows__+:: [
      g.row('Store')
      .addPanel(
        g.panel('gPRC (Unary) Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
        g.grpcQpsPanel('server', 'namespace="$namespace",%(selector)s,grpc_type="unary"' % thanos.store) +
        g.addDashboardLink(thanos.store.title)
      )
      .addPanel(
        g.panel('gPRC (Unary) Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
        g.grpcErrorsPanel('server', 'namespace="$namespace",%(selector)s,grpc_type="unary"' % thanos.store) +
        g.addDashboardLink(thanos.store.title)
      )
      .addPanel(
        g.sloLatency(
          'gRPC Latency 99th Percentile',
          'Shows how long has it taken to handle requests from queriers.',
          'grpc_server_handling_seconds_bucket{grpc_type="unary",namespace="$namespace",%(selector)s}' % thanos.store,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.store.title)
      ),
    ],
  },

  latencyByOperationPanel(metricName, selector, multiplier='1'):: {
    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum(rate(%s_bucket{%s}[$interval])) by (job, operation, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P99 {{job}}',
        refId: 'A',
        step: 10,
      },
      {
        expr: 'sum(rate(%s_sum{%s}[$interval])) by (job, operation) * %s / sum(rate(%s_count{%s}[$interval])) by (job, operation)' % [metricName, selector, multiplier, metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'mean {{job}}',
        refId: 'B',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum(rate(%s_bucket{%s}[$interval])) by (job, operation, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P50 {{job}}',
        refId: 'C',
        step: 10,
      },
    ],
    yaxes: g.yaxes('s'),
  },
}
