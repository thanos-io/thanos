local g = import '../lib/thanos-grafana-builder/builder.libsonnet';
local utils = import '../lib/utils.libsonnet';

{
  local thanos = self,
  store+:: {
    selector: error 'must provide selector for Thanos Store dashboard',
    title: error 'must provide title for Thanos Store dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job="$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.store != null then 'store.json']:
      local grpcUnarySelector = utils.joinLabels([thanos.store.dashboard.selector, 'grpc_type="unary"']);
      local grpcServerStreamSelector = utils.joinLabels([thanos.store.dashboard.selector, 'grpc_type="server_stream"']);

      g.dashboard(thanos.store.title)
      .addRow(
        g.row('gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcUnarySelector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcUnarySelector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcUnarySelector, thanos.store.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcServerStreamSelector, thanos.store.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('Bucket Operations')
        .addPanel(
          g.panel('Rate', 'Shows rate of execution for operations against the bucket.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_objstore_bucket_operations_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'operation']), thanos.store.dashboard.selector],
            '{{job}} {{operation}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of executed operations against the bucket.') +
          g.queryPanel(
            'sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operation_failures_total{%(selector)s}[$interval])) / sum by (%(dimensions)s) (rate(thanos_objstore_bucket_operations_total{%(selector)s}[$interval]))' % thanos.store.dashboard { dimensions: utils.joinLabels([thanos.store.dashboard.dimensions, 'operation']) },
            '{{job}} {{operation}}'
          ) +
          { yaxes: g.yaxes({ format: 'percentunit' }) } +
          g.stack,
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to execute operations against the bucket, in quantiles.') +
          $.latencyByOperationPanel('thanos_objstore_bucket_operation_duration_seconds', thanos.store.dashboard.selector, thanos.store.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('Block Operations')
        .addPanel(
          g.panel('Block Load Rate', 'Shows rate of block loads from the bucket.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_bucket_store_block_loads_total{%s}[$interval]))' % [thanos.store.dashboard.dimensions, thanos.store.dashboard.selector],
            'block loads'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Block Load Errors', 'Shows ratio of errors compared to the total number of block loads from the bucket.') +
          g.qpsErrTotalPanel(
            'thanos_bucket_store_block_load_failures_total{%s}' % thanos.store.dashboard.selector,
            'thanos_bucket_store_block_loads_total{%s}' % thanos.store.dashboard.selector,
            thanos.store.dashboard.dimensions
          )
        )
        .addPanel(
          g.panel('Block Drop Rate', 'Shows rate of block drops.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_bucket_store_block_drops_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'operation']), thanos.store.dashboard.selector],
            'block drops {{job}}'
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Block Drop Errors', 'Shows ratio of errors compared to the total number of block drops.') +
          g.qpsErrTotalPanel(
            'thanos_bucket_store_block_drop_failures_total{%s}' % thanos.store.dashboard.selector,
            'thanos_bucket_store_block_drops_total{%s}' % thanos.store.dashboard.selector,
            thanos.store.dashboard.dimensions
          )
        )
      )
      .addRow(
        g.row('Cache Operations')
        .addPanel(
          g.panel('Requests', 'Show rate of cache requests.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_store_index_cache_requests_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'item_type']), thanos.store.dashboard.selector],
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Hits', 'Shows ratio of errors compared to the total number of cache hits.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_store_index_cache_hits_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'item_type']), thanos.store.dashboard.selector],
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Added', 'Show rate of added items to cache.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_store_index_cache_items_added_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'item_type']), thanos.store.dashboard.selector],
            '{{job}} {{item_type}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Evicted', 'Show rate of evicted items from cache.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_store_index_cache_items_evicted_total{%s}[$interval]))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'item_type']), thanos.store.dashboard.selector],
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
              'histogram_quantile(0.99, sum by (%s) (rate(thanos_bucket_store_sent_chunk_size_bytes_bucket{%s}[$interval])))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'le']), thanos.store.dashboard.selector],
              'sum by (%(dimensions)s) (rate(thanos_bucket_store_sent_chunk_size_bytes_sum{%(selector)s}[$interval])) / sum by (%(dimensions)s) (rate(thanos_bucket_store_sent_chunk_size_bytes_count{%(selector)s}[$interval]))' % thanos.store.dashboard,
              'histogram_quantile(0.99, sum by (%s) (rate(thanos_bucket_store_sent_chunk_size_bytes_bucket{%s}[$interval])))' % [utils.joinLabels([thanos.store.dashboard.dimensions, 'le']), thanos.store.dashboard.selector],
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
              'thanos_bucket_store_series_blocks_queried{%s, quantile="0.99"}' % thanos.store.dashboard.selector,
              'sum by (%(dimensions)s) (rate(thanos_bucket_store_series_blocks_queried_sum{%(selector)s}[$interval])) / sum by (%(dimensions)s) (rate(thanos_bucket_store_series_blocks_queried_count{%(selector)s}[$interval]))' % thanos.store.dashboard,
              'thanos_bucket_store_series_blocks_queried{%s, quantile="0.50"}' % thanos.store.dashboard.selector,
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
              'thanos_bucket_store_series_data_fetched{%s, quantile="0.99"}' % thanos.store.dashboard.selector,
              'sum by (%(dimensions)s) (rate(thanos_bucket_store_series_data_fetched_sum{%(selector)s}[$interval])) / sum by (%(dimensions)s) (rate(thanos_bucket_store_series_data_fetched_count{%(selector)s}[$interval]))' % thanos.store.dashboard,
              'thanos_bucket_store_series_data_fetched{%s, quantile="0.50"}' % thanos.store.dashboard.selector,
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
              'thanos_bucket_store_series_result_series{%s,quantile="0.99"}' % thanos.store.dashboard.selector,
              'sum by (%(dimensions)s) (rate(thanos_bucket_store_series_result_series_sum{%(selector)s}[$interval])) / sum by (%(dimensions)s) (rate(thanos_bucket_store_series_result_series_count{%(selector)s}[$interval]))' % thanos.store.dashboard,
              'thanos_bucket_store_series_result_series{%s,quantile="0.50"}' % thanos.store.dashboard.selector,
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
          g.latencyPanel('thanos_bucket_store_series_get_all_duration_seconds', thanos.store.dashboard.selector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Merge', 'Shows how long has it taken to merge series.') +
          g.latencyPanel('thanos_bucket_store_series_merge_duration_seconds', thanos.store.dashboard.selector, thanos.store.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Gate', 'Shows how long has it taken for a series to wait at the gate.') +
          g.latencyPanel('thanos_bucket_store_series_gate_duration_seconds', thanos.store.dashboard.selector, thanos.store.dashboard.dimensions)
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.store.dashboard.selector, thanos.store.dashboard.dimensions)
      ),

    __overviewRows__+:: [
      g.row('Store')
      .addPanel(
        g.panel('gPRC (Unary) Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
        g.grpcRequestsPanel('grpc_server_handled_total', utils.joinLabels([thanos.dashboard.overview.selector, 'grpc_type="unary"']), thanos.dashboard.overview.dimensions) +
        g.addDashboardLink(thanos.store.title)
      )
      .addPanel(
        g.panel('gPRC (Unary) Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
        g.grpcErrorsPanel('grpc_server_handled_total', utils.joinLabels([thanos.dashboard.overview.selector, 'grpc_type="unary"']), thanos.dashboard.overview.dimensions) +
        g.addDashboardLink(thanos.store.title)
      )
      .addPanel(
        g.sloLatency(
          'gRPC Latency 99th Percentile',
          'Shows how long has it taken to handle requests from queriers.',
          'grpc_server_handling_seconds_bucket{%s}' % utils.joinLabels([thanos.dashboard.overview.selector, 'grpc_type="unary"']),
          thanos.dashboard.overview.dimensions,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.store.title)
      ),
    ],
  },

  latencyByOperationPanel(metricName, selector, dimensions, multiplier='1'):: {
    local params = { metricName: metricName, selector: selector, multiplier: multiplier, dimensions: dimensions },

    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum by (%(dimensions)s, operation, le) (rate(%(metricName)s_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P99 {{job}}',
        refId: 'A',
        step: 10,
      },
      {
        expr: 'sum by (%(dimensions)s, operation) (rate(%(metricName)s_sum{%(selector)s}[$interval])) * %(multiplier)s  / sum by (%(dimensions)s, operation) (rate(%(metricName)s_count{%(selector)s}[$interval]))' % params,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'mean {{job}}',
        refId: 'B',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum by (%(dimensions)s, operation, le) (rate(%(metricName)s_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
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
