local g = import '../lib/thanos-grafana-builder/builder.libsonnet';
local utils = import '../lib/utils.libsonnet';

{
  local thanos = self,
  queryFrontend+:: {
    selector: error 'must provide selector for Thanos Query Frontend dashboard',
    title: error 'must provide title for Thanos Query Frontend dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job=~"$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.queryFrontend != null then 'query-frontend.json']:
      local queryFrontendHandlerSelector = utils.joinLabels([thanos.queryFrontend.dashboard.selector, 'handler="query-frontend"']);
      local queryFrontendOpSelector = utils.joinLabels([thanos.queryFrontend.dashboard.selector, 'op="query_range"']);
      g.dashboard(thanos.queryFrontend.title)
      .addRow(
        g.row('Query Frontend API')
        .addPanel(
          g.panel('Rate of requests', 'Shows rate of requests against Query Frontend for the given time.') +
          g.httpQpsPanel('http_requests_total', queryFrontendHandlerSelector, thanos.queryFrontend.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Rate of queries', 'Shows rate of queries passing through Query Frontend') +
          g.httpQpsPanel('thanos_query_frontend_queries_total', queryFrontendOpSelector, thanos.queryFrontend.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests against Query Frontend.') +
          g.httpErrPanel('http_requests_total', queryFrontendHandlerSelector, thanos.queryFrontend.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', queryFrontendHandlerSelector, thanos.queryFrontend.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('Cache Operations')
        .addPanel(
          g.panel('Requests', 'Show rate of cache requests.') +
          g.queryPanel(
            'sum by (%s) (rate(cortex_cache_request_duration_seconds_count{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.queryFrontend.dashboard.dimensions, 'tripperware']), thanos.queryFrontend.dashboard.selector],
            '{{job}} {{tripperware}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Querier cache gets vs misses', 'Show rate of Querier cache gets vs misses.') +
          g.queryPanel(
            'sum by (%s) (rate(querier_cache_gets_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.queryFrontend.dashboard.dimensions, 'tripperware']), thanos.queryFrontend.dashboard.selector],
            'Cache gets - {{job}} {{tripperware}}',
          ) +
          g.queryPanel(
            'sum by (%s) (rate(querier_cache_misses_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.queryFrontend.dashboard.dimensions, 'tripperware']), thanos.queryFrontend.dashboard.selector],
            'Cache misses - {{job}} {{tripperware}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Cortex fetched keys', 'Shows rate of cortex fetched keys.') +
          g.queryPanel(
            'sum by (%s) (rate(cortex_cache_fetched_keys_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.queryFrontend.dashboard.dimensions, 'tripperware']), thanos.queryFrontend.dashboard.selector],
            '{{job}} {{tripperware}}',
          ) +
          g.stack
        )
        .addPanel(
          g.panel('Cortex cache hits', 'Shows rate of cortex cache hits.') +
          g.queryPanel(
            'sum by (%s) (rate(cortex_cache_hits_total{%s}[$__rate_interval]))' % [utils.joinLabels([thanos.queryFrontend.dashboard.dimensions, 'tripperware']), thanos.queryFrontend.dashboard.selector],
            '{{job}} {{tripperware}}',
          ) +
          g.stack
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.queryFrontend.dashboard.selector, thanos.queryFrontend.dashboard.dimensions)
      ),
  },
}
