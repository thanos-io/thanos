local g = import '../lib/thanos-grafana-builder/builder.libsonnet';
local utils = import '../lib/utils.libsonnet';

{
  local thanos = self,
  query_frontend+:: {
    selector: error 'must provide selector for Thanos Query Frontend dashboard',
    title: error 'must provide title for Thanos Query Frontend dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job=~"$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
    },
  },
  grafanaDashboards+:: {
    [if thanos.query_frontend != null then 'query_frontend.json']:
      local queryFrontendHandlerSelector = utils.joinLabels([thanos.query_frontend.dashboard.selector, 'handler="query-frontend"']);
      g.dashboard(thanos.query_frontend.title)
      .addRow(
        g.row('Query Frontend API')
        .addPanel(
          g.panel('Rate', 'Shows rate of requests against /query_frontend for the given time.') +
          g.httpQpsPanel('http_requests_total', queryFrontendHandlerSelector, thanos.query_frontend.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the the total number of handled requests against /query_frontend.') +
          g.httpErrPanel('http_requests_total', queryFrontendHandlerSelector, thanos.query_frontend.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', queryFrontendHandlerSelector, thanos.query_frontend.dashboard.dimensions)
        )
      ),
  },
}
