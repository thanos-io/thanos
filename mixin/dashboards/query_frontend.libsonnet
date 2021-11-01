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
    [if thanos.queryFrontend != null then 'query_frontend.json']:
      g.dashboard(thanos.queryFrontend.title)
      .addRow(
        g.resourceUtilizationRow(thanos.queryFrontend.dashboard.selector, thanos.queryFrontend.dashboard.dimensions)
      ),
  },
}
