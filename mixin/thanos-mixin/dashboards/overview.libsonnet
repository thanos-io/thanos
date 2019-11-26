local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  grafanaDashboards+:: {
    'overview.json':
      g.dashboard($._config.grafanaThanos.dashboardOverviewTitle) +
      g.template('namespace', 'kube_pod_info'),
  },
} +
{
  local grafanaDashboards = super.grafanaDashboards,
  grafanaDashboards+:: {
    'overview.json'+: {

      __enumeratedRows__+:: std.foldl(
        function(acc, row)
          local n = std.length(row.panels);
          local panelIndex = acc.counter;
          local panels = std.makeArray(
            n, function(i)
              row.panels[i] { id: panelIndex + i }
          );
          acc { counter:: acc.counter + n, rows+: [row { panels: panels }] },
        grafanaDashboards.__overviewRows__,
        { counter:: 1, rows: [] }
      ),

      rows+: self.__enumeratedRows__.rows,
    },
  },
}
