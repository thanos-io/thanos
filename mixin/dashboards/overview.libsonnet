local g = import '../lib/thanos-grafana-builder/builder.libsonnet';

{
  local thanos = self,
  overview:: {
    title: error 'must provide title for Thanos Overview dashboard',
  },
  grafanaDashboards+:: {
    'overview.json':
      g.dashboard(thanos.dashboard.overview.title),
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
