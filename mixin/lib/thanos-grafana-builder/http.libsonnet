local utils = import '../utils.libsonnet';

{
  httpQpsPanel(metric, selector, dimensions):: {
    local aggregatedLabels = std.split(dimensions, ','),
    local dimensionsTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]),

    seriesOverrides: [
      { alias: '/1../', color: '#EAB839' },
      { alias: '/2../', color: '#37872D' },
      { alias: '/3../', color: '#E0B400' },
      { alias: '/4../', color: '#1F60C4' },
      { alias: '/5../', color: '#C4162A' },
    ],

    targets: [
      {
        expr: 'sum by (%s) (rate(%s{%s}[$interval]))' % [utils.joinLabels(aggregatedLabels + ['handler', 'code']), metric, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: dimensionsTemplate + ' {{handler}} {{code}}',
        step: 10,
      },
    ],
  } + $.stack,

  httpErrPanel(metric, selector, dimensions)::
    $.qpsErrTotalPanel(
      '%s{%s,code=~"5.."}' % [metric, selector],
      '%s{%s}' % [metric, selector],
      dimensions
    ),
}
