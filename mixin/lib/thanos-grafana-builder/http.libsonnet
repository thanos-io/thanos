{
  httpQpsPanel(metricName, selector, aggregator):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),

    seriesOverrides: [
      { alias: '/1../', color: '#EAB839' },
      { alias: '/2../', color: '#37872D' },
      { alias: '/3../', color: '#E0B400' },
      { alias: '/4../', color: '#1F60C4' },
      { alias: '/5../', color: '#C4162A' },
    ],

    targets: [
      {
        expr: 'sum by (%s, code) (rate(%s{%s}[$interval]))' % [aggregator, metricName, selector],  // handler
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: aggregatorTemplate + ' {{code}}',  // handler
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  httpErrPanel(metricName, selector, aggregator)::
    $.qpsErrTotalPanel(
      '%s{%s,code=~"5.."}' % [metricName, selector],
      '%s{%s}' % [metricName, selector],
      aggregator
    ),
}
