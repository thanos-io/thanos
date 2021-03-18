{
  httpQpsPanel(metric, selector, aggregator):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]),

    seriesOverrides: [
      { alias: '/1../', color: '#EAB839' },
      { alias: '/2../', color: '#37872D' },
      { alias: '/3../', color: '#E0B400' },
      { alias: '/4../', color: '#1F60C4' },
      { alias: '/5../', color: '#C4162A' },
    ],

    targets: [
      {
        expr: 'sum by (%s, code) (rate(%s{%s}[$interval]))' % [aggregator, metric, selector],  // handler
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: aggregatorTemplate + ' {{code}}',  // handler
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  httpErrPanel(metric, selector, aggregator)::
    $.qpsErrTotalPanel(
      '%s{%s,code=~"5.."}' % [metric, selector],
      '%s{%s}' % [metric, selector],
      aggregator
    ),
}
