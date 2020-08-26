{
  httpQpsPanel(metricName, selector):: {
    seriesOverrides: [
      { alias: '/1../', color: '#EAB839' },
      { alias: '/2../', color: '#7EB26D' },
      { alias: '/3../', color: '#6ED0E0' },
      { alias: '/4../', color: '#EF843C' },
      { alias: '/5../', color: '#E24D42' },
    ],
    targets: [
      {
        expr: 'sum by (handler, code) (rate(%s{%s}[$interval]))' % [metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{code}}',
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  httpErrPanel(metricName, selector)::
    $.qpsErrTotalPanel(
      '%s{%s,code=~"5.."}' % [metricName, selector],
      '%s{%s}' % [metricName, selector],
    ),
}
