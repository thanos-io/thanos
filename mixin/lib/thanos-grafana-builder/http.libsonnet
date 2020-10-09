{
  httpQpsPanel(metricName, selector):: {
    seriesOverrides: [
      { alias: '/1../', color: '#EAB839' },
      { alias: '/2../', color: '#37872D' },
      { alias: '/3../', color: '#E0B400' },
      { alias: '/4../', color: '#1F60C4' },
      { alias: '/5../', color: '#C4162A' },
    ],
    targets: [
      {
        expr: 'sum by (code) (rate(%s{%s}[$interval]))' % [metricName, selector],
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
