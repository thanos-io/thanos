{
  httpQpsPanel(metricName, selector):: {
    aliasColors: {
      '1xx': '#EAB839',
      '2xx': '#7EB26D',
      '3xx': '#6ED0E0',
      '4xx': '#EF843C',
      '5xx': '#E24D42',
      success: '#7EB26D',
      'error': '#E24D42',
    },
    targets: [
      {
        expr: 'sum(label_replace(rate(%s{%s}[$interval]),"status_code", "${1}xx", "code", "([0-9])..")) by (job, handler, status_code)' % [metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{job}} {{handler}} {{status_code}}',
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
