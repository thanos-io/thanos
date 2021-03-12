{
  httpQpsPanel(metricName, selector, aggregator):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),

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
        expr: 'sum by (%s, handler, status_code) (label_replace(rate(%s{%s}[$interval]),"status_code", "${1}xx", "code", "([0-9]).."))' % [aggregator, metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: aggregatorTemplate + ' {{handler}} {{status_code}}',
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
