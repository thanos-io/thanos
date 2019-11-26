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
        expr: 'sum(label_replace(rate(%s{%s}[$interval]),"status_code", "${1}xx", "code", "([0-9])..")) by (job, status_code)' % [metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{job}} {{status_code}}',
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  httpQpsPanelDetailed(metricName, selector)::
    $.httpQpsPanel(metricName, selector) {
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
    },

  httpErrPanel(metricName, selector)::
    $.qpsErrTotalPanel(
      '%s{%s,code~="5.."}' % [metricName, selector],
      '%s{%s}' % [metricName, selector],
    ),

  httpErrDetailsPanel(metricName, selector)::
    $.queryPanel(
      'sum(rate(%s{%s,code!~"2.."}[$interval])) by (job, handler, code)' % [metricName, selector],
      '{{job}} {{handler}} {{code}}'
    ) +
    { yaxes: $.yaxes({ format: 'percentunit' }) } +
    $.stack,

  httpLatencyDetailsPanel(metricName, selector, multiplier='1'):: {
    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum(rate(%s_bucket{%s}[$interval])) by (job, handler, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P99 {{job}} {{handler}}',
        refId: 'A',
        step: 10,
      },
      {
        expr: 'sum(rate(%s_sum{%s}[$interval])) by (job, handler) * %s / sum(rate(%s_count{%s}[$interval])) by (job, handler)' % [metricName, selector, multiplier, metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'mean {{job}} {{handler}}',
        refId: 'B',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum(rate(%s_bucket{%s}[$interval])) by (job, handler, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P50 {{job}} {{handler}}',
        refId: 'C',
        step: 10,
      },
    ],
    yaxes: $.yaxes('s'),
  },
}
