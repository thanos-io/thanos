local utils = import '../utils.libsonnet';

{
  sloLatency(title, description, selector, dimensions, quantile, warning, critical)::
    local aggregatedLabels = std.split(dimensions, ',');
    local dimensionsTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]);

    $.panel(title, description) +
    $.queryPanel(
      'histogram_quantile(%.2f, sum by (%s) (rate(%s[$interval])))' % [quantile, utils.joinLabels(aggregatedLabels + ['le']), selector],
      dimensionsTemplate + ' P' + quantile * 100
    ) +
    {
      yaxes: $.yaxes('s'),
      thresholds+: [
        {
          value: warning,
          colorMode: 'warning',
          op: 'gt',
          fill: true,
          line: true,
          yaxis: 'left',
        },
        {
          value: critical,
          colorMode: 'critical',
          op: 'gt',
          fill: true,
          line: true,
          yaxis: 'left',
        },
      ],
    },
}
