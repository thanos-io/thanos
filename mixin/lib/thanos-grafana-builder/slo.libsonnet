{
  sloLatency(title, description, selector, aggregator, quantile, warning, critical)::
    local aggregatedLabels = std.split(aggregator, ',');
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]);

    $.panel(title, description) +
    $.queryPanel(
      'histogram_quantile(%.2f, sum by (%s, le) (rate(%s[$interval])))' % [quantile, aggregator, selector],
      aggregatorTemplate + ' P' + quantile * 100
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
