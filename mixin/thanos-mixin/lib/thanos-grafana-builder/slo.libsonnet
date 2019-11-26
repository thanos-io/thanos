{
  sloLatency(title, description, selector, quantile, warning, critical)::
    $.panel(title, description) +
    $.queryPanel(
      'histogram_quantile(%.2f, sum(rate(%s[$interval])) by (job, le))' % [quantile, selector],
      '{{job}} P' + quantile * 100
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
