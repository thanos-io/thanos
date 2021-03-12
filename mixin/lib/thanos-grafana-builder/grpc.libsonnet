{
  grpcQpsPanel(type, selector, aggregator):: {
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server',
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),

    aliasColors: {
      Aborted: '#EAB839',
      AlreadyExists: '#7EB26D',
      FailedPrecondition: '#6ED0E0',
      Unimplemented: '#6ED0E0',
      InvalidArgument: '#EF843C',
      NotFound: '#EF843C',
      PermissionDenied: '#EF843C',
      Unauthenticated: '#EF843C',
      Canceled: '#E24D42',
      DataLoss: '#E24D42',
      DeadlineExceeded: '#E24D42',
      Internal: '#E24D42',
      OutOfRange: '#E24D42',
      ResourceExhausted: '#E24D42',
      Unavailable: '#E24D42',
      Unknown: '#E24D42',
      OK: '#7EB26D',
      'error': '#E24D42',
    },
    targets: [
      {
        expr: 'sum by (%s, grpc_method, grpc_code) (rate(%s_handled_total{%s}[$interval]))' % [aggregator, prefix, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: aggregatorTemplate + ' {{grpc_method}} {{grpc_code}}',
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  grpcErrorsPanel(type, selector, aggregator)::
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server';
    $.qpsErrTotalPanel(
      '%s_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss",%s}' % [prefix, selector],
      '%s_started_total{%s}' % [prefix, selector],
      aggregator,
    ),

  grpcLatencyPanel(type, selector, aggregator, multiplier='1')::
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server';
    local params = { prefix: prefix, selector: selector, aggregator: aggregator, multiplier: multiplier };
    $.queryPanel(
      [
        'histogram_quantile(0.99, sum by (%(aggregator)s, grpc_method, le) (rate(%(prefix)s_handling_seconds_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
        |||
          sum by (%(aggregator)s) (rate(%(prefix)s_handling_seconds_sum{%(selector)s}[$interval])) * %(multiplier)s
          /
          sum by (%(aggregator)s) (rate(%(prefix)s_handling_seconds_count{%(selector)s}[$interval]))
        ||| % params,
        'histogram_quantile(0.50, sum by (%(aggregator)s, grpc_method, le) (rate(%(prefix)s_handling_seconds_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
      ],
      [
        'P99 {{job}} {{grpc_method}}',
        'mean {{job}} {{grpc_method}}',
        'P50 {{job}} {{grpc_method}}',
      ]
    ) +
    { yaxes: $.yaxes('s') },
}
