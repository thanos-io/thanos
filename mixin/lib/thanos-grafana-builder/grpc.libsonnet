{
  grpcQpsPanel(type, selector):: {
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server',

    seriesOverrides: [
      { alias: '/Aborted/', color: '#EAB839' },
      { alias: '/AlreadyExists/', color: '#7EB26D' },
      { alias: '/FailedPrecondition/', color: '#6ED0E0' },
      { alias: '/Unimplemented/', color: '#6ED0E0' },
      { alias: '/InvalidArgument/', color: '#EF843C' },
      { alias: '/NotFound/', color: '#EF843C' },
      { alias: '/PermissionDenied/', color: '#EF843C' },
      { alias: '/Unauthenticated/', color: '#EF843C' },
      { alias: '/Canceled/', color: '#E24D42' },
      { alias: '/DataLoss/', color: '#E24D42' },
      { alias: '/DeadlineExceeded/', color: '#E24D42' },
      { alias: '/Internal/', color: '#E24D42' },
      { alias: '/OutOfRange/', color: '#E24D42' },
      { alias: '/ResourceExhausted/', color: '#E24D42' },
      { alias: '/Unavailable/', color: '#E24D42' },
      { alias: '/Unknown/', color: '#E24D42' },
      { alias: '/OK/', color: '#7EB26D' },
      { alias: 'error', color: '#E24D42' },
    ],
    targets: [
      {
        expr: 'sum by (grpc_code) (rate(%s_handled_total{%s}[$interval]))' % [prefix, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{grpc_code}}',
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  grpcErrorsPanel(type, selector)::
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server';
    $.qpsErrTotalPanel(
      '%s_handled_total{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss",%s}' % [prefix, selector],
      '%s_started_total{%s}' % [prefix, selector],
    ),

  grpcLatencyPanel(type, selector, multiplier='1')::
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server';
    $.queryPanel(
      [
        'histogram_quantile(0.99, sum by (le) (rate(%s_handling_seconds_bucket{%s}[$interval]))) * %s' % [prefix, selector, multiplier],
        'histogram_quantile(0.90, sum by (le) (rate(%s_handling_seconds_bucket{%s}[$interval]))) * %s' % [prefix, selector, multiplier],
        'histogram_quantile(0.50, sum by (le) (rate(%s_handling_seconds_bucket{%s}[$interval]))) * %s' % [prefix, selector, multiplier],
      ],
      ['p99', 'p90', 'p50']
    ) +
    { yaxes: $.yaxes('s') },
}
