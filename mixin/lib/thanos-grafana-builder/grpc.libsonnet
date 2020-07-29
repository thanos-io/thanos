{
  grpcQpsPanel(type, selector):: {
    local prefix = if type == 'client' then 'grpc_client' else 'grpc_server',

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
        expr: 'sum(rate(%s_handled_total{%s}[$interval])) by (job, grpc_method, grpc_code)' % [prefix, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{job}} {{grpc_method}} {{grpc_code}}',
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
        'histogram_quantile(0.99, sum(rate(%s_handling_seconds_bucket{%s}[$interval])) by (job, grpc_method, le)) * %s' % [prefix, selector, multiplier],
        |||
          sum(rate(%s_handling_seconds_sum{%s}[$interval])) by (job) * %s
          /
          sum(rate(%s_handling_seconds_count{%s}[$interval])) by (job)
        ||| % [prefix, selector, multiplier, prefix, selector],
        'histogram_quantile(0.50, sum(rate(%s_handling_seconds_bucket{%s}[$interval])) by (job, grpc_method, le)) * %s' % [prefix, selector, multiplier],
      ],
      [
        'P99 {{job}} {{grpc_method}}',
        'mean {{job}} {{grpc_method}}',
        'P50 {{job}} {{grpc_method}}',
      ]
    ) +
    { yaxes: $.yaxes('s') },
}
