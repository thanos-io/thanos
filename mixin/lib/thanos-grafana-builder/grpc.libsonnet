{
  grpcRequestsPanel(metric, selector, aggregator):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),

    seriesOverrides: [
      { alias: '/Aborted/', color: '#EAB839' },
      { alias: '/AlreadyExists/', color: '#37872D' },
      { alias: '/FailedPrecondition/', color: '#E0B400' },
      { alias: '/Unimplemented/', color: '#E0B400' },
      { alias: '/InvalidArgument/', color: '#1F60C4' },
      { alias: '/NotFound/', color: '#1F60C4' },
      { alias: '/PermissionDenied/', color: '#1F60C4' },
      { alias: '/Unauthenticated/', color: '#1F60C4' },
      { alias: '/Canceled/', color: '#C4162A' },
      { alias: '/DataLoss/', color: '#C4162A' },
      { alias: '/DeadlineExceeded/', color: '#C4162A' },
      { alias: '/Internal/', color: '#C4162A' },
      { alias: '/OutOfRange/', color: '#C4162A' },
      { alias: '/ResourceExhausted/', color: '#C4162A' },
      { alias: '/Unavailable/', color: '#C4162A' },
      { alias: '/Unknown/', color: '#C4162A' },
      { alias: '/OK/', color: '#37872D' },
      { alias: 'error', color: '#C4162A' },
    ],
    targets: [
      {
        expr: 'sum by (%s, grpc_code) (rate(%s{%s}[$interval]))' % [metric, metric, selector],  // grpc_method
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: aggregatorTemplate + ' {{grpc_code}}',  // {{grpc_method}}
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  grpcErrorsPanel(metric, selector, aggregator)::
    $.qpsErrTotalPanel(
      '%s{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss",%s}' % [metric, selector],
      '%s{%s}' % [metric, selector],
      aggregator
    ),
}
