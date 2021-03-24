local utils = import '../utils.libsonnet';

{
  grpcRequestsPanel(metric, selector, dimensions):: {
    local aggregatedLabels = std.split(dimensions, ','),
    local dimensionsTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]),

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
        expr: 'sum by (%s) (rate(%s{%s}[$interval]))' % [utils.joinLabels(aggregatedLabels + ['grpc_method', 'grpc_code']), metric, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: dimensionsTemplate + ' {{grpc_method}} {{grpc_code}}',
        step: 10,
      },
    ],
  } + $.stack,

  grpcErrorsPanel(metric, selector, dimensions)::
    $.qpsErrTotalPanel(
      '%s{grpc_code=~"Unknown|ResourceExhausted|Internal|Unavailable|DataLoss",%s}' % [metric, selector],
      '%s{%s}' % [metric, selector],
      dimensions
    ),
}
