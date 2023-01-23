local g = import '../lib/thanos-grafana-builder/builder.libsonnet';
local utils = import '../lib/utils.libsonnet';


{
  local thanos = self,
  receive+:: {
    selector: error 'must provide selector for Thanos Receive dashboard',
    title: error 'must provide title for Thanos Receive dashboard',
    dashboard:: {
      selector: std.join(', ', thanos.dashboard.selector + ['job=~"$job"']),
      dimensions: std.join(', ', thanos.dashboard.dimensions + ['job']),
      tenantSelector: std.join(', ', thanos.dashboard.selector + ['job=~"$job"', 'tenant=~"$tenant"']),
      tenantDimensions: std.join(', ', thanos.dashboard.dimensions + ['job', 'tenant']),
    },
  },
  grafanaDashboards+:: {
    local grafana = import 'grafonnet/grafana.libsonnet',
    local template = grafana.template,
    [if thanos.receive != null then 'receive.json']:
      local receiveHandlerSelector = utils.joinLabels([thanos.receive.dashboard.selector, 'handler="receive"']);
      local grpcUnaryWriteSelector = utils.joinLabels([thanos.receive.dashboard.selector, 'grpc_type="unary"', 'grpc_method="RemoteWrite"']);
      local grpcUnaryReadSelector = utils.joinLabels([thanos.receive.dashboard.selector, 'grpc_type="unary"', 'grpc_method!="RemoteWrite"']);
      local grpcServerStreamSelector = utils.joinLabels([thanos.receive.dashboard.selector, 'grpc_type="server_stream"']);

      local tenantReceiveHandlerSeclector = utils.joinLabels([thanos.receive.dashboard.tenantSelector, 'handler="receive"']);
      local tenantHttpCode2XXSelector = std.join(', ', [tenantReceiveHandlerSeclector, 'code=~"2.."']);
      local tenantHttpCodeNot2XXSelector = std.join(', ', [tenantReceiveHandlerSeclector, 'code!~"2.."']);

      local tenantWithHttpCodeDimensions = std.join(', ', ['tenant', 'code']);
      g.dashboard(thanos.receive.title) {
        templating+: {
          list+: [
            template.new(
              'tenant',
              '$datasource',
              'label_values(http_requests_total{%s}, %s)' % [std.join(', ', [thanos.receive.dashboard.selector] + ['tenant!=""']), 'tenant'],
              label='tenant',
              refresh=1,
              sort=2,
              current='all',
              allValues=null,
              includeAll=true
            ),
          ],
        },
      }
      .addRow(
        g.row('WRITE - Incoming Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of incoming requests.') +
          g.httpQpsPanel('http_requests_total', receiveHandlerSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
          g.httpErrPanel('http_requests_total', receiveHandlerSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle incoming requests in quantiles.') +
          g.latencyPanel('http_request_duration_seconds', receiveHandlerSelector, thanos.receive.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('WRITE - Incoming Request (tenant focus)')
        .addPanel(
          g.panel('Rate of write requests (by tenant and code)') +
          g.queryPanel(
            'sum by (%s) (rate(http_requests_total{%s}[$__rate_interval]))' % [tenantWithHttpCodeDimensions, tenantReceiveHandlerSeclector],
            '{{code}} - {{tenant}}'
          )
        )
        .addPanel(
          g.panel('Number of errors (by tenant and code)') +
          g.queryPanel(
            'sum by (%s) (rate(http_requests_total{%s}[$__rate_interval]))' % [
              tenantWithHttpCodeDimensions,
              tenantHttpCodeNot2XXSelector,
            ],
            '{{code}} - {{tenant}}'
          )
        )
        .addPanel(
          g.panel('Average request duration (by tenant)') +
          g.queryPanel(
            'sum by (%s) (rate(http_request_duration_seconds_sum{%s}[$__rate_interval])) / sum by (%s) (http_request_duration_seconds_count{%s})' % [
              thanos.receive.dashboard.tenantDimensions,
              tenantReceiveHandlerSeclector,
              thanos.receive.dashboard.tenantDimensions,
              tenantReceiveHandlerSeclector,
            ],
            '{{tenant}}'
          )
        )
      )
      .addRow(
        g.row('HTTP requests (tenant focus)')
        .addPanel(
          g.panel('Average successful HTTP request size (per tenant and code, only 2XX)') +
          g.queryPanel(
            'sum by (%s) (rate(http_request_size_bytes_sum{%s}[$__rate_interval])) / sum by (%s) (rate(http_request_size_bytes_count{%s}[$__rate_interval]))' % [
              thanos.receive.dashboard.tenantDimensions,
              tenantHttpCode2XXSelector,
              thanos.receive.dashboard.tenantDimensions,
              tenantHttpCode2XXSelector,
            ],
            '{{tenant}}'
          )
        )
        .addPanel(
          g.panel('Average failed HTTP request size (per tenant and code, non 2XX)') +
          g.queryPanel(
            'sum by (%s) (rate(http_request_size_bytes_sum{%s}[$__rate_interval])) / sum by (%s) (rate(http_request_size_bytes_count{%s}[$__rate_interval]))' % [
              thanos.receive.dashboard.tenantDimensions,
              tenantHttpCodeNot2XXSelector,
              thanos.receive.dashboard.tenantDimensions,
              tenantHttpCodeNot2XXSelector,
            ],
            '{{tenant}}'
          )
        )
        .addPanel(
          g.panel('Inflight requests (per tenant and method)') +
          g.queryPanel(
            'sum by (%s) (http_inflight_requests{%s})' % [
              std.join(', ', [thanos.receive.dashboard.tenantDimensions, 'method']),
              tenantReceiveHandlerSeclector,
            ],
            '{{method}} - {{tenant}}'
          )
        )
      )
      .addRow(
        g.row('Series & Samples (tenant focus)')
        .addPanel(
          g.panel('Rate of series received (per tenant, only 2XX)') +
          g.queryPanel(
            'sum(rate(thanos_receive_write_timeseries_bucket{%s}[$__rate_interval])) by (%s) ' % [
              utils.joinLabels([thanos.receive.dashboard.tenantSelector, 'code=~"2.."']),
              thanos.receive.dashboard.tenantDimensions,
            ],
            '{{tenant}}'
          )
        )
        .addPanel(
          g.panel('Rate of series not written (per tenant and code, non 2XX)') +
          g.queryPanel(
            'sum(rate(thanos_receive_write_timeseries_bucket{%s}[$__rate_interval])) by (%s) ' % [
              utils.joinLabels([thanos.receive.dashboard.tenantSelector, 'code!~"2.."']),
              tenantWithHttpCodeDimensions,
            ],
            '{{code}} - {{tenant}}'
          )
        )
        .addPanel(
          g.panel('Rate of samples received (per tenant, only 2XX)') +
          g.queryPanel(
            'sum(rate(thanos_receive_write_samples_sum{%s}[$__rate_interval])) by (%s) ' % [
              utils.joinLabels([thanos.receive.dashboard.tenantSelector, 'code=~"2.."']),
              thanos.receive.dashboard.tenantDimensions,
            ],
            '{{tenant}}'
          )
        )
        .addPanel(
          g.panel('Rate of samples not written (per tenant and code, non 2XX)') +
          g.queryPanel(
            'sum(rate(thanos_receive_write_samples_sum{%s}[$__rate_interval])) by (%s) ' % [
              utils.joinLabels([thanos.receive.dashboard.tenantSelector, 'code!~"2.."']),
              tenantWithHttpCodeDimensions,
            ],
            '{{code}} - {{tenant}}'
          )
        )
      )
      .addRow(
        g.row('WRITE - Replication')
        .addPanel(
          g.panel('Rate', 'Shows rate of replications to other receive nodes.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_receive_replications_total{%s}[$__rate_interval]))' % [thanos.receive.dashboard.dimensions, thanos.receive.dashboard.selector],
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of replications to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_replications_total{%s}' % utils.joinLabels([thanos.receive.dashboard.selector, 'result="error"']),
            'thanos_receive_replications_total{%s}' % thanos.receive.dashboard.selector,
            thanos.receive.dashboard.dimensions
          )
        )
      )
      .addRow(
        g.row('WRITE - Forward Request')
        .addPanel(
          g.panel('Rate', 'Shows rate of forwarded requests to other receive nodes.') +
          g.queryPanel(
            'sum by (%s) (rate(thanos_receive_forward_requests_total{%s}[$__rate_interval]))' % [thanos.receive.dashboard.dimensions, thanos.receive.dashboard.selector],
            'all {{job}}',
          )
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of forwareded requests to other receive nodes.') +
          g.qpsErrTotalPanel(
            'thanos_receive_forward_requests_total{%s}' % utils.joinLabels([thanos.receive.dashboard.selector, 'result="error"']),
            'thanos_receive_forward_requests_total{%s}' % thanos.receive.dashboard.selector,
            thanos.receive.dashboard.dimensions
          )
        )
      )
      .addRow(
        // TODO(https://github.com/thanos-io/thanos/issues/3926)
        g.row('WRITE - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcUnaryWriteSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcUnaryWriteSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcUnaryWriteSelector, thanos.receive.dashboard.dimensions)
        )
      )
      .addRow(
        // TODO(https://github.com/thanos-io/thanos/issues/3926)
        g.row('READ - gRPC (Unary)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Unary gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcUnaryReadSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcUnaryReadSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcUnaryReadSelector, thanos.receive.dashboard.dimensions)
        )
      )
      .addRow(
        // TODO(https://github.com/thanos-io/thanos/issues/3926)
        g.row('READ - gRPC (Stream)')
        .addPanel(
          g.panel('Rate', 'Shows rate of handled Streamed gRPC requests from queriers.') +
          g.grpcRequestsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Errors', 'Shows ratio of errors compared to the total number of handled requests from queriers.') +
          g.grpcErrorsPanel('grpc_server_handled_total', grpcServerStreamSelector, thanos.receive.dashboard.dimensions)
        )
        .addPanel(
          g.panel('Duration', 'Shows how long has it taken to handle requests from queriers, in quantiles.') +
          g.latencyPanel('grpc_server_handling_seconds', grpcServerStreamSelector, thanos.receive.dashboard.dimensions)
        )
      )
      .addRow(
        g.row('Last Updated')
        .addPanel(
          g.panel('Successful Upload', 'Shows the relative time of last successful upload to the object-store bucket.') +
          g.tablePanel(
            ['time() - max by (%s) (thanos_objstore_bucket_last_successful_upload_time{%s})' % [utils.joinLabels([thanos.receive.dashboard.dimensions, 'bucket']), thanos.receive.dashboard.selector]],
            {
              Value: {
                alias: 'Uploaded Ago',
                unit: 's',
                type: 'number',
              },
            },
          )
        )
      )
      .addRow(
        g.resourceUtilizationRow(thanos.receive.dashboard.selector, thanos.receive.dashboard.dimensions)
      ),

    __overviewRows__+:: if thanos.receive == null then [] else [
      g.row('Receive')
      .addPanel(
        g.panel('Incoming Requests Rate', 'Shows rate of incoming requests.') +
        g.httpQpsPanel('http_requests_total', utils.joinLabels([thanos.dashboard.overview.selector, 'handler="receive"']), thanos.dashboard.overview.dimensions) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.panel('Incoming Requests Errors', 'Shows ratio of errors compared to the total number of handled incoming requests.') +
        g.httpErrPanel('http_requests_total', utils.joinLabels([thanos.dashboard.overview.selector, 'handler="receive"']), thanos.dashboard.overview.dimensions) +
        g.addDashboardLink(thanos.receive.title)
      )
      .addPanel(
        g.sloLatency(
          'Incoming Requests Latency 99th Percentile',
          'Shows how long has it taken to handle incoming requests.',
          'http_request_duration_seconds_bucket{%s}' % utils.joinLabels([thanos.dashboard.overview.selector, 'handler="receive"']),
          thanos.dashboard.overview.dimensions,
          0.99,
          0.5,
          1
        ) +
        g.addDashboardLink(thanos.receive.title)
      ),
    ],
  },
}
