{
  local thanos = self,
  receiver+:: {
    jobPrefix: error 'must provide job prefix for Thanos Receive alerts',
    selector: error 'must provide selector for Thanos Receive alerts',
  },
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-receiver.rules',
        rules: [
          {
            alert: 'ThanosReceiverHttpRequestErrorRateHigh',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to handle {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum(rate(http_requests_total{code=~"5..", %(selector)s, handler="receive"}[5m]))
              /
                sum(rate(http_requests_total{%(selector)s, handler="receive"}[5m]))
              ) * 100 > 5
            ||| % thanos.receiver,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiverHttpRequestLatencyHigh',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for requests.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (job, le) (http_request_duration_seconds_bucket{%(selector)s, handler="receive"})) > 10
              and
                sum by (job) (rate(http_request_duration_seconds_count{%(selector)s, handler="receive"}[5m])) > 0
              )
            ||| % thanos.receiver,
            'for': '10m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiverHighForwardRequestFailures',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to forward {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_receive_forward_requests_total{result="error", %(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_receive_forward_requests_total{%(selector)s}[5m]))
              * 100 > 5
              )
            ||| % thanos.receiver,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiverHighHashringFileRefreshFailures',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to refresh hashring file, {{ $value | humanize }} of attempts failed.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_receive_hashrings_file_errors_total{%(selector)s}[5m]))
              /
                sum by (job) (rate(thanos_receive_hashrings_file_refreshes_total{%(selector)s}[5m]))
              > 0
              )
            ||| % thanos.receiver,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosReceiverConfigReloadFailure',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} has not been able to reload hashring configurations.',
            },
            expr: 'avg(thanos_receive_config_last_reload_successful{%(selector)s}) by (job) != 1' % thanos.receiver,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
        ],
      },
    ],
  },
}
