{
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'thanos-receive.rules',
        rules: [
          {
            alert: 'ThanosReceiveHttpRequestErrorRateHigh',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to handle {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum(rate(http_requests_total{code=~"5..", %(thanosReceiveSelector)s, handler="receive"}[5m]))
              /
                sum(rate(http_requests_total{%(thanosReceiveSelector)s, handler="receive"}[5m]))
              ) * 100 > 5
            ||| % $._config,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiveHttpRequestLatencyHigh',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} has a 99th percentile latency of {{ $value }} seconds for requests.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (job, le) (http_request_duration_seconds_bucket{%(thanosReceiveSelector)s, handler="receive"})) > 10
              and
                sum by (job) (rate(http_request_duration_seconds_count{%(thanosReceiveSelector)s, handler="receive"}[5m])) > 0
              )
            ||| % $._config,
            'for': '10m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiveHighForwardRequestFailures',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to forward {{ $value | humanize }}% of requests.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_receive_forward_requests_total{result="error", %(thanosReceiveSelector)s}[5m]))
              /
                sum by (job) (rate(thanos_receive_forward_requests_total{%(thanosReceiveSelector)s}[5m]))
              * 100 > 5
              )
            ||| % $._config,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiveHighHashringFileRefreshFailures',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} is failing to refresh hashring file, {{ $value | humanize }} of attempts failed.',
            },
            expr: |||
              (
                sum by (job) (rate(thanos_receive_hashrings_file_errors_total{%(thanosReceiveSelector)s}[5m]))
              /
                sum by (job) (rate(thanos_receive_hashrings_file_refreshes_total{%(thanosReceiveSelector)s}[5m]))
              > 0
              )
            ||| % $._config,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosReceiveConfigReloadFailure',
            annotations: {
              message: 'Thanos Receive {{$labels.job}} has not been able to reload hashring configurations.',
            },
            expr: 'avg(thanos_receive_config_last_reload_successful{%(thanosReceiveSelector)s}) by (job) != 1' % $._config,
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
