{
  local thanos = self,
  receive+:: {
    selector: error 'must provide selector for Thanos Receive alerts',
    httpErrorThreshold: 5,
    ingestionThreshold: 50,
    forwardErrorThreshold: 20,
    refreshErrorThreshold: 0,
    p99LatencyThreshold: 10,
    dimensions: std.join(', ', std.objectFields(thanos.targetGroups) + ['job']),
  },
  prometheusAlerts+:: {
    groups+: if thanos.receive == null then [] else [
      local location = if std.length(std.objectFields(thanos.targetGroups)) > 0 then ' in %s' % std.join('/', ['{{$labels.%s}}' % level for level in std.objectFields(thanos.targetGroups)]) else '';
      {
        name: 'thanos-receive',
        rules: [
          {
            alert: 'ThanosReceiveHttpRequestErrorRateHigh',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s is failing to handle {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Receive is failing to handle requests.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(http_requests_total{code=~"5..", %(selector)s, handler="receive"}[5m]))
              /
                sum by (%(dimensions)s) (rate(http_requests_total{%(selector)s, handler="receive"}[5m]))
              ) * 100 > %(httpErrorThreshold)s
            ||| % thanos.receive,
            'for': '5m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiveHttpRequestLatencyHigh',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s has a 99th percentile latency of {{ $value }} seconds for requests.' % location,
              summary: 'Thanos Receive has high HTTP requests latency.',
            },
            expr: |||
              (
                histogram_quantile(0.99, sum by (%(dimensions)s, le) (rate(http_request_duration_seconds_bucket{%(selector)s, handler="receive"}[5m]))) > %(p99LatencyThreshold)s
              and
                sum by (%(dimensions)s) (rate(http_request_duration_seconds_count{%(selector)s, handler="receive"}[5m])) > 0
              )
            ||| % thanos.receive,
            'for': '10m',
            labels: {
              severity: 'critical',
            },
          },
          {
            alert: 'ThanosReceiveHighReplicationFailures',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s is failing to replicate {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Receive is having high number of replication failures.',
            },
            expr: |||
              thanos_receive_replication_factor > 1
                and
              (
                (
                  sum by (%(dimensions)s) (rate(thanos_receive_replications_total{result="error", %(selector)s}[5m]))
                /
                  sum by (%(dimensions)s) (rate(thanos_receive_replications_total{%(selector)s}[5m]))
                )
                >
                (
                  max by (%(dimensions)s) (floor((thanos_receive_replication_factor{%(selector)s}+1) / 2))
                /
                  max by (%(dimensions)s) (thanos_receive_hashring_nodes{%(selector)s})
                )
              ) * 100
            ||| % thanos.receive,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosReceiveHighForwardRequestFailures',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s is failing to forward {{$value | humanize}}%% of requests.' % location,
              summary: 'Thanos Receive is failing to forward requests.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_receive_forward_requests_total{result="error", %(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_receive_forward_requests_total{%(selector)s}[5m]))
              ) * 100 > %(forwardErrorThreshold)s
            ||| % thanos.receive,
            'for': '5m',
            labels: {
              severity: 'info',
            },
          },
          {
            alert: 'ThanosReceiveHighHashringFileRefreshFailures',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s is failing to refresh hashring file, {{$value | humanize}} of attempts failed.' % location,
              summary: 'Thanos Receive is failing to refresh hasring file.',
            },
            expr: |||
              (
                sum by (%(dimensions)s) (rate(thanos_receive_hashrings_file_errors_total{%(selector)s}[5m]))
              /
                sum by (%(dimensions)s) (rate(thanos_receive_hashrings_file_refreshes_total{%(selector)s}[5m]))
              > %(refreshErrorThreshold)s
              )
            ||| % thanos.receive,
            'for': '15m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosReceiveConfigReloadFailure',
            annotations: {
              description: 'Thanos Receive {{$labels.job}}%s has not been able to reload hashring configurations.' % location,
              summary: 'Thanos Receive has not been able to reload configuration.',
            },
            expr: 'avg by (%(dimensions)s) (thanos_receive_config_last_reload_successful{%(selector)s}) != 1' % thanos.receive,
            'for': '5m',
            labels: {
              severity: 'warning',
            },
          },
          {
            alert: 'ThanosReceiveNoUpload',
            annotations: {
              description: 'Thanos Receive {{$labels.instance}}%s has not uploaded latest data to object storage.' % location,
              summary: 'Thanos Receive has not uploaded latest data to object storage.',
            },
            expr: |||
              (up{%(selector)s} - 1)
              + on (%(dimensions)s, instance) # filters to only alert on current instance last 3h
              (sum by (%(dimensions)s, instance) (increase(thanos_shipper_uploads_total{%(selector)s}[3h])) == 0)
            ||| % thanos.receive,
            'for': '3h',
            labels: {
              severity: 'critical',
            },
          },
        ],
      },
    ],
  },
}
