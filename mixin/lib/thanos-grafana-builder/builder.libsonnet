local utils = import '../utils.libsonnet';

(import 'grafana-builder/grafana.libsonnet') +
{
  collapse: {
    collapse: true,
  },

  panel(title, description=null)::
    super.panel(title) { [if description != null then 'description']: description },

  addDashboardLink(name): {
    links+: [
      {
        dashboard: name,
        includeVars: true,
        keepTime: true,
        title: name,
        type: 'dashboard',
      },
    ],
  },

  spanSize(size):: {
    span: size,
  },

  postfix(postfix):: {
    postfix: postfix,
  },

  sparkline:: {
    sparkline: {
      show: true,
      lineColor: 'rgb(31, 120, 193)',
      fillColor: 'rgba(31, 118, 189, 0.18)',
    },
  },

  latencyPanel(metricName, selector, dimensions, multiplier='1'):: {
    local aggregatedLabels = std.split(dimensions, ','),
    local dimensionsTemplate = std.join(' ', ['{{%s}}' % std.stripChars(label, ' ') for label in aggregatedLabels]),

    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(%.2f, sum by (%s) (rate(%s_bucket{%s}[$interval]))) * %s' % [percentile, utils.joinLabels([dimensions, 'le']), metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'p%d %s' % [100 * percentile, dimensionsTemplate],
        logBase: 10,
        min: null,
        max: null,
        step: 10,
      }
      for percentile in [0.5, 0.9, 0.99]
    ],
    yaxes: $.yaxes('s'),
    seriesOverrides: [
      {
        alias: 'p99',
        color: '#FA6400',
        fill: 1,
        fillGradient: 1,
      },
      {
        alias: 'p90',
        color: '#E0B400',
        fill: 1,
        fillGradient: 1,
      },
      {
        alias: 'p50',
        color: '#37872D',
        fill: 10,
        fillGradient: 0,
      },
    ],
  },

  qpsErrTotalPanel(selectorErr, selectorTotal, dimensions):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [dimensions, selector],

    aliasColors: {
      'error': '#E24D42',
    },
    targets: [
      {
        expr: '%s / %s' % [expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'error',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit' }),
  } + $.stack,

  qpsSuccErrRatePanel(selectorErr, selectorTotal, dimensions):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [dimensions, selector],

    aliasColors: {
      success: '#7EB26D',
      'error': '#E24D42',
    },
    targets: [
      {
        expr: '%s / %s' % [expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'error',
        step: 10,
      },
      {
        expr: '(%s - %s) / %s' % [expr(selectorTotal), expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'success',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit', max: 1 }),
  } + $.stack,

  resourceUtilizationRow(selector, dimensions)::
    $.row('Resources')
    .addPanel(
      $.panel('Memory Used') +
      $.queryPanel(
        [
          'go_memstats_alloc_bytes{%s}' % selector,
          'go_memstats_heap_alloc_bytes{%s}' % selector,
          'rate(go_memstats_alloc_bytes_total{%s}[30s])' % selector,
          'rate(go_memstats_heap_alloc_bytes{%s}[30s])' % selector,
          'go_memstats_stack_inuse_bytes{%s}' % selector,
          'go_memstats_heap_inuse_bytes{%s}' % selector,
        ],
        [
          'alloc all {{instance}}',
          'alloc heap {{instance}}',
          'alloc rate all {{instance}}',
          'alloc rate heap {{instance}}',
          'inuse heap {{instance}}',
          'inuse stack {{instance}}',
        ]
      ) +
      { yaxes: $.yaxes('bytes') },
    )
    .addPanel(
      $.panel('Goroutines') +
      $.queryPanel(
        'go_goroutines{%s}' % selector,
        '{{instance}}'
      )
    )
    .addPanel(
      $.panel('GC Time Quantiles') +
      $.queryPanel(
        'go_gc_duration_seconds{%s}' % selector,
        '{{quantile}} {{instance}}'
      )
    ) +
    $.collapse,
} +
(import 'grpc.libsonnet') +
(import 'http.libsonnet') +
(import 'slo.libsonnet')
