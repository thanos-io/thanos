local grafana = import 'grafonnet/grafana.libsonnet';
local template = grafana.template;

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

  latencyPanel(metricName, selector, aggregator, multiplier='1'):: {
    local aggregatedLabels = std.split(aggregator, ','),
    local aggregatorTemplate = std.join(' ', ['{{%s}}' % label for label in aggregatedLabels]),
    local params = { metricName: metricName, selector: selector, aggregator: aggregator, multiplier: multiplier },

    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum by (%(aggregator)s, le) (rate(%(metricName)s_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P99 ' + aggregatorTemplate,
        refId: 'A',
        step: 10,
      },
      {
        expr: 'sum by (%(aggregator)s) (rate(%(metricName)s_sum{%(selector)s}[$interval])) * %(multiplier)s / sum by (%(aggregator)s) (rate(%(metricName)s_count{%(selector)s}[$interval]))' % params,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'mean ' + aggregatorTemplate,
        refId: 'B',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum by (%(aggregator)s, le) (rate(%(metricName)s_bucket{%(selector)s}[$interval]))) * %(multiplier)s' % params,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P50 ' + aggregatorTemplate,
        refId: 'C',
        step: 10,
      },
    ],
    yaxes: $.yaxes('s'),
  },

  qpsErrTotalPanel(selectorErr, selectorTotal, aggregator):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [aggregator, selector],

    aliasColors: {
      'error': '#E24D42',
    },
    targets: [
      {
        expr: '%s / %s' % [expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'error',
        refId: 'A',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit' }),
  } + $.stack,

  qpsSuccErrRatePanel(selectorErr, selectorTotal, aggregator):: {
    local expr(selector) = 'sum by (%s) (rate(%s[$interval]))' % [aggregator, selector],

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
        refId: 'A',
        step: 10,
      },
      {
        expr: '(%s - %s) / %s' % [expr(selectorTotal), expr(selectorErr), expr(selectorTotal)],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'success',
        refId: 'B',
        step: 10,
      },
    ],
    yaxes: $.yaxes({ format: 'percentunit', max: 1 }),
  } + $.stack,

  resourceUtilizationRow(selector)::
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
          'inuse stack {{instance}}',
          'inuse heap {{instance}}',
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
