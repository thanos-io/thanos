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

  template(name, metricName, selector='', includeAll=false, allValues='')::
    local t = if includeAll then
      template.new(
        name,
        '$datasource',
        'label_values(%s{%s}, %s)' % [metricName, selector, name],
        label=name,
        refresh=1,
        sort=2,
        current='all',
        allValues=allValues,
        includeAll=true
      )
    else
      template.new(
        name,
        '$datasource',
        'label_values(%s{%s}, %s)' % [metricName, selector, name],
        label=name,
        refresh=1,
        sort=2,
      );

    {
      templating+: {
        list+: [
          t,
        ],
      },
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

  latencyPanel(metricName, selector, multiplier='1'):: {
    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum(rate(%s_bucket{%s}[$interval])) by (job, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P99 {{job}}',
        refId: 'A',
        step: 10,
      },
      {
        expr: 'sum(rate(%s_sum{%s}[$interval])) by (job) * %s / sum(rate(%s_count{%s}[$interval])) by (job)' % [metricName, selector, multiplier, metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'mean {{job}}',
        refId: 'B',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum(rate(%s_bucket{%s}[$interval])) by (job, le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'P50 {{job}}',
        refId: 'C',
        step: 10,
      },
    ],
    yaxes: $.yaxes('s'),
  },

  qpsErrTotalPanel(selectorErr, selectorTotal):: {
    local expr(selector) = 'sum(rate(' + selector + '[$interval]))',  // {{job}}

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

  qpsSuccErrRatePanel(selectorErr, selectorTotal):: {
    local expr(selector) = 'sum(rate(' + selector + '[$interval]))',  // {{job}}

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

  resourceUtilizationRow()::
    $.row('Resources')
    .addPanel(
      $.panel('Memory Used') +
      $.queryPanel(
        [
          'go_memstats_alloc_bytes{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}',
          'go_memstats_heap_alloc_bytes{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}',
          'rate(go_memstats_alloc_bytes_total{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}[30s])',
          'rate(go_memstats_heap_alloc_bytes{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}[30s])',
          'go_memstats_stack_inuse_bytes{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}',
          'go_memstats_heap_inuse_bytes{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}',
        ],
        [
          'alloc all {{pod}}',
          'alloc heap {{pod}}',
          'alloc rate all {{pod}}',
          'alloc rate heap {{pod}}',
          'inuse stack {{pod}}',
          'inuse heap {{pod}}',
        ]
      ) +
      { yaxes: $.yaxes('bytes') },
    )
    .addPanel(
      $.panel('Goroutines') +
      $.queryPanel(
        'go_goroutines{namespace="$namespace",job=~"$job"}',
        '{{pod}}'
      )
    )
    .addPanel(
      $.panel('GC Time Quantiles') +
      $.queryPanel(
        'go_gc_duration_seconds{namespace="$namespace",job=~"$job",kubernetes_pod_name=~"$pod"}',
        '{{quantile}} {{pod}}'
      )
    ) +
    $.collapse,
} +
(import 'grpc.libsonnet') +
(import 'http.libsonnet') +
(import 'slo.libsonnet')
