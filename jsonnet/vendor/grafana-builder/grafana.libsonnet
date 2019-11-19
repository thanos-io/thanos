{
  dashboard(title, uid=''):: {
    // Stuff that isn't materialised.
    _nextPanel:: 1,
    addRow(row):: self {
      // automatically number panels in added rows.
      local n = std.length(row.panels),
      local nextPanel = super._nextPanel,
      local panels = std.makeArray(n, function(i)
        row.panels[i] { id: nextPanel + i }),

      _nextPanel: nextPanel + n,
      rows+: [row { panels: panels }],
    },

    addTemplate(name, metric_name, label_name, hide=0):: self {
      templating+: {
        list+: [{
          allValue: null,
          current: {
            text: 'prod',
            value: 'prod',
          },
          datasource: '$datasource',
          hide: hide,
          includeAll: false,
          label: name,
          multi: false,
          name: name,
          options: [],
          query: 'label_values(%s, %s)' % [metric_name, label_name],
          refresh: 1,
          regex: '',
          sort: 2,
          tagValuesQuery: '',
          tags: [],
          tagsQuery: '',
          type: 'query',
          useTags: false,
        }],
      },
    },

    addMultiTemplate(name, metric_name, label_name, hide=0):: self {
      templating+: {
        list+: [{
          allValue: null,
          current: {
            selected: true,
            text: 'All',
            value: '$__all',
          },
          datasource: '$datasource',
          hide: hide,
          includeAll: true,
          label: name,
          multi: true,
          name: name,
          options: [],
          query: 'label_values(%s, %s)' % [metric_name, label_name],
          refresh: 1,
          regex: '',
          sort: 2,
          tagValuesQuery: '',
          tags: [],
          tagsQuery: '',
          type: 'query',
          useTags: false,
        }],
      },
    },

    // Stuff that is materialised.
    uid: uid,
    annotations: {
      list: [],
    },
    hideControls: false,
    links: [],
    rows: [],
    schemaVersion: 14,
    style: 'dark',
    tags: [],
    editable: true,
    gnetId: null,
    graphTooltip: 0,
    templating: {
      list: [
        {
          current: {
            text: 'Prometheus',
            value: 'Prometheus',
          },
          hide: 0,
          label: null,
          name: 'datasource',
          options: [],
          query: 'prometheus',
          refresh: 1,
          regex: '',
          type: 'datasource',
        },
      ],
    },
    time: {
      from: 'now-1h',
      to: 'now',
    },
    refresh: '10s',
    timepicker: {
      refresh_intervals: [
        '5s',
        '10s',
        '30s',
        '1m',
        '5m',
        '15m',
        '30m',
        '1h',
        '2h',
        '1d',
      ],
      time_options: [
        '5m',
        '15m',
        '1h',
        '6h',
        '12h',
        '24h',
        '2d',
        '7d',
        '30d',
      ],
    },
    timezone: 'utc',
    title: title,
    version: 0,
  },

  row(title):: {
    _panels:: [],
    addPanel(panel):: self {
      _panels+: [panel],
    },

    panels:
      // Automatically distribute panels within a row.
      local n = std.length(self._panels);
      [
        p { span: std.floor(12 / n) }
        for p in self._panels
      ],

    collapse: false,
    height: '250px',
    repeat: null,
    repeatIteration: null,
    repeatRowId: null,
    showTitle: true,
    title: title,
    titleSize: 'h6',
  },

  panel(title):: {
    aliasColors: {},
    bars: false,
    dashLength: 10,
    dashes: false,
    datasource: '$datasource',
    fill: 1,
    legend: {
      avg: false,
      current: false,
      max: false,
      min: false,
      show: true,
      total: false,
      values: false,
    },
    lines: true,
    linewidth: 1,
    links: [],
    nullPointMode: 'null as zero',
    percentage: false,
    pointradius: 5,
    points: false,
    renderer: 'flot',
    seriesOverrides: [],
    spaceLength: 10,
    span: 6,
    stack: false,
    steppedLine: false,
    targets: [],
    thresholds: [],
    timeFrom: null,
    timeShift: null,
    title: title,
    tooltip: {
      shared: true,
      sort: 0,
      value_type: 'individual',
    },
    type: 'graph',
    xaxis: {
      buckets: null,
      mode: 'time',
      name: null,
      show: true,
      values: [],
    },
    yaxes: $.yaxes('short'),
  },

  queryPanel(queries, legends, legendLink=null):: {

    local qs =
      if std.type(queries) == 'string'
      then [queries]
      else queries,
    local ls =
      if std.type(legends) == 'string'
      then [legends]
      else legends,

    local qsandls = if std.length(ls) == std.length(qs)
    then std.makeArray(std.length(qs), function(x) { q: qs[x], l: ls[x] })
    else error 'length of queries is not equal to length of legends',

    targets+: [
      {
        legendLink: legendLink,
        expr: ql.q,
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: ql.l,
        step: 10,
      }
      for ql in qsandls
    ],
  },

  statPanel(query, format='percentunit'):: {
    type: 'singlestat',
    thresholds: '70,80',
    format: format,
    targets: [
      {
        expr: query,
        format: 'time_series',
        instant: true,
        intervalFactor: 2,
        refId: 'A',
      },
    ],
  },

  tablePanel(queries, labelStyles):: {
    local qs =
      if std.type(queries) == 'string'
      then [queries]
      else queries,

    local style(labelStyle) =
      if std.type(labelStyle) == 'string'
      then {
        alias: labelStyle,
        colorMode: null,
        colors: [],
        dateFormat: 'YYYY-MM-DD HH:mm:ss',
        decimals: 2,
        thresholds: [],
        type: 'string',
        unit: 'short',
      }
      else {
        alias: labelStyle.alias,
        colorMode: null,
        colors: [],
        dateFormat: 'YYYY-MM-DD HH:mm:ss',
        decimals: if std.objectHas(labelStyle, 'decimals') then labelStyle.decimals else 2,
        thresholds: [],
        type: if std.objectHas(labelStyle, 'type') then labelStyle.type else 'number',
        unit: if std.objectHas(labelStyle, 'unit') then labelStyle.unit else 'short',
        link: std.objectHas(labelStyle, 'link'),
        linkTooltip: if std.objectHas(labelStyle, 'linkTooltip') then labelStyle.linkTooltip else 'Drill down',
        linkUrl: if std.objectHas(labelStyle, 'link') then labelStyle.link else '',
      },

    _styles:: {
      // By default hide time.
      Time: {
        alias: 'Time',
        dateFormat: 'YYYY-MM-DD HH:mm:ss',
        type: 'hidden',
      },
    } + {
      [label]: style(labelStyles[label])
      for label in std.objectFields(labelStyles)
    },

    styles: [
      self._styles[pattern] { pattern: pattern }
      for pattern in std.objectFields(self._styles)
    ] + [style('') + { pattern: '/.*/' }],

    transform: 'table',
    type: 'table',
    targets: [
      {
        expr: qs[i],
        format: 'table',
        instant: true,
        intervalFactor: 2,
        legendFormat: '',
        step: 10,
        refId: std.char(65 + i),
      }
      for i in std.range(0, std.length(qs) - 1)
    ],
  },

  stack:: {
    stack: true,
    fill: 10,
    linewidth: 0,
  },

  yaxes(args)::
    local format = if std.type(args) == 'string' then args else null;
    local options = if std.type(args) == 'object' then args else {};
    [
      {
        format: format,
        label: null,
        logBase: 1,
        max: null,
        min: 0,
        show: true,
      } + options,
      {
        format: 'short',
        label: null,
        logBase: 1,
        max: null,
        min: null,
        show: false,
      },
    ],

  qpsPanel(selector):: {
    aliasColors: {
      '1xx': '#EAB839',
      '2xx': '#7EB26D',
      '3xx': '#6ED0E0',
      '4xx': '#EF843C',
      '5xx': '#E24D42',
      success: '#7EB26D',
      'error': '#E24D42',
    },
    targets: [
      {
        expr: 'sum by (status) (label_replace(label_replace(rate(' + selector + '[$__interval]),'
              + ' "status", "${1}xx", "status_code", "([0-9]).."),'
              + ' "status", "${1}",   "status_code", "([a-z]+)"))',
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '{{status}}',
        refId: 'A',
        step: 10,
      },
    ],
  } + $.stack,

  latencyPanel(metricName, selector, multiplier='1e3'):: {
    nullPointMode: 'null as zero',
    targets: [
      {
        expr: 'histogram_quantile(0.99, sum(rate(%s_bucket%s[$__interval])) by (le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '99th Percentile',
        refId: 'A',
        step: 10,
      },
      {
        expr: 'histogram_quantile(0.50, sum(rate(%s_bucket%s[$__interval])) by (le)) * %s' % [metricName, selector, multiplier],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: '50th Percentile',
        refId: 'B',
        step: 10,
      },
      {
        expr: 'sum(rate(%s_sum%s[$__interval])) * %s / sum(rate(%s_count%s[$__interval]))' % [metricName, selector, multiplier, metricName, selector],
        format: 'time_series',
        intervalFactor: 2,
        legendFormat: 'Average',
        refId: 'C',
        step: 10,
      },
    ],
    yaxes: $.yaxes('ms'),
  },

  selector:: {
    eq(label, value):: { label: label, op: '=', value: value },
    neq(label, value):: { label: label, op: '!=', value: value },
    re(label, value):: { label: label, op: '=~', value: value },
    nre(label, value):: { label: label, op: '!~', value: value },
  },

  toPrometheusSelector(selector)::
    local pairs = [
      '%(label)s%(op)s"%(value)s"' % matcher
      for matcher in selector
    ];
    '{%s}' % std.join(', ', pairs),
}
