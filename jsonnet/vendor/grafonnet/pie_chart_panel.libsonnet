{
  /**
   * Returns a new pie chart panel that can be added in a row.
   * It requires the pie chart panel plugin in grafana, which needs to be explicitly installed.
   *
   * @param title The title of the pie chart panel.
   * @param description Description of the panel
   * @param span Width of the panel
   * @param min_span Min span
   * @param datasource Datasource
   * @param aliasColors Define color mappings
   * @param pieType Type of pie chart (one of pie or donut)
   * @return A json that represents a pie chart panel
   */
  new(
    title,
    description='',
    span=null,
    min_span=null,
    datasource=null,
    height=null,
    aliasColors={},
    pieType='pie',
  ):: {
    type: 'grafana-piechart-panel',
    pieType: pieType,
    title: title,
    aliasColors: aliasColors,
    [if span != null then 'span']: span,
    [if min_span != null then 'minSpan']: min_span,
    [if height != null then 'height']: height,
    datasource: datasource,
    targets: [
    ],
    _nextTarget:: 0,
    addTarget(target):: self {
      local nextTarget = super._nextTarget,
      _nextTarget: nextTarget + 1,
      targets+: [target { refId: std.char(std.codepoint('A') + nextTarget) }],
    },
  },
}
