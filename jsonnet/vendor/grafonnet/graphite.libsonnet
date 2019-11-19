{
  /**
   * Return an Graphite Target
   *
   * @param target Graphite Query. Nested queries are possible by adding the query reference (refId).
   * @param targetFull Expanding the @target. Used in nested queries.
   * @param hide Disable query on graph.
   * @param textEditor Enable raw query mode.
   * @param datasource Datasource.

   * @return Panel target
   */
  target(
    target,
    targetFull=null,
    hide=false,
    textEditor=false,
    datasource=null,
  ):: {
    target: target,
    hide: hide,
    textEditor: textEditor,

    [if targetFull != null then 'targetFull']: targetFull,
    [if datasource != null then 'datasource']: datasource,
  },
}
