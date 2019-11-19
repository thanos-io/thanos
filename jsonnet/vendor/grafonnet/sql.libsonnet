{
  target(
    rawSql,
    datasource=null,
    format='time_series',
  ):: {
    [if datasource != null then 'datasource']: datasource,
    format: format,
    rawSql: rawSql,
  },
}
