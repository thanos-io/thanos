{
  /**
   * Return an InfluxDB Target
   *
   * @param query Raw InfluxQL statement
   * @param alias Alias By pattern
   * @param datasource Datasource
   * @param rawQuery En/Disable raw query mode
   * @param resultFormat Format results as 'Time series' or 'Table'

   * @return Panel target
   */
  target(
    query,
    alias=null,
    datasource=null,
    rawQuery=true,
    resultFormat='time_series',
  ):: {
    query: query,
    rawQuery: rawQuery,
    resultFormat: resultFormat,

    [if alias != null then 'alias']: alias,
    [if datasource != null then 'datasource']: datasource,
  },
}
