{
  /**
   * Return a CloudWatch Target
   *
   * @param region
   * @param namespace
   * @param metric
   * @param datasource
   * @param statistic
   * @param alias
   * @param highResolution
   * @param period
   * @param dimensions

   * @return Panel target
   */

  target(
    region,
    namespace,
    metric,
    datasource=null,
    statistic='Average',
    alias=null,
    highResolution=false,
    period='1m',
    dimensions={}
  ):: {
    region: region,
    namespace: namespace,
    metricName: metric,
    [if datasource != null then 'datasource']: datasource,
    statistics: [statistic],
    [if alias != null then 'alias']: alias,
    highResolution: highResolution,
    period: period,
    dimensions: dimensions,
  },
}
