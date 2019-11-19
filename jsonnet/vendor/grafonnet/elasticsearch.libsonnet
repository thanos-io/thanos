{
  target(
    query,
    id=null,
    datasource=null,
    metrics=[{
      field: 'value',
      id: null,
      type: 'percentiles',
      settings: {
        percents: [
          '90',
        ],
      },
    }],
    bucketAggs=[{
      field: 'timestamp',
      id: null,
      type: 'date_histogram',
      settings: {
        interval: '1s',
        min_doc_count: 0,
        trimEdges: 0,
      },
    }],
    timeField,
    alias=null,
  ):: {
    [if datasource != null then 'datasource']: datasource,
    query: query,
    id: id,
    timeField: timeField,
    bucketAggs: bucketAggs,
    metrics: metrics,
    alias: alias,
    // TODO: generate bucket ids
  },
}
