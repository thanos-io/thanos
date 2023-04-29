import React, { FC } from 'react';

export interface QueryStats {
  loadTime: number;
  resolution: number;
  resultSeries: number;
  traceID: string;
}

const QueryStatsView: FC<QueryStats> = (props) => {
  const { loadTime, resolution, resultSeries, traceID } = props;

  const base = `Load time: ${loadTime}ms &ensp; Resolution: ${resolution}s &ensp; Result series: ${resultSeries}`;
  const str = traceID ? base + ` &ensp; TraceID: ${traceID}` : base;

  return (
    <div className="query-stats">
      <span className="float-right" dangerouslySetInnerHTML={{ __html: str }}></span>
    </div>
  );
};

export default QueryStatsView;
