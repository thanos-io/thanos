import React, { FC } from 'react';

export interface QueryStats {
  loadTime: number;
  resolution: number;
  resultSeries: number;
  traceID: string;
}

const QueryStatsView: FC<QueryStats> = (props) => {
  const { loadTime, resolution, resultSeries, traceID } = props;

  return (
    <div className="query-stats">
      <span className="float-right">
        Load time: {loadTime}ms &ensp; Resolution: {resolution}s &ensp; Result series: {resultSeries} &ensp; TraceID: {traceID}
      </span>
    </div>
  );
};

export default QueryStatsView;
