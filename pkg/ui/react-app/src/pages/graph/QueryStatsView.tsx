import React, { FC } from 'react';

export interface QueryStats {
  loadTime: number;
  resolution: number;
  resultSeries: number;
  traceID: string | null;
}

const QueryStatsView: FC<QueryStats> = ({ loadTime, resolution, resultSeries, traceID }) => {
  return (
    <div className="query-stats">
      <span className="float-right">
        Load time: {loadTime}ms &ensp; Resolution: {resolution}s &ensp; Result series: {resultSeries}
        {traceID ? <React.Fragment>&ensp; Trace ID: {traceID}</React.Fragment> : null}
      </span>
    </div>
  );
};

export default QueryStatsView;
