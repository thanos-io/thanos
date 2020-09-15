import React, { FC } from 'react';
import { UncontrolledAlert } from 'reactstrap';
import Graph from './Graph';
import { QueryParams } from '../../types/types';
import { isPresent } from '../../utils';

interface GraphTabContentProps {
  data: any;
  stacked: boolean;
  useLocalTime: boolean;
  lastQueryParams: QueryParams | null;
}

export const GraphTabContent: FC<GraphTabContentProps> = ({ data, stacked, useLocalTime, lastQueryParams }) => {
  if (!isPresent(data)) {
    return <UncontrolledAlert color="light">No data queried yet</UncontrolledAlert>;
  }
  if (data.result.length === 0) {
    return <UncontrolledAlert color="secondary">Empty query result</UncontrolledAlert>;
  }
  if (data.resultType !== 'matrix') {
    return (
      <UncontrolledAlert color="danger">
        Query result is of wrong type '{data.resultType}', should be 'matrix' (range vector).
      </UncontrolledAlert>
    );
  }
  return <Graph data={data} stacked={stacked} useLocalTime={useLocalTime} queryParams={lastQueryParams} />;
};
