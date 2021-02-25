import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import { UncontrolledAlert } from 'reactstrap';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Store } from './store';
import { StorePoolPanel } from './StorePoolPanel';

export interface StoreListProps {
  [storeType: string]: Store[];
}

export const StoreContent: FC<{ data: StoreListProps }> = ({ data }) => {
  const storePools = Object.keys(data);
  return (
    <>
      {storePools.length > 0 ? (
        storePools.map<JSX.Element>((storeGroup) => (
          <StorePoolPanel key={storeGroup} title={storeGroup} storePool={data[storeGroup]} />
        ))
      ) : (
        <UncontrolledAlert color="warning">No stores registered.</UncontrolledAlert>
      )}
    </>
  );
};

const StoresWithStatusIndicator = withStatusIndicator(StoreContent);

export const Stores: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix = '' }) => {
  const { response, error, isLoading } = useFetch<StoreListProps>(`${pathPrefix}/api/v1/stores`);
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';

  return (
    <StoresWithStatusIndicator
      data={response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
    />
  );
};

export default Stores;
