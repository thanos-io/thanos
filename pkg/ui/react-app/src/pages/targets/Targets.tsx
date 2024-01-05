import React, { FC, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import Filter from './Filter';
import ScrapePoolList from './ScrapePoolList';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { UncontrolledAlert } from 'reactstrap';

const Targets: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  const [isLoading, setIsLoading] = useState(false);

  const handleLoadingChange = (loadingState: boolean) => {
    setIsLoading(loadingState);
  };

  return (
    <>
      {isLoading ? (
        <UncontrolledAlert color="danger">
          Sometimes the targets page doesn't fully load because it takes too long to load targets from leaf nodes. Please
          wait some time or try again later.
        </UncontrolledAlert>
      ) : (
        <>
          <h2>Targets</h2>
          <ScrapePoolList pathPrefix={pathPrefix} onLoadingChange={handleLoadingChange} />
        </>
      )}
    </>
  );
};

export default Targets;
