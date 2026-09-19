import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import Filter from './Filter';
import ScrapePoolList from './ScrapePoolList';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useLocalStorage } from '../../hooks/useLocalStorage';

const Targets: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  const scrapePoolListProps = { pathPrefix };

  return (
    <>
      <h2>Targets</h2>
      <ScrapePoolList {...scrapePoolListProps} />
    </>
  );
};

export default Targets;
