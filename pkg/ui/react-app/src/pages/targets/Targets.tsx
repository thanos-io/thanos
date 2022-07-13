import React, { FC } from 'react';
import ScrapePoolList from './ScrapePoolList';
import PathPrefixProps from '../../types/PathPrefixProps';

const Targets: FC<PathPrefixProps> = ({ pathPrefix }) => {
  const scrapePoolListProps = { pathPrefix };

  return (
    <>
      <h2>Targets</h2>
      <ScrapePoolList {...scrapePoolListProps} />
    </>
  );
};

export default Targets;
