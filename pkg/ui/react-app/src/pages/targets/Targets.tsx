import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import Filter from './Filter';
import ScrapePoolList from './ScrapePoolList';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { UncontrolledAlert } from 'reactstrap';
import { scrapePoolListWarnings } from './ScrapePoolList';

const Targets: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  return (
    <>
      {
        <>
          {
            <div>
              {scrapePoolListWarnings.map((warning, index) => (
                <UncontrolledAlert key={index} color="warning">
                  {warning}
                </UncontrolledAlert>
              ))}
            </div>
          }
          <h2>Targets</h2>
          <ScrapePoolList pathPrefix={pathPrefix} />
        </>
      }
    </>
  );
};

export default Targets;
