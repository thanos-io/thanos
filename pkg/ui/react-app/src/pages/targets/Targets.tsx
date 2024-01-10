import React, { FC, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import Filter from './Filter';
import ScrapePoolList from './ScrapePoolList';
import PathPrefixProps from '../../types/PathPrefixProps';
import { useLocalStorage } from '../../hooks/useLocalStorage';
import { UncontrolledAlert } from 'reactstrap';

const Targets: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  const [warnings, setWarnings] = useState<string[]>([]);
  return (
    <>
      {
        <>
          <div>
            {warnings.map((warning, index) => (
              <UncontrolledAlert key={index} color="warning">
                {warning}
              </UncontrolledAlert>
            ))}
          </div>
          <h2>Targets</h2>
          <ScrapePoolList pathPrefix={pathPrefix} setWarnings={setWarnings} />
        </>
      }
    </>
  );
};

export default Targets;
