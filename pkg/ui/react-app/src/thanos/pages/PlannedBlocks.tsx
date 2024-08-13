import React from 'react';
import { RouteComponentProps } from '@reach/router';
import Blocks from './blocks/Blocks';
import PathPrefixProps from '../../types/PathPrefixProps';

const PlannedBlocks: React.FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  return <Blocks view="planned" pathPrefix={pathPrefix} />;
};

export default PlannedBlocks;
