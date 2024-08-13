import React from 'react';
import { RouteComponentProps } from '@reach/router';
import Blocks from './blocks/Blocks';
import PathPrefixProps from '../../types/PathPrefixProps';

const GlobalBlocks: React.FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
  return <Blocks view="global" pathPrefix={pathPrefix} />;
};

export default GlobalBlocks;

