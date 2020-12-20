import React, { FC } from 'react';
import PathPrefixProps from '../../../types/PathPrefixProps';

interface NotFoundProps {
  default: boolean;
  defaultRoute: string;
}

const NotFound: FC<NotFoundProps & PathPrefixProps> = ({ pathPrefix, defaultRoute }) => {
  return (
    <div
      style={{
        position: 'absolute',
        left: '50%',
        top: '50%',
        transform: 'translate(-50%, -50%)',
      }}
    >
      <h1>404, Page Not Found!</h1>
      <h5>
        <a href={`${pathPrefix}/new${defaultRoute}`}>Go back to home page</a>
      </h5>
    </div>
  );
};

export default NotFound;
