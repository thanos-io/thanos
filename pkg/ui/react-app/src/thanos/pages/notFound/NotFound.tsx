import React, { FC } from 'react';

interface NotFoundProps {
  default: boolean;
}

const NotFound: FC<NotFoundProps> = () => <h3>404, Page Not Found</h3>;

export default NotFound;
