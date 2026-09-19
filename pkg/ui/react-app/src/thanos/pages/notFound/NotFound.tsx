import React, { FC } from 'react';
import { Container } from 'reactstrap';
import PathPrefixProps from '../../../types/PathPrefixProps';
import styles from './NotFound.module.css';

interface NotFoundProps {
  default: boolean;
  defaultRoute: string;
}

const NotFound: FC<NotFoundProps & PathPrefixProps> = ({ pathPrefix, defaultRoute }) => {
  return (
    <Container className={styles.container}>
      <h1>404, Page not Found!</h1>
      <h5>
        <a href={`${pathPrefix}${defaultRoute}`}>Go back to home page</a>
      </h5>
    </Container>
  );
};

export default NotFound;
