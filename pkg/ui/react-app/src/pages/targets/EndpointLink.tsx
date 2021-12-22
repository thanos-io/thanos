import React, { FC } from 'react';
import { Badge, UncontrolledAlert } from 'reactstrap';

export interface EndpointLinkProps {
  endpoint: string;
  globalUrl: string;
}

const EndpointLink: FC<EndpointLinkProps> = ({ endpoint, globalUrl }) => {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch (e) {
    return (
      <UncontrolledAlert color="danger">
        <strong>Error:</strong> {(e as Error).message}
      </UncontrolledAlert>
    );
  }

  const { host, pathname, protocol, searchParams }: URL = url;
  const params = Array.from(searchParams.entries());

  return (
    <>
      <a href={globalUrl}>{`${protocol}//${host}${pathname}`}</a>
      {params.length > 0 ? <br /> : null}
      {params.map(([labelName, labelValue]: [string, string]) => {
        return (
          <Badge color="primary" className={`mr-1 ${labelName}`} key={labelName}>
            {`${labelName}="${labelValue}"`}
          </Badge>
        );
      })}
    </>
  );
};

export default EndpointLink;
