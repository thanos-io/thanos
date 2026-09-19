import React, { FC } from 'react';
import { Badge, ListGroup, ListGroupItem } from 'reactstrap';
import { Labels } from './store';

export type StoreLabelsProps = { labelSets: Labels[] };

export const StoreLabels: FC<StoreLabelsProps> = ({ labelSets }) => {
  return (
    <ListGroup>
      {labelSets.map((labels, idx) => (
        <ListGroupItem key={idx}>
          {Object.entries(labels).map(([name, value]) => (
            <Badge key={name} color="primary" style={{ margin: '0px 5px' }}>{`${name}="${value}"`}</Badge>
          ))}
        </ListGroupItem>
      ))}
    </ListGroup>
  );
};

export default StoreLabels;
