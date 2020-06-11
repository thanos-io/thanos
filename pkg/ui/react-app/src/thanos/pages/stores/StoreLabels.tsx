import React, { FC } from 'react';
import { Badge, ListGroup, ListGroupItem } from 'reactstrap';
import { Labels } from './store';

export type StoreLabelsProps = { labelSet: Labels[] };
export const StoreLabels: FC<StoreLabelsProps> = ({ labelSet }) => {
  console.log({ labelSet });
  return (
    <ListGroup>
      {labelSet.map(({ labels }) => (
        <ListGroupItem>
          {labels.map(label => (
            <Badge color="primary" style={{ margin: '0px 5px' }}>{`${label.name}="${label.value}"`}</Badge>
          ))}
        </ListGroupItem>
      ))}
    </ListGroup>
  );
};

export default StoreLabels;
