import React, { FC } from 'react';
import { Badge, ListGroup, ListGroupItem } from 'reactstrap';
import { Labels } from './store';

export type StoreLabelsProps = { labelSets: Labels[] };

export const StoreLabels: FC<StoreLabelsProps> = ({ labelSets }) => {
  return (
    <ListGroup>
      {labelSets.map(({ labels }, idx) => (
        <ListGroupItem key={idx}>
          {labels.map(label => (
            <Badge key={label.name} color="primary" style={{ margin: '0px 5px' }}>{`${label.name}="${label.value}"`}</Badge>
          ))}
        </ListGroupItem>
      ))}
    </ListGroup>
  );
};

export default StoreLabels;
