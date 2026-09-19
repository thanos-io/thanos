import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';
import { ListGroup, ListGroupItem } from 'reactstrap';
import StoreLabels from './StoreLabels';
import { sampleAPIResponse } from './__testdata__/testdata';

describe('storeLabels', () => {
  const { labelSets } = sampleAPIResponse.data.store[0];
  const storeLabels = shallow(<StoreLabels labelSets={labelSets} />);

  it('renders a listGroup', () => {
    const listGroup = storeLabels.find(ListGroup);
    expect(listGroup).toHaveLength(1);
  });

  it('renders a ListGroupItem for each labelSet', () => {
    const listGroupItems = storeLabels.find(ListGroupItem);
    expect(listGroupItems).toHaveLength(labelSets.length);
  });

  it('renders discovered labels', () => {
    expect(toJson(storeLabels)).toMatchSnapshot();
  });
});
