import React from 'react';
import { mount } from 'enzyme';
import { BlocksRow, SourceView, SourceViewProps } from './SourceView';
import { sampleAPIResponse } from './__testdata__/testdata';
import { sortBlocks } from './helpers';

const sorted = sortBlocks(sampleAPIResponse.data.blocks, sampleAPIResponse.data.label, false);
const source = 'prometheus_one';

describe('Blocks SourceView', () => {
  const defaultProps: SourceViewProps = {
    title: source,
    data: sorted[source],
    selectBlock: (): void => {
      // do nothing
    },
    gridMinTime: 1596096000000,
    gridMaxTime: 1595108031471,
    blockSearch: '',
    compactionLevel: 0,
  };

  const sourceView = mount(<SourceView {...defaultProps} />);

  it('renders a paragraph with title and size', () => {
    const title = sourceView.find('div > span');
    expect(title).toHaveLength(2);

    expect(title.find('span').at(0).text()).toEqual(source);
    expect(title.find('span').at(1).text()).toEqual('3.50 GiB');
  });

  it('renders a row for each unique resolution and compaction level pair', () => {
    const rows = sourceView.find(BlocksRow);
    expect(rows).toHaveLength(Object.keys(sorted[source]).length);
  });
});
