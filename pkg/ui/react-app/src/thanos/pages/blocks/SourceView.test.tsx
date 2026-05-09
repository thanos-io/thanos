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

  it('renders a paragraph with title, size, and labels', () => {
    const spans = sourceView.find('div > span');
    expect(spans.length).toBeGreaterThanOrEqual(2);

    expect(spans.at(0).text()).toEqual(source);
    expect(spans.at(1).text()).toEqual('3.50 GiB');
    // Labels should be visible on each source row
    expect(sourceView.text()).toContain('monitor');
  });

  it('renders a row for each unique resolution and compaction level pair', () => {
    const rows = sourceView.find(BlocksRow);
    expect(rows).toHaveLength(Object.keys(sorted[source]).length);
  });
});
