import React from 'react';
import { mount } from 'enzyme';
import moment from 'moment';
import { BlockDetails, BlockDetailsProps } from './BlockDetails';
import { sampleAPIResponse } from './__testdata__/testdata';

const sampleBlock = sampleAPIResponse.data.blocks[0];
const formatTime = (time: number): string => {
  return moment.unix(time / 1000).format('LLL');
};

describe('BlockDetails', () => {
  const defaultProps: BlockDetailsProps = {
    block: sampleBlock,
    selectBlock: (): void => {
      // do nothing
    },
    disableAdminOperations: false,
  };
  window.URL.createObjectURL = jest.fn();
  const blockDetails = mount(<BlockDetails {...defaultProps} />);

  it('renders a heading with block ulid', () => {
    const title = blockDetails.find({ 'data-testid': 'ulid' });
    expect(title).toHaveLength(1);
    expect(title.text()).toEqual(sampleBlock.ulid);
  });

  it('renders start time of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'start-time' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(formatTime(sampleBlock.minTime));
  });

  it('renders end time of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'end-time' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(formatTime(sampleBlock.maxTime));
  });

  it('renders duration of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'duration' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(moment.duration(sampleBlock.maxTime - sampleBlock.minTime, 'ms').humanize());
  });

  it('renders total number of series in the block', () => {
    const div = blockDetails.find({ 'data-testid': 'series' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.stats.numSeries.toString());
  });

  it('renders total number of samples in the block', () => {
    const div = blockDetails.find({ 'data-testid': 'samples' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.stats.numSamples.toString());
  });

  it('renders total number of chunks in the block', () => {
    const div = blockDetails.find({ 'data-testid': 'chunks' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.stats.numChunks.toString());
  });

  it('renders downsampling resolution of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'resolution' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.thanos.downsample.resolution.toString());
  });

  it('renders compaction level of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'level' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.compaction.level.toString());
  });

  it('renders source of the block', () => {
    const div = blockDetails.find({ 'data-testid': 'source' });
    expect(div).toHaveLength(1);
    expect(div.find('span').text()).toBe(sampleBlock.thanos.source);
  });

  it('renders the download button', () => {
    const div = blockDetails.find({ 'data-testid': 'download' });
    window.URL.createObjectURL = jest.fn(() => 'details');
    expect(div).toHaveLength(1);
    expect(div.find('a').text()).toBe('Download meta.json');
  });

  it('renders a list of the labels', () => {
    const div = blockDetails.find({ 'data-testid': 'labels' });
    const list = div.find('ul');
    expect(div).toHaveLength(1);
    expect(list).toHaveLength(1);

    const labels = list.find('li');
    expect(labels).toHaveLength(Object.keys(sampleBlock.thanos.labels).length);
  });
});
