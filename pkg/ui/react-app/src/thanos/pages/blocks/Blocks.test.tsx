import React from 'react';
import { mount, ReactWrapper } from 'enzyme';
import { FetchMock } from 'jest-fetch-mock/types';
import { UncontrolledAlert } from 'reactstrap';
import Blocks from './Blocks';
import { QueryParamProvider } from 'use-query-params';
import { SourceView } from './SourceView';
import TimeInput from '../../../pages/graph/TimeInput';
import { sampleAPIResponse } from './__testdata__/testdata';
import { act } from 'react-dom/test-utils';

describe('Blocks', () => {
  beforeEach(() => {
    fetchMock.resetMocks();
  });

  describe('when data is returned', () => {
    let blocks: ReactWrapper;
    let mock: FetchMock;

    beforeEach(() => {
      mock = fetchMock.mockResponse(JSON.stringify(sampleAPIResponse));
    });

    it('renders sources', async () => {
      await act(async () => {
        blocks = mount(
          <QueryParamProvider>
            <Blocks />
          </QueryParamProvider>
        );
      });
      blocks.update();
      expect(mock).toHaveBeenCalledWith('/api/v1/blocks?view=global', { cache: 'no-store', credentials: 'same-origin' });

      const sourceViews = blocks.find(SourceView);
      expect(sourceViews).toHaveLength(8);
    });

    it('starts with an empty end time and remains empty after clearing a selected time', async () => {
      await act(async () => {
        blocks = mount(
          <QueryParamProvider>
            <Blocks />
          </QueryParamProvider>
        );
      });
      blocks.update();

      expect(blocks.find(TimeInput).prop('time')).toBeNull();

      const explicitEndTime = Math.max(...sampleAPIResponse.data.blocks.map((block) => block.maxTime));
      act(() => {
        blocks.find(TimeInput).prop('onChangeTime')(explicitEndTime);
      });
      blocks.update();
      expect(blocks.find(TimeInput).prop('time')).toEqual(explicitEndTime);

      act(() => {
        blocks.find(TimeInput).prop('onChangeTime')(null);
      });
      blocks.update();
      expect(blocks.find(TimeInput).prop('time')).toBeNull();
    });

    it('fetched data with different view', async () => {
      await act(async () => {
        blocks = mount(
          <QueryParamProvider>
            <Blocks view="loaded" />
          </QueryParamProvider>
        );
      });
      blocks.update();
      expect(mock).toHaveBeenCalledWith('/api/v1/blocks?view=loaded', { cache: 'no-store', credentials: 'same-origin' });

      const sourceViews = blocks.find(SourceView);
      expect(sourceViews).toHaveLength(8);
    });
  });

  describe('when there are no blocks', () => {
    it('displays a warning alert', async () => {
      const mock = fetchMock.mockResponse(
        JSON.stringify({
          status: 'success',
          data: {
            blocks: [],
          },
        })
      );

      let blocks: any;
      await act(async () => {
        blocks = mount(
          <QueryParamProvider>
            <Blocks />
          </QueryParamProvider>
        );
      });
      blocks.update();

      expect(mock).toHaveBeenCalledWith('/api/v1/blocks?view=global', { cache: 'no-store', credentials: 'same-origin' });

      const alert = blocks.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('warning');
      expect(alert.text()).toContain('No blocks found.');
    });
  });

  describe('when an error is returned', () => {
    it('displays an error alert', async () => {
      const mock = fetchMock.mockReject(new Error('Error fetching blocks'));

      let blocks: any;
      await act(async () => {
        blocks = mount(
          <QueryParamProvider>
            <Blocks />
          </QueryParamProvider>
        );
      });
      blocks.update();

      expect(mock).toHaveBeenCalledWith('/api/v1/blocks?view=global', { cache: 'no-store', credentials: 'same-origin' });

      const alert = blocks.find(UncontrolledAlert);
      expect(alert.prop('color')).toBe('danger');
      expect(alert.text()).toContain('Error fetching blocks');
    });
  });
});
