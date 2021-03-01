import React from 'react';
import { mount } from 'enzyme';
import { Button, Collapse, Table, Badge } from 'reactstrap';
import StorePoolPanel, { StorePoolPanelProps, MAX_TIME } from './StorePoolPanel';
import StoreLabels from './StoreLabels';
import { getColor } from '../../../pages/targets/target';
import { formatTime, parseTime } from '../../../utils';
import { sampleAPIResponse } from './__testdata__/testdata';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

describe('StorePoolPanel', () => {
  const defaultProps: StorePoolPanelProps = {
    title: 'sidecar',
    storePool: sampleAPIResponse.data.sidecar,
  };

  const storePoolPanel = mount(<StorePoolPanel {...defaultProps} />);

  it('renders a container', () => {
    const div = storePoolPanel.find('div').filterWhere((elem) => elem.hasClass('container-fluid'));
    expect(div).toHaveLength(1);
  });

  describe('Header', () => {
    it('renders a span with title', () => {
      const span = storePoolPanel.find('h3 > span');
      expect(span).toHaveLength(1);
      expect(span.text()).toEqual('sidecar');
    });

    it('collapses the table when clicked on show less button', () => {
      const btn = storePoolPanel.find(Button);
      expect(btn).toHaveLength(1);
      btn.simulate('click');

      const collapse = storePoolPanel.find(Collapse);
      expect(collapse.prop('isOpen')).toBe(false);
    });

    it('expands the table again after clicking show more button', () => {
      const btn = storePoolPanel.find(Button);
      expect(btn).toHaveLength(1);
      btn.simulate('click');

      const collapse = storePoolPanel.find(Collapse);
      expect(collapse.prop('isOpen')).toBe(true);
    });
  });

  it('renders an open Collapse component by default', () => {
    const collapse = storePoolPanel.find(Collapse);
    expect(collapse.prop('isOpen')).toBe(true);
  });

  describe('for each store', () => {
    const table = storePoolPanel.find(Table);
    defaultProps.storePool.forEach((store, idx) => {
      const { name, minTime, maxTime, labelSets, lastCheck, lastError } = store;
      const row = table.find('tr').at(idx + 1);

      it('renders store endpoint', () => {
        const td = row.find({ 'data-testid': 'endpoint' });
        expect(td).toHaveLength(1);
        expect(td.text()).toBe(name);
      });

      it('renders a badge for health', () => {
        const health = lastError ? 'down' : 'up';
        const td = row.find({ 'data-testid': 'health' });
        expect(td).toHaveLength(1);

        const badge = td.find(Badge);
        expect(badge).toHaveLength(1);
        expect(badge.prop('color')).toEqual(getColor(health));
        expect(badge.text()).toEqual(health.toUpperCase());
      });

      it('renders labelSets', () => {
        const td = row.find({ 'data-testid': 'storeLabels' });
        expect(td).toHaveLength(1);

        const storeLabels = td.find(StoreLabels);
        expect(storeLabels).toHaveLength(1);
        expect(storeLabels.prop('labelSets')).toEqual(labelSets);
      });

      it('renders minTime', () => {
        const td = row.find({ 'data-testid': 'minTime' });
        expect(td).toHaveLength(1);

        if (minTime >= MAX_TIME) {
          const infinityIcon = td.find(FontAwesomeIcon);
          expect(infinityIcon).toHaveLength(1);
        } else {
          expect(td.text()).toBe(formatTime(minTime));
        }
      });

      it('renders maxTime', () => {
        const td = row.find({ 'data-testid': 'maxTime' });
        expect(td).toHaveLength(1);

        if (maxTime >= MAX_TIME) {
          const infinityIcon = td.find(FontAwesomeIcon);
          expect(infinityIcon).toHaveLength(1);
        } else {
          expect(td.text()).toBe(formatTime(maxTime));
        }
      });

      it('renders lastCheck', () => {
        const td = row.find({ 'data-testid': 'lastCheck' });
        expect(td).toHaveLength(1);

        if (parseTime(lastCheck) >= MAX_TIME) {
          const infinityIcon = td.find(FontAwesomeIcon);
          expect(infinityIcon).toHaveLength(1);
        }
      });

      it('renders a badge for Errors', () => {
        const td = row.find({ 'data-testid': 'lastError' });
        const badge = td.find(Badge);
        expect(badge).toHaveLength(lastError ? 1 : 0);
        if (lastError) {
          expect(badge.prop('color')).toEqual('danger');
          expect(badge.children().text()).toEqual(lastError);
        }
      });
    });
  });
});
