import * as React from 'react';
import { mount } from 'enzyme';
import QueryStatsView from './QueryStatsView';

describe('QueryStatsView', () => {
  it('renders props as query stats', () => {
    const queryStatsProps = {
      loadTime: 100,
      resolution: 5,
      resultSeries: 10000,
      traceID: 'e575f9d4eab63a90cdc3dc4ef1b8dda0',
    };
    const queryStatsView = mount(<QueryStatsView {...queryStatsProps} />);
    expect(queryStatsView.find('.query-stats').prop('className')).toEqual('query-stats');
    expect(queryStatsView.find('.float-right').prop('className')).toEqual('float-right');
    expect(queryStatsView.find('.float-right').html()).toEqual(
      `<span class="float-right">Load time: ${queryStatsProps.loadTime}ms   Resolution: ${queryStatsProps.resolution}s   Result series: ${queryStatsProps.resultSeries}   Trace ID: ${queryStatsProps.traceID}</span>`
    );
  });
});
