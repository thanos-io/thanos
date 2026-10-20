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
    const text = queryStatsView.find('.float-right').text();

    expect(queryStatsView.find('.query-stats').prop('className')).toEqual('query-stats');
    expect(queryStatsView.find('.float-right').prop('className')).toEqual('float-right');
    expect(text).toContain(`Load time: ${queryStatsProps.loadTime}ms`);
    expect(text).toContain(`Resolution: ${queryStatsProps.resolution}s`);
    expect(text).toContain(`Result series: ${queryStatsProps.resultSeries}`);
    expect(text).toContain(`Trace ID: ${queryStatsProps.traceID}`);
  });

  it('omits the trace ID when it is not available', () => {
    const queryStatsProps = {
      loadTime: 100,
      resolution: 5,
      resultSeries: 10000,
      traceID: null,
    };
    const queryStatsView = mount(<QueryStatsView {...queryStatsProps} />);
    const text = queryStatsView.find('.float-right').text();

    expect(text).toContain(`Result series: ${queryStatsProps.resultSeries}`);
    expect(text).not.toContain('Trace ID');
  });
});
