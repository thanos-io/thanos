import * as React from 'react';
import { mount, shallow } from 'enzyme';
import { act } from 'react-dom/test-utils';
import Panel, { getQueryResolution, PanelOptions, PanelProps, PanelType } from './Panel';
import GraphControls from './GraphControls';
import { NavLink, TabPane } from 'reactstrap';
import TimeInput from './TimeInput';
import DataTable from './DataTable';
import { GraphTabContent } from './GraphTabContent';
import ExpressionInput from './ExpressionInput';

const defaultProps: PanelProps = {
  id: 'abc123',
  useLocalTime: false,
  options: {
    expr: 'prometheus_engine',
    type: PanelType.Table,
    range: 10,
    endTime: 1572100217898,
    resolution: 28,
    stacked: false,
    maxSourceResolution: 'auto',
    useDeduplication: true,
    forceTracing: false,
    usePartialResponse: false,
    storeMatches: [],
    engine: 'prometheus',
    analyze: false,
    disableAnalyzeCheckbox: false,
    tenant: 'default-tenant',
  },
  onUsePartialResponseChange: (): void => {
    // Do nothing.
  },
  onOptionsChanged: (): void => {
    // Do nothing.
  },
  pastQueries: [],
  metricNames: [
    'prometheus_engine_queries',
    'prometheus_engine_queries_concurrent_max',
    'prometheus_engine_query_duration_seconds',
  ],
  removePanel: (): void => {
    // Do nothing.
  },
  onExecuteQuery: (): void => {
    // Do nothing.
  },
  stores: [],
  enableAutocomplete: true,
  defaultStep: '1s',
  enableHighlighting: true,
  enableLinter: true,
  defaultEngine: 'prometheus',
  queryMode: 'local',
  usePartialResponse: true,
};

const querySuccessResponse = JSON.stringify({
  status: 'success',
  data: {
    resultType: 'matrix',
    result: [],
  },
});

describe('Panel', () => {
  const panel = shallow(<Panel {...defaultProps} />);

  beforeEach(() => {
    fetchMock.resetMocks();
    fetchMock.mockResponse(querySuccessResponse);
  });

  describe('getQueryResolution', () => {
    it('keeps explicit resolution unchanged', () => {
      expect(getQueryResolution({ range: 60 * 60 * 1000, resolution: 28 }, '1s')).toEqual(28);
    });

    it('rounds the calculated default resolution up to a common query step', () => {
      expect(getQueryResolution({ range: 60 * 60 * 1000, resolution: null }, '1s')).toEqual(15);
    });

    it('rounds the configured default step up before using it as the minimum', () => {
      expect(getQueryResolution({ range: 60 * 1000, resolution: null }, '13s')).toEqual(15);
    });
  });

  it('uses the rounded default resolution in graph query requests', async () => {
    const graphPanel = shallow(
      <Panel
        {...defaultProps}
        pathPrefix=""
        options={{ ...defaultProps.options, type: PanelType.Graph, range: 60 * 60 * 1000, resolution: null }}
      />
    );
    const executeQuery = graphPanel.find(ExpressionInput).prop('executeQuery');
    if (executeQuery === undefined) {
      throw new Error('executeQuery prop is missing');
    }
    fetchMock.mockClear();

    await act(async () => {
      executeQuery();
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = fetchMock.mock.calls[0][1]?.body;
    expect(body instanceof URLSearchParams).toBe(true);
    if (!(body instanceof URLSearchParams)) {
      throw new Error('query request body is not URLSearchParams');
    }
    expect(body.get('step')).toEqual('15');
  });

  it('renders NavLinks', () => {
    const results: PanelOptions[] = [];
    const onOptionsChanged = (opts: PanelOptions): void => {
      results.push(opts);
    };
    const panel = shallow(<Panel {...defaultProps} onOptionsChanged={onOptionsChanged} />);
    // Panel construction updates Explain checkbox prop to disbale.
    // Hence, a result is added and dropping it.
    results.length = 0;
    const links = panel.find(NavLink);
    [
      { panelType: 'Table', active: true },
      { panelType: 'Graph', active: false },
    ].forEach((tc: { panelType: string; active: boolean }, i: number) => {
      const link = links.at(i);
      const className = tc.active ? 'active' : '';
      expect(link.prop('className')).toEqual(className);
      link.simulate('click');
      expect(results).toHaveLength(1);
      expect(results[0].type).toEqual(tc.panelType.toLowerCase());
      results.pop();
    });
  });

  it('renders a TabPane with a TimeInput and a DataTable when in table mode', () => {
    const tab = panel.find(TabPane).filterWhere((tab) => tab.prop('tabId') === 'table');
    const timeInput = tab.find(TimeInput);
    expect(timeInput.prop('time')).toEqual(defaultProps.options.endTime);
    expect(timeInput.prop('range')).toEqual(defaultProps.options.range);
    expect(timeInput.prop('placeholder')).toEqual('Evaluation time');
    expect(tab.find(DataTable)).toHaveLength(1);
  });

  it('renders a TabPane with a Graph and GraphControls when in graph mode', () => {
    const options = {
      expr: 'prometheus_engine',
      type: PanelType.Graph,
      range: 10,
      endTime: 1572100217898,
      resolution: 28,
      stacked: false,
      maxSourceResolution: 'auto',
      useDeduplication: true,
      forceTracing: false,
      usePartialResponse: false,
      storeMatches: [],
      engine: 'prometheus',
      analyze: false,
      disableAnalyzeCheckbox: false,
      tenant: 'default-tenant',
    };
    const graphPanel = mount(<Panel {...defaultProps} options={options} />);
    const controls = graphPanel.find(GraphControls);
    graphPanel.setState({ data: { resultType: 'matrix', result: [] } });
    const graph = graphPanel.find(GraphTabContent);
    expect(controls.prop('endTime')).toEqual(options.endTime);
    expect(controls.prop('range')).toEqual(options.range);
    expect(controls.prop('resolution')).toEqual(options.resolution);
    expect(controls.prop('stacked')).toEqual(options.stacked);
    expect(graph.prop('stacked')).toEqual(options.stacked);
    expect(controls.prop('maxSourceResolution')).toEqual(options.maxSourceResolution);
  });

  describe('when switching between modes', () => {
    [
      { from: PanelType.Table, to: PanelType.Graph },
      { from: PanelType.Graph, to: PanelType.Table },
    ].forEach(({ from, to }: { from: PanelType; to: PanelType }) => {
      it(`${from} -> ${to} nulls out data`, () => {
        const props = {
          ...defaultProps,
          options: { ...defaultProps.options, type: from },
        };
        const panel = shallow(<Panel {...props} />);
        const instance: any = panel.instance();
        panel.setState({ data: 'somedata' });
        expect(panel.state('data')).toEqual('somedata');
        instance.handleChangeType(to);
        expect(panel.state('data')).toBeNull();
      });
    });
  });

  describe('when changing query then time', () => {
    it('executes the new query', () => {
      const initialExpr = 'time()';
      const newExpr = 'time() - time()';
      const panel = shallow(<Panel {...defaultProps} options={{ ...defaultProps.options, expr: initialExpr }} />);
      const instance: any = panel.instance();
      instance.executeQuery();
      const executeQuerySpy = jest.spyOn(instance, 'executeQuery');
      //change query without executing
      panel.setProps({ options: { ...defaultProps.options, expr: newExpr } });
      expect(executeQuerySpy).toHaveBeenCalledTimes(0);
      //execute query implicitly with time change
      panel.setProps({ options: { ...defaultProps.options, expr: newExpr, endTime: 1575744840 } });
      expect(executeQuerySpy).toHaveBeenCalledTimes(1);
    });
  });
});
