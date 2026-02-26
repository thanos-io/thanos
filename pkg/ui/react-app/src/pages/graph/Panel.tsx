import React, { Component } from 'react';

import {
  UncontrolledAlert,
  Alert,
  Button,
  Col,
  Nav,
  NavItem,
  NavLink,
  Row,
  TabContent,
  TabPane,
  Input,
  Label,
} from 'reactstrap';
import Select from 'react-select';

import moment from 'moment-timezone';
import Checkbox from '../../components/Checkbox';
import ListTree, { QueryTree } from '../../components/ListTree';
import { ExplainTree } from './ExpressionInput';
import ExpressionInput from './ExpressionInput';
import GraphControls from './GraphControls';
import { GraphTabContent } from './GraphTabContent';
import DataTable from './DataTable';
import TimeInput from './TimeInput';
import QueryStatsView, { QueryStats } from './QueryStatsView';
import { Store } from '../../thanos/pages/stores/store';
import PathPrefixProps from '../../types/PathPrefixProps';
import { QueryParams } from '../../types/types';
import { parseDuration } from '../../utils';
import { defaultTenant, tenantHeader, displayTenantBox } from '../../thanos/config';

export interface PanelProps {
  id: string;
  options: PanelOptions;
  onOptionsChanged: (opts: PanelOptions) => void;
  useLocalTime: boolean;
  pastQueries: string[];
  metricNames: string[];
  removePanel: () => void;
  onExecuteQuery: (query: string) => void;
  stores: Store[];
  enableAutocomplete: boolean;
  enableHighlighting: boolean;
  usePartialResponse: boolean;
  enableLinter: boolean;
  defaultStep: string;
  defaultEngine: string;
  queryMode: string;
  onUsePartialResponseChange: (value: boolean) => void;
}

interface PanelState {
  data: any; // TODO: Type data.
  lastQueryParams: QueryParams | null;
  loading: boolean;
  error: string | null;
  warnings: string[] | null;
  stats: QueryStats | null;
  exprInputValue: string;
  analysis: QueryTree | null;
  explainOutput: ExplainTree | null;
  isHovered: boolean;
}

export interface PanelOptions {
  expr: string;
  type: PanelType;
  range: number; // Range in milliseconds.
  endTime: number | null; // Timestamp in milliseconds.
  resolution: number | null; // Resolution in seconds.
  stacked: boolean;
  maxSourceResolution: string;
  useDeduplication: boolean;
  forceTracing: boolean;
  usePartialResponse: boolean;
  storeMatches: Store[];
  engine: string;
  analyze: boolean;
  disableAnalyzeCheckbox: boolean;
  tenant: string;
}

export enum PanelType {
  Graph = 'graph',
  Table = 'table',
}

export const PanelDefaultOptions: PanelOptions = {
  type: PanelType.Table,
  expr: '',
  range: 60 * 60 * 1000,
  endTime: null,
  resolution: null,
  stacked: false,
  maxSourceResolution: '0s',
  useDeduplication: true,
  forceTracing: false,
  usePartialResponse: true,
  storeMatches: [],
  engine: '',
  analyze: false,
  disableAnalyzeCheckbox: false,
  tenant: '',
};

class Panel extends Component<PanelProps & PathPrefixProps, PanelState> {
  private abortInFlightFetch: (() => void) | null = null;

  constructor(props: PanelProps) {
    super(props);

    this.state = {
      data: null,
      lastQueryParams: null,
      loading: false,
      warnings: null,
      error: null,
      stats: null,
      exprInputValue: props.options.expr,
      explainOutput: null,
      analysis: null,
      isHovered: false,
    };

    if (this.props.options.engine === '') {
      this.props.options.engine = this.props.defaultEngine;
    }
    this.handleEngine(this.props.options.engine);

    this.handleChangeDeduplication = this.handleChangeDeduplication.bind(this);
    this.handleChangeForceTracing = this.handleChangeForceTracing.bind(this); //forceTracing
    this.handleChangePartialResponse = this.handleChangePartialResponse.bind(this);
    this.handleStoreMatchChange = this.handleStoreMatchChange.bind(this);
    this.handleChangeEngine = this.handleChangeEngine.bind(this);
    this.handleChangeAnalyze = this.handleChangeAnalyze.bind(this);
    this.handleMouseEnter = this.handleMouseEnter.bind(this);
    this.handleMouseLeave = this.handleMouseLeave.bind(this);
    this.handleChangeTenant = this.handleChangeTenant.bind(this);
  }
  componentDidUpdate({ options: prevOpts }: PanelProps): void {
    const {
      endTime,
      range,
      resolution,
      type,
      maxSourceResolution,
      useDeduplication,
      forceTracing,
      usePartialResponse,
      engine,
      analyze,
      tenant,
      // TODO: Add support for Store Matches
    } = this.props.options;
    if (
      prevOpts.endTime !== endTime ||
      prevOpts.range !== range ||
      prevOpts.resolution !== resolution ||
      prevOpts.type !== type ||
      prevOpts.maxSourceResolution !== maxSourceResolution ||
      prevOpts.useDeduplication !== useDeduplication ||
      prevOpts.usePartialResponse !== usePartialResponse ||
      prevOpts.forceTracing !== forceTracing ||
      prevOpts.engine !== engine ||
      prevOpts.analyze !== analyze ||
      prevOpts.tenant !== tenant
      // Check store matches
    ) {
      this.executeQuery();
    }
  }

  componentDidMount(): void {
    this.executeQuery();
    const storedValue = localStorage.getItem('usePartialResponse');
    if (storedValue !== null) {
      // Set the default value in state and local storage
      this.setOptions({ usePartialResponse: true });
      this.props.onUsePartialResponseChange(true);
      localStorage.setItem('usePartialResponse', JSON.stringify(true));
    }
  }

  executeQuery = (): void => {
    const { exprInputValue: expr } = this.state;
    const queryStart = Date.now();
    this.props.onExecuteQuery(expr);
    if (this.props.options.expr !== expr) {
      this.setOptions({ expr });
    }
    if (expr === '') {
      return;
    }

    if (this.abortInFlightFetch) {
      this.abortInFlightFetch();
      this.abortInFlightFetch = null;
    }

    const abortController = new AbortController();
    this.abortInFlightFetch = () => abortController.abort();
    this.setState({ loading: true });

    const endTime = this.getEndTime().valueOf() / 1000; // TODO: shouldn't valueof only work when it's a moment?
    const startTime = endTime - this.props.options.range / 1000;
    const resolution =
      this.props.options.resolution ||
      Math.max(Math.floor(this.props.options.range / 250000), (parseDuration(this.props.defaultStep) || 0) / 1000);
    const params: URLSearchParams = new URLSearchParams({
      query: expr,
      dedup: this.props.options.useDeduplication.toString(),
      partialResponse: this.props.options.usePartialResponse.toString(),
    });

    // Add storeMatches to query params.
    // eslint-disable-next-line @typescript-eslint/no-unused-expressions
    this.props.options.storeMatches?.forEach((store: Store) =>
      params.append('storeMatch[]', `{__address__="${store.name}"}`)
    );

    let path: string;
    switch (this.props.options.type) {
      case 'graph':
        path = '/api/v1/query_range';
        params.append('start', startTime.toString());
        params.append('end', endTime.toString());
        params.append('step', resolution.toString());
        params.append('max_source_resolution', this.props.options.maxSourceResolution);
        params.append('engine', this.props.options.engine);
        params.append('analyze', this.props.options.analyze.toString());
        params.append('tenant', this.props.options.tenant);
        // TODO path prefix here and elsewhere.
        break;
      case 'table':
        path = '/api/v1/query';
        params.append('time', endTime.toString());
        params.append('engine', this.props.options.engine);
        params.append('analyze', this.props.options.analyze.toString());
        params.append('tenant', this.props.options.tenant);
        break;
      default:
        throw new Error('Invalid panel type "' + this.props.options.type + '"');
    }

    // Create request headers
    const requestHeaders: HeadersInit = new Headers();
    requestHeaders.set('Content-Type', 'application/x-www-form-urlencoded');

    if (this.props.options.forceTracing) {
      requestHeaders.set('X-Thanos-Force-Tracing', 'true');
    }

    if (this.props.options.tenant.length > 0) {
      requestHeaders.set(tenantHeader, this.props.options.tenant);
    }

    fetch(`${this.props.pathPrefix}${path}`, {
      method: 'POST',
      headers: requestHeaders,
      body: params,
      cache: 'no-store',
      credentials: 'same-origin',
      signal: abortController.signal,
    })
      .then((resp) => {
        return resp.json().then((json) => {
          return {
            json,
            headers: resp.headers,
          };
        });
      })
      .then(({ json, headers }) => {
        if (json.status !== 'success') {
          throw new Error(json.error || 'invalid response JSON');
        }
        let resultSeries = 0;
        let analysis = null;
        if (json.data) {
          const { resultType, result } = json.data;
          if (resultType === 'scalar') {
            resultSeries = 1;
          } else if (result && result.length > 0) {
            resultSeries = result.length;
          }
          analysis = json.data.analysis;
        }
        const traceID = headers.get('X-Thanos-Trace-ID');
        this.setState({
          error: null,
          data: json.data,
          lastQueryParams: {
            startTime,
            endTime,
            resolution,
            traceID: traceID ? traceID : '',
          },
          warnings: json.warnings,
          stats: {
            loadTime: Date.now() - queryStart,
            resolution,
            resultSeries,
            traceID,
          },
          loading: false,
          analysis: analysis,
          explainOutput: null,
        });
        this.abortInFlightFetch = null;
      })
      .catch((error) => {
        if (error.name === 'AbortError') {
          // Aborts are expected, don't show an error for them.
          return;
        }
        this.setState({
          error: 'Error executing query: ' + error.message,
          loading: false,
        });
      });
  };

  setOptions(opts: any): void {
    const newOpts = { ...this.props.options, ...opts };
    this.props.onOptionsChanged(newOpts);
  }

  handleExpressionChange = (expr: string): void => {
    this.setState({ exprInputValue: expr });
  };

  handleChangeRange = (range: number): void => {
    this.setOptions({ range: range });
  };

  getEndTime = (): number | moment.Moment => {
    if (this.props.options.endTime === null) {
      return moment();
    }
    return this.props.options.endTime;
  };

  handleChangeEndTime = (endTime: number | null): void => {
    this.setOptions({ endTime: endTime });
  };

  handleChangeResolution = (resolution: number | null): void => {
    this.setOptions({ resolution: resolution });
  };

  handleChangeMaxSourceResolution = (maxSourceResolution: string): void => {
    this.setOptions({ maxSourceResolution });
  };

  handleChangeType = (type: PanelType): void => {
    this.setState({ data: null });
    this.setOptions({ type: type });
  };

  handleChangeStacking = (stacked: boolean): void => {
    this.setOptions({ stacked: stacked });
  };

  handleChangeDeduplication = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.setOptions({ useDeduplication: event.target.checked });
  };
  handleChangeForceTracing = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.setOptions({ forceTracing: event.target.checked });
  };

  handleChangePartialResponse = (event: React.ChangeEvent<HTMLInputElement>): void => {
    let newValue = event.target.checked;

    const storedValue = localStorage.getItem('usePartialResponse');

    if (storedValue === 'true') {
      newValue = true;
    }
    this.setOptions({ usePartialResponse: newValue });
    this.props.onUsePartialResponseChange(newValue);

    localStorage.setItem('usePartialResponse', JSON.stringify(event.target.checked));
  };

  handleStoreMatchChange = (selectedStores: any): void => {
    this.setOptions({ storeMatches: selectedStores || [] });
  };

  handleToggleAlert = (): void => {
    this.setState({ error: null });
  };

  handleToggleWarn = (): void => {
    this.setState({ warnings: null });
  };

  handleChangeEngine = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.handleEngine(event.target.value);
  };

  handleChangeAnalyze = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.setOptions({ analyze: event.target.checked });
  };

  handleChangeTenant = (event: React.ChangeEvent<HTMLInputElement>): void => {
    this.setOptions({ tenant: event.target.value });
  };

  handleMouseEnter = () => {
    this.setState({ isHovered: true });
  };

  handleMouseLeave = () => {
    this.setState({ isHovered: false });
  };

  handleEngine = (engine: string): void => {
    if (engine === 'prometheus') {
      this.setOptions({ engine: engine, analyze: false, disableAnalyzeCheckbox: true });
    } else {
      this.setOptions({ engine: engine, disableAnalyzeCheckbox: false });
    }
  };

  handleTimeRangeSelection = (startTime: number, endTime: number) => {
    this.setOptions({ range: endTime - startTime, endTime: endTime });
  };

  getExplainOutput = (): void => {
    //We need to pass the same parameters as query endpoints, to the explain endpoints.
    const endTime = this.getEndTime().valueOf() / 1000;
    const startTime = endTime - this.props.options.range / 1000;
    const resolution =
      this.props.options.resolution ||
      Math.max(Math.floor(this.props.options.range / 250000), (parseDuration(this.props.defaultStep) || 0) / 1000);
    const abortController = new AbortController();
    const params: URLSearchParams = new URLSearchParams({
      query: this.state.exprInputValue,
      dedup: this.props.options.useDeduplication.toString(),
      partialResponse: this.props.options.usePartialResponse.toString(),
    });

    // Add storeMatches to query params.
    // eslint-disable-next-line @typescript-eslint/no-unused-expressions
    this.props.options.storeMatches?.forEach((store: Store) =>
      params.append('storeMatch[]', `{__address__="${store.name}"}`)
    );

    let path: string;
    switch (this.props.options.type) {
      case 'graph':
        path = '/api/v1/query_range_explain';
        params.append('start', startTime.toString());
        params.append('end', endTime.toString());
        params.append('step', resolution.toString());
        params.append('max_source_resolution', this.props.options.maxSourceResolution);
        params.append('engine', this.props.options.engine);
        params.append('analyze', this.props.options.analyze.toString());
        // TODO path prefix here and elsewhere.
        break;
      case 'table':
        path = '/api/v1/query_explain';
        params.append('time', endTime.toString());
        params.append('engine', this.props.options.engine);
        params.append('analyze', this.props.options.analyze.toString());
        break;
      default:
        throw new Error('Invalid panel type "' + this.props.options.type + '"');
    }

    fetch(`${this.props.pathPrefix}${path}?${params}`, {
      cache: 'no-store',
      credentials: 'same-origin',
      signal: abortController.signal,
    })
      .then((resp) => resp.json())
      .then((json) => {
        if (json.status !== 'success') {
          throw new Error(json.error || 'invalid response JSON');
        }
        let result = null;
        if (json.data) {
          result = json.data;
        }
        this.setState({ explainOutput: result });
      })
      .catch((error) => {
        if (error.name === 'AbortError') {
          // Aborts are expected, don't show an error for them.
          return;
        }
        this.setState({
          error: 'Error getting query explaination: ' + error.message,
          loading: false,
        });
      });
  };

  render(): JSX.Element {
    const { pastQueries, metricNames, options, id, stores } = this.props;

    return (
      <div className="panel">
        <Row>
          <Col>
            <ExpressionInput
              pathPrefix={this.props.pathPrefix}
              value={this.state.exprInputValue}
              onExpressionChange={this.handleExpressionChange}
              executeQuery={this.executeQuery}
              loading={this.state.loading}
              enableAutocomplete={this.props.enableAutocomplete}
              enableHighlighting={this.props.enableHighlighting}
              enableLinter={this.props.enableLinter}
              queryHistory={pastQueries}
              metricNames={metricNames}
              executeExplain={this.getExplainOutput}
              disableExplain={this.props.options.engine === 'prometheus'}
            />
          </Col>
        </Row>
        <Row>
          <Col>
            <UncontrolledAlert isOpen={!!this.state.error} toggle={this.handleToggleAlert} color="danger">
              {this.state.error}
            </UncontrolledAlert>
          </Col>
        </Row>
        <Row>
          <Col>
            <UncontrolledAlert
              isOpen={this.state.warnings || false}
              toggle={this.handleToggleWarn}
              color="info"
              style={{ whiteSpace: 'break-spaces' }}
            >
              {this.state.warnings}
            </UncontrolledAlert>
          </Col>
        </Row>
        <Row>
          <Col>
            <div className="float-left">
              <Checkbox
                disabled={this.props.queryMode !== 'local' && this.props.options.engine !== 'prometheus'}
                wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
                id={`use-deduplication-checkbox-${id}`}
                onChange={this.handleChangeDeduplication}
                defaultChecked={options.useDeduplication}
              >
                Use Deduplication
              </Checkbox>
              <Checkbox
                disabled={this.props.queryMode !== 'local' && this.props.options.engine !== 'prometheus'}
                wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
                id={`use-partial-resp-checkbox-${id}`}
                onChange={this.handleChangePartialResponse}
                defaultChecked={options.usePartialResponse}
              >
                Use Partial Response
              </Checkbox>
              <Checkbox
                wrapperStyles={{ marginLeft: 20, display: 'inline-block' }}
                id={`force-tracing-checkbox-${id}`}
                onChange={this.handleChangeForceTracing}
                defaultChecked={options.forceTracing}
              >
                Force Tracing
              </Checkbox>
              <Label
                style={{ marginLeft: '10px', display: 'inline-block' }}
                for={`select-engine=${id}`}
                className="control-label"
              >
                Engine
              </Label>
              <Input
                style={{
                  width: 'auto',
                  marginLeft: '10px',
                  display: 'inline-block',
                }}
                id={`select-engine=${id}`}
                type="select"
                value={options.engine}
                onChange={this.handleChangeEngine}
                bsSize="sm"
              >
                <option value="prometheus">Prometheus</option>
                <option value="thanos">Thanos</option>
              </Input>
              <Label style={{ marginLeft: '10px', display: displayTenantBox }} className="control-label">
                Tenant
              </Label>
              <Input
                style={{
                  width: 'auto',
                  marginLeft: '10px',
                  display: displayTenantBox,
                }}
                id={`tenant=${id}`}
                type="text"
                bsSize="sm"
                onChange={this.handleChangeTenant}
                placeholder={`${defaultTenant}`}
                value={options.tenant}
              ></Input>
            </div>
            <div className="float-right" onMouseEnter={this.handleMouseEnter} onMouseLeave={this.handleMouseLeave}>
              <Checkbox
                wrapperStyles={{ marginRight: 20, display: 'inline-block' }}
                id={`analyze-${id}`}
                onChange={this.handleChangeAnalyze}
                checked={options.analyze}
                disabled={options.disableAnalyzeCheckbox}
                className="analyze-checkbox"
              >
                Analyze
              </Checkbox>
              <div
                style={{
                  position: 'relative',
                }}
              >
                <div
                  style={{
                    display: this.state.isHovered && options.disableAnalyzeCheckbox ? 'block' : 'none',
                    position: 'absolute',
                    top: '-20px',
                    left: '-5px',
                    backgroundColor: '#333',
                    color: '#fff',
                    padding: '2px',
                    borderRadius: '3px',
                    fontSize: '15px',
                    zIndex: 1,
                  }}
                >
                  Change engine to 'thanos'
                </div>
              </div>
            </div>
          </Col>
        </Row>
        <Row hidden={!(options.analyze && this.state.analysis)}>
          <Col>
            <Alert color="info" style={{ overflowX: 'auto', whiteSpace: 'nowrap', width: '100%' }}>
              <ListTree id={`analyze-tree-${id}`} node={this.state.analysis} />
            </Alert>
          </Col>
        </Row>
        <Row hidden={!(this.props.options.engine === 'thanos' && this.state.explainOutput)}>
          <Col>
            <Alert color="info" style={{ overflowX: 'auto', whiteSpace: 'nowrap', width: '100%' }}>
              <ListTree id={`explain-tree-${id}`} node={this.state.explainOutput} />
            </Alert>
          </Col>
        </Row>
        {stores?.length > 0 && (
          <Row>
            <Col>
              <div className="store-filter-wrapper">
                <label className="store-filter-label">Store Filter:</label>
                <Select
                  defaultValue={options.storeMatches}
                  options={stores}
                  isMulti
                  getOptionLabel={(option: Store) => option.name}
                  getOptionValue={(option: Store) => option.name}
                  closeMenuOnSelect={false}
                  styles={{
                    container: (provided, state) => ({
                      ...provided,
                      marginBottom: 20,
                      zIndex: 3,
                      width: '100%',
                      color: '#000',
                    }),
                  }}
                  onChange={this.handleStoreMatchChange}
                />
              </div>
            </Col>
          </Row>
        )}
        <Row>
          <Col>
            <Nav tabs>
              <NavItem>
                <NavLink
                  className={options.type === 'table' ? 'active' : ''}
                  onClick={() => this.handleChangeType(PanelType.Table)}
                >
                  Table
                </NavLink>
              </NavItem>
              <NavItem>
                <NavLink
                  className={options.type === 'graph' ? 'active' : ''}
                  onClick={() => this.handleChangeType(PanelType.Graph)}
                >
                  Graph
                </NavLink>
              </NavItem>
              {!this.state.loading && !this.state.error && this.state.stats && <QueryStatsView {...this.state.stats} />}
            </Nav>
            <TabContent activeTab={options.type}>
              <TabPane tabId="table">
                {options.type === 'table' && (
                  <>
                    <div className="table-controls">
                      <TimeInput
                        time={options.endTime}
                        useLocalTime={this.props.useLocalTime}
                        range={options.range}
                        placeholder="Evaluation time"
                        onChangeTime={this.handleChangeEndTime}
                      />
                    </div>
                    <DataTable data={this.state.data} />
                  </>
                )}
              </TabPane>
              <TabPane tabId="graph">
                {this.props.options.type === 'graph' && (
                  <>
                    <GraphControls
                      range={options.range}
                      endTime={options.endTime}
                      useLocalTime={this.props.useLocalTime}
                      resolution={options.resolution}
                      stacked={options.stacked}
                      maxSourceResolution={options.maxSourceResolution}
                      queryMode={this.props.queryMode}
                      engine={options.engine}
                      onChangeRange={this.handleChangeRange}
                      onChangeEndTime={this.handleChangeEndTime}
                      onChangeResolution={this.handleChangeResolution}
                      onChangeStacking={this.handleChangeStacking}
                      onChangeMaxSourceResolution={this.handleChangeMaxSourceResolution}
                    />
                    <GraphTabContent
                      data={this.state.data}
                      stacked={options.stacked}
                      useLocalTime={this.props.useLocalTime}
                      handleTimeRangeSelection={this.handleTimeRangeSelection}
                      lastQueryParams={this.state.lastQueryParams}
                    />
                  </>
                )}
              </TabPane>
            </TabContent>
          </Col>
        </Row>
        <Row>
          <Col>
            <Button className="float-right" color="link" onClick={this.props.removePanel} size="sm">
              Remove Panel
            </Button>
          </Col>
        </Row>
      </div>
    );
  }
}

export default Panel;
