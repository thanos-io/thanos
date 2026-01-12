import React, { ChangeEvent, FC, useMemo, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import { Button, Form, Input, InputGroup, InputGroupAddon, UncontrolledAlert } from 'reactstrap';
import { BooleanParam, NumberParam, StringParam, useQueryParams, withDefault } from 'use-query-params';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Block } from './block';
import { SourceView } from './SourceView';
import { BlockDetails } from './BlockDetails';
import { BlockSearchInput } from './BlockSearchInput';
import { BlockFilterCompaction } from './BlockFilterCompaction';
import { getBlockByUlid, getFilteredBlockPools, sortBlocks } from './helpers';
import styles from './blocks.module.css';
import TimeRange from './TimeRange';
import Checkbox from '../../../components/Checkbox';
import { FlagMap } from '../../../pages/flags/Flags';
import TimeInput from '../../../pages/graph/TimeInput';
import { formatDuration, parseDuration } from '../../../utils';
import { faMinus, faPlus } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

const daySeconds = 24 * 60 * 60;

const rangeSteps = [1, 7, 30, 180, 360, 720].map((s) => s * daySeconds * 1000);

export interface BlockListProps {
  blocks: Block[];
  err: string | null;
  label: string;
  refreshedAt: string;
}

export const BlocksContent: FC<{ data: BlockListProps } & PathPrefixProps> = ({ pathPrefix = '', data }) => {
  const [selectedBlock, selectBlock] = useState<Block>();
  const [searchState, setSearchState] = useState<string>('');

  const { blocks, label, err } = data;

  const [gridMinTime, gridMaxTime] = useMemo(() => {
    if (!blocks || blocks.length === 0) return [0, 0];

    let gridMinTime = blocks[0].minTime;
    let gridMaxTime = blocks[0].maxTime;

    blocks.forEach((block) => {
      gridMinTime = Math.min(gridMinTime, block.minTime);
      gridMaxTime = Math.max(gridMaxTime, block.maxTime);
    });
    return [gridMinTime, gridMaxTime];
  }, [blocks]);

  const [
    {
      'min-time': queryViewMinTime,
      'max-time': queryViewMaxTime,
      ulid: blockSearchParam,
      'find-overlapping': findOverlappingParam,
      'filter-compaction': filterCompactionParam,
      'compaction-level': compactionLevelParam,
    },
    setQuery,
  ] = useQueryParams({
    'min-time': withDefault(NumberParam, gridMinTime),
    'max-time': withDefault(NumberParam, gridMaxTime),
    ulid: withDefault(StringParam, ''),
    'find-overlapping': withDefault(BooleanParam, false),
    'filter-compaction': withDefault(BooleanParam, false),
    'compaction-level': withDefault(NumberParam, 0),
  });

  const viewMinTime = queryViewMinTime;
  const viewMaxTime = queryViewMaxTime;

  // Initialize time controls from query parameters
  const [endTime, setEndTime] = useState<number>(viewMaxTime);
  const [range, setRange] = useState<number>(rangeSteps[rangeSteps.length - 1]);
  const [rangeInput, setRangeInput] = useState<string>(formatDuration(rangeSteps[rangeSteps.length - 1]));

  const [filterCompaction, setFilterCompaction] = useState<boolean>(filterCompactionParam);
  const [findOverlappingBlocks, setFindOverlappingBlocks] = useState<boolean>(findOverlappingParam);
  const [compactionLevel, setCompactionLevel] = useState<number>(compactionLevelParam);
  const [compactionLevelInput, setCompactionLevelInput] = useState<string>(compactionLevelParam.toString());
  const [blockSearch, setBlockSearch] = useState<string>(blockSearchParam);

  const blockPools = useMemo(() => sortBlocks(blocks, label, findOverlappingBlocks), [blocks, label, findOverlappingBlocks]);
  const filteredBlocks = useMemo(() => getBlockByUlid(blocks, blockSearch), [blocks, blockSearch]);
  const filteredBlockPools = useMemo(() => getFilteredBlockPools(blockPools, filteredBlocks), [filteredBlocks, blockPools]);

  const { response: flagsRes } = useFetch<FlagMap>(`${pathPrefix}/api/v1/status/flags`);
  const disableAdminOperations = flagsRes?.data?.['disable-admin-operations'] === 'true' || false;

  const setViewTime = (times: number[]): void => {
    setQuery({
      'min-time': times[0],
      'max-time': times[1],
    });
  };

  const handleTimeRangeChange = (times: number[]): void => {
    const [newMinTime, newMaxTime] = times;
    const newRange = newMaxTime - newMinTime;

    setEndTime(newMaxTime);
    setRange(newRange);
    setRangeInput(formatDuration(newRange));

    // Update the view
    setViewTime(times);
  };

  const updateTimeRange = (currentEndTime: number, currentRange: number) => {
    setViewTime([currentEndTime - currentRange, currentEndTime]);
  };

  const onChangeRange = (newRange: number) => {
    setRange(newRange);
    setRangeInput(formatDuration(newRange));
    updateTimeRange(endTime, newRange);
  };

  const onChangeEndTime = (newEndTime: number | null) => {
    if (newEndTime == null) {
      newEndTime = gridMaxTime;
    }
    // Cap the end time to gridMaxTime
    const cappedEndTime = Math.min(newEndTime, gridMaxTime);
    setEndTime(cappedEndTime);
    updateTimeRange(cappedEndTime, range);
  };

  const onChangeRangeInput = (rangeText: string): void => {
    const newRange = parseDuration(rangeText);
    if (newRange !== null) {
      onChangeRange(newRange);
    }
  };

  const increaseRange = (): void => {
    for (const step of rangeSteps) {
      if (range < step) {
        onChangeRange(step);
        return;
      }
    }
  };

  const decreaseRange = (): void => {
    for (const step of rangeSteps.slice().reverse()) {
      if (range > step) {
        onChangeRange(step);
        return;
      }
    }
  };

  const setBlockSearchInput = (searchState: string): void => {
    setQuery({
      ulid: searchState,
    });
    setBlockSearch(searchState);
  };

  const onChangeCompactionCheckbox = (target: EventTarget & HTMLInputElement) => {
    setFilterCompaction(target.checked);
    if (target.checked) {
      const compactionLevel: number = parseInt(compactionLevelInput);
      setQuery({
        'filter-compaction': target.checked,
        'compaction-level': compactionLevel,
      });
      setCompactionLevel(compactionLevel);
    } else {
      setQuery({
        'filter-compaction': target.checked,
        'compaction-level': 0,
      });
      setCompactionLevel(0);
    }
  };

  const onChangeCompactionInput = (target: HTMLInputElement) => {
    if (filterCompaction) {
      setQuery({
        'compaction-level': parseInt(target.value),
      });
      setCompactionLevel(parseInt(target.value));
    }
    setCompactionLevelInput(target.value);
  };

  if (err) return <UncontrolledAlert color="danger">{err.toString()}</UncontrolledAlert>;

  return (
    <>
      {blocks.length > 0 ? (
        <>
          <BlockSearchInput
            onChange={({ target }: ChangeEvent<HTMLInputElement>): void => setSearchState(target.value)}
            onClick={() => setBlockSearchInput(searchState)}
            defaultValue={blockSearchParam}
          />
          <Form inline className="graph-controls" onSubmit={(e) => e.preventDefault()}>
            <InputGroup className="range-input" size="sm">
              <InputGroupAddon addonType="prepend">
                <Button title="Decrease range" onClick={decreaseRange}>
                  <FontAwesomeIcon icon={faMinus} fixedWidth />
                </Button>
              </InputGroupAddon>

              <Input
                value={rangeInput}
                onChange={(e) => setRangeInput(e.target.value)}
                onBlur={(e) => onChangeRangeInput(e.target.value)}
              />

              <InputGroupAddon addonType="append">
                <Button title="Increase range" onClick={increaseRange}>
                  <FontAwesomeIcon icon={faPlus} fixedWidth />
                </Button>
              </InputGroupAddon>
            </InputGroup>

            <TimeInput
              time={endTime}
              useLocalTime={true}
              range={range}
              placeholder="End time"
              onChangeTime={onChangeEndTime}
            />

            <Checkbox
              id="find-overlap-block-checkbox"
              onChange={({ target }) => {
                setQuery({
                  'find-overlapping': target.checked,
                });
                setFindOverlappingBlocks(target.checked);
              }}
              defaultChecked={findOverlappingBlocks}
              wrapperStyles={{ marginBottom: 0 }}
            >
              Enable finding overlapping blocks
            </Checkbox>

            <BlockFilterCompaction
              id="filter-compaction-checkbox"
              defaultChecked={filterCompaction}
              onChangeCheckbox={({ target }) => onChangeCompactionCheckbox(target)}
              onChangeInput={({ target }: ChangeEvent<HTMLInputElement>): void => {
                onChangeCompactionInput(target);
              }}
              defaultValue={compactionLevelInput}
            />
          </Form>
          <div className={styles.container}>
            <div className={styles.grid}>
              <div className={styles.sources}>
                {Object.keys(filteredBlockPools).length > 0 ? (
                  Object.keys(filteredBlockPools).map((pk) => (
                    <SourceView
                      key={pk}
                      data={filteredBlockPools[pk]}
                      title={pk}
                      selectBlock={selectBlock}
                      gridMinTime={viewMinTime}
                      gridMaxTime={viewMaxTime}
                      blockSearch={blockSearch}
                      compactionLevel={compactionLevel}
                    />
                  ))
                ) : (
                  <div>
                    <h3>No Blocks Found!</h3>
                  </div>
                )}
              </div>
              <TimeRange
                gridMinTime={gridMinTime}
                gridMaxTime={gridMaxTime}
                viewMinTime={viewMinTime}
                viewMaxTime={viewMaxTime}
                onChange={handleTimeRangeChange}
              />
            </div>
            <BlockDetails
              pathPrefix={pathPrefix}
              selectBlock={selectBlock}
              block={selectedBlock}
              disableAdminOperations={disableAdminOperations}
            />
          </div>
        </>
      ) : (
        <UncontrolledAlert color="warning">No blocks found.</UncontrolledAlert>
      )}
    </>
  );
};

const BlocksWithStatusIndicator = withStatusIndicator(BlocksContent);

interface BlocksProps {
  view?: string;
}

export const Blocks: FC<RouteComponentProps & PathPrefixProps & BlocksProps> = ({ pathPrefix = '', view = 'global' }) => {
  const { response, error, isLoading } = useFetch<BlockListProps>(
    `${pathPrefix}/api/v1/blocks${view ? '?view=' + view : ''}`
  );
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';

  return (
    <BlocksWithStatusIndicator
      pathPrefix={pathPrefix}
      data={response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
    />
  );
};

export default Blocks;
