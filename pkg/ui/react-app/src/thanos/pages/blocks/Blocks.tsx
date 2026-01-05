import React, { ChangeEvent, FC, useMemo, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import { Button, Input, InputGroup, InputGroupAddon, UncontrolledAlert, Form } from 'reactstrap';
import { useQueryParams, withDefault, NumberParam, StringParam, BooleanParam } from 'use-query-params';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Block } from './block';
import { SourceView } from './SourceView';
import { BlockDetails } from './BlockDetails';
import { BlockSearchInput } from './BlockSearchInput';
import { BlockFilterCompaction } from './BlockFilterCompaction';
import { sortBlocks, getBlockByUlid, getFilteredBlockPools } from './helpers';
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

  const sixMonthsInMs = 180 * daySeconds * 1000; // 6 months in milliseconds
  const [enableTimeFiltering, setEnableTimeFiltering] = useState<boolean>(false);
  const [range, setRange] = useState<number>(sixMonthsInMs);
  const [localEndTime, setLocalEndTime] = useState<number | null>(Date.now()); // Default to current time

  const { blocks, label, err } = data;

  // Use local state for time filtering
  const endTime = enableTimeFiltering ? localEndTime : null;
  const onChangeRange = (newRange: number) => setRange(newRange);
  const onChangeEndTime = (newEndTime: number | null) => setLocalEndTime(newEndTime);

  // Filter blocks by time range when time filtering is enabled
  const timeFilteredBlocks = useMemo(() => {
    if (enableTimeFiltering && endTime !== null && range > 0) {
      const startTime = endTime - range;
      return blocks.filter((block) => {
        return !(endTime < block.minTime || startTime > block.maxTime);
      });
    }
    return blocks;
  }, [blocks, enableTimeFiltering, endTime, range]);

  const [gridMinTime, gridMaxTime] = useMemo(() => {
    if (!err && timeFilteredBlocks.length > 0) {
      let gridMinTime = timeFilteredBlocks[0].minTime;
      let gridMaxTime = timeFilteredBlocks[0].maxTime;
      timeFilteredBlocks.forEach((block) => {
        if (block.minTime < gridMinTime) {
          gridMinTime = block.minTime;
        }
        if (block.maxTime > gridMaxTime) {
          gridMaxTime = block.maxTime;
        }
      });
      return [gridMinTime, gridMaxTime];
    }
    return [0, 0];
  }, [timeFilteredBlocks, err]);

  // Calculate view time based on time filtering
  const calculatedViewMinTime = enableTimeFiltering && endTime !== null && range > 0 ? endTime - range : gridMinTime;
  const calculatedViewMaxTime = enableTimeFiltering && endTime !== null ? endTime : gridMaxTime;

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
    'min-time': withDefault(NumberParam, calculatedViewMinTime),
    'max-time': withDefault(NumberParam, calculatedViewMaxTime),
    ulid: withDefault(StringParam, ''),
    'find-overlapping': withDefault(BooleanParam, false),
    'filter-compaction': withDefault(BooleanParam, false),
    'compaction-level': withDefault(NumberParam, 0),
  });

  // Use calculated view times when time filtering is enabled
  const viewMinTime = enableTimeFiltering ? calculatedViewMinTime : queryViewMinTime;
  const viewMaxTime = enableTimeFiltering ? calculatedViewMaxTime : queryViewMaxTime;

  const [filterCompaction, setFilterCompaction] = useState<boolean>(filterCompactionParam);
  const [findOverlappingBlocks, setFindOverlappingBlocks] = useState<boolean>(findOverlappingParam);
  const [compactionLevel, setCompactionLevel] = useState<number>(compactionLevelParam);
  const [compactionLevelInput, setCompactionLevelInput] = useState<string>(compactionLevelParam.toString());
  const [blockSearch, setBlockSearch] = useState<string>(blockSearchParam);

  const rangeRef = React.createRef<HTMLInputElement>();

  const blockPools = useMemo(
    () => sortBlocks(timeFilteredBlocks, label, findOverlappingBlocks),
    [timeFilteredBlocks, label, findOverlappingBlocks]
  );
  const filteredBlocks = useMemo(() => getBlockByUlid(timeFilteredBlocks, blockSearch), [timeFilteredBlocks, blockSearch]);
  const filteredBlockPools = useMemo(() => getFilteredBlockPools(blockPools, filteredBlocks), [filteredBlocks, blockPools]);

  const { response: flagsRes } = useFetch<FlagMap>(`${pathPrefix}/api/v1/status/flags`);
  const disableAdminOperations = flagsRes?.data?.['disable-admin-operations'] === 'true' || false;

  const setViewTime = (times: number[]): void => {
    setQuery({
      'min-time': times[0],
      'max-time': times[1],
    });
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

  const onChangeRangeInput = (rangeText: string): void => {
    const newRange = parseDuration(rangeText);
    if (newRange === null) {
      onChangeRange(range);
    } else {
      onChangeRange(newRange);
    }
  };

  const changeRangeInput = (newRange: number): void => {
    if (rangeRef.current) {
      rangeRef.current.value = formatDuration(newRange);
    }
  };

  const increaseRange = (): void => {
    for (const step of rangeSteps) {
      if (range < step) {
        changeRangeInput(step);
        onChangeRange(step);
        return;
      }
    }
  };

  const decreaseRange = (): void => {
    for (const step of rangeSteps.slice().reverse()) {
      if (range > step) {
        changeRangeInput(step);
        onChangeRange(step);
        return;
      }
    }
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
            <Checkbox
              id="enable-time-filtering"
              onChange={({ target }) => setEnableTimeFiltering(target.checked)}
              defaultChecked={enableTimeFiltering}
              wrapperStyles={{ marginBottom: 0 }}
            >
              Enable time filtering
            </Checkbox>

            <InputGroup className="range-input" size="sm">
              <InputGroupAddon addonType="prepend">
                <Button title="Decrease range" onClick={decreaseRange} disabled={!enableTimeFiltering}>
                  <FontAwesomeIcon icon={faMinus} fixedWidth />
                </Button>
              </InputGroupAddon>

              <Input
                defaultValue={formatDuration(range)}
                innerRef={rangeRef}
                onBlur={() => onChangeRangeInput(rangeRef.current!.value)}
                disabled={!enableTimeFiltering}
              />

              <InputGroupAddon addonType="append">
                <Button title="Increase range" onClick={increaseRange} disabled={!enableTimeFiltering}>
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
                onChange={setViewTime}
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
