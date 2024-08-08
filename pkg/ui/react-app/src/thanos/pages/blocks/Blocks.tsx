import React, { ChangeEvent, FC, useMemo, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import { UncontrolledAlert } from 'reactstrap';
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
    if (!err && blocks.length > 0) {
      let gridMinTime = blocks[0].minTime;
      let gridMaxTime = blocks[0].maxTime;
      blocks.forEach((block) => {
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
  }, [blocks, err]);

  const [
    {
      'min-time': viewMinTime,
      'max-time': viewMaxTime,
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
          <div className={styles.blockFilter}>
            <Checkbox
              id="find-overlap-block-checkbox"
              onChange={({ target }) => {
                setQuery({
                  'find-overlapping': target.checked,
                });
                setFindOverlappingBlocks(target.checked);
              }}
              defaultChecked={findOverlappingBlocks}
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
          </div>
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

export const PlanBlocksContent: FC<{ data: BlockListProps } & PathPrefixProps> = ({ data, pathPrefix }) => {
  const { blocks = [], err } = data;

  if (err) {
    return (
      <UncontrolledAlert color="danger">{err.toString()}</UncontrolledAlert>
    );
  }

  if (blocks.length === 0) {
    return (
      <UncontrolledAlert color="warning">
        No planned blocks found.
      </UncontrolledAlert>
    );
  }

  return (
    <div>
      <ul>
        {blocks.map((block) => (
          <li key={block.ulid}>
            ULID: {block.ulid} (PathPrefix: {pathPrefix}){" "}
            {/* Display pathPrefix for context */}
          </li>
        ))}
      </ul>
    </div>
  );
};

const BlocksWithStatusIndicator = withStatusIndicator(BlocksContent);
const PlanViewWithStatusIndicator = withStatusIndicator(PlanBlocksContent);

interface BlocksProps {
  view?: string;
}

export const Blocks: FC<RouteComponentProps & PathPrefixProps & BlocksProps> = ({ pathPrefix = '', view = 'global' }) => {
  const {
    response: globalBlocksResponse,
    error: globalBlocksError,
    isLoading: globalBlocksLoading,
  } = useFetch<BlockListProps>(`${pathPrefix}/api/v1/blocks${view ? '?view=' + view : ''}`);

  const {
    response: planResponse,
    error: planError,
    isLoading: planLoading,
  } = useFetch<BlockListProps>(`${pathPrefix}/api/v1/blocks/plan`);

  if (globalBlocksLoading || planLoading) return <div>Loading...</div>;
  if (globalBlocksError) return <UncontrolledAlert color="danger">{globalBlocksError.toString()}</UncontrolledAlert>;
  if (planError) return <UncontrolledAlert color="danger">{planError.toString()}</UncontrolledAlert>;

  if (!globalBlocksResponse.data && !planResponse.data)
    return <UncontrolledAlert color="warning">No blocks data available.</UncontrolledAlert>;

  return (
    <div>
      <h2>Planned Blocks</h2>
      <PlanViewWithStatusIndicator
        pathPrefix={pathPrefix}
        data={planResponse.data || { blocks: [] }}
        isLoading={planLoading}
        error={planError}
      />

      <h2>Global Blocks</h2>
      <BlocksWithStatusIndicator
        pathPrefix={pathPrefix}
        data={globalBlocksResponse.data || { blocks: [] }}
        error={globalBlocksError}
        isLoading={globalBlocksLoading}
      />
    </div>
  );
};

export default Blocks;
