import React, { FC, useMemo, useState } from 'react';
import { RouteComponentProps } from '@reach/router';
import { Alert } from 'reactstrap';
import { withStatusIndicator } from '../../../components/withStatusIndicator';
import { useFetch } from '../../../hooks/useFetch';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Block } from './block';
import { SourceView } from './SourceView';
import { BlockDetails } from './BlockDetails';
import { sortBlocks } from './helpers';
import styles from './blocks.module.css';

export interface BlockListProps {
  blocks: Block[];
  err: string | null;
  label: string;
  refreshedAt: string;
}

export const BlocksContent: FC<{ data: BlockListProps }> = ({ data }) => {
  const [selectedBlock, selectBlock] = useState<Block>();

  const { blocks, label } = data;

  const blockPools = useMemo(() => sortBlocks(blocks, label), [blocks, label]);
  const [gridMinTime, gridMaxTime] = useMemo(() => {
    let gridMinTime = blocks[0].minTime;
    let gridMaxTime = blocks[0].maxTime;
    blocks.forEach(block => {
      if (block.minTime < gridMinTime) {
        gridMinTime = block.minTime;
      }
      if (block.maxTime > gridMaxTime) {
        gridMaxTime = block.maxTime;
      }
    });
    return [gridMinTime, gridMaxTime];
  }, [blocks]);

  return (
    <>
      {blocks.length > 0 ? (
        <div className={styles.container}>
          <div className={styles.grid}>
            {Object.keys(blockPools).map(pk => (
              <SourceView
                key={pk}
                data={blockPools[pk]}
                title={pk}
                selectBlock={selectBlock}
                gridMinTime={gridMinTime}
                gridMaxTime={gridMaxTime}
              />
            ))}
          </div>
          <BlockDetails selectBlock={selectBlock} block={selectedBlock} />
        </div>
      ) : (
        <Alert color="warning">No blocks found.</Alert>
      )}
    </>
  );
};

const BlocksWithStatusIndicator = withStatusIndicator(BlocksContent);

export const Blocks: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix = '' }) => {
  const { response, error, isLoading } = useFetch<BlockListProps>(`${pathPrefix}/api/v1/blocks`);
  const { status: responseStatus } = response;
  const badResponse = responseStatus !== 'success' && responseStatus !== 'start fetching';

  return (
    <BlocksWithStatusIndicator
      data={response.data}
      error={badResponse ? new Error(responseStatus) : error}
      isLoading={isLoading}
    />
  );
};

export default Blocks;
