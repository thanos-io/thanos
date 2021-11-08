import React, { FC } from 'react';
import { Block, BlocksPool } from './block';
import { BlockSpan } from './BlockSpan';
import styles from './blocks.module.css';
import { getBlockByUlid, getBlocksByCompactionLevel } from './helpers';

export const BlocksRow: FC<{
  blocks: Block[];
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  blockSearch: string;
  compactionLevel: number;
}> = ({ blocks, gridMinTime, gridMaxTime, selectBlock, blockSearch, compactionLevel }) => {
  let filteredBlocks = getBlockByUlid(blocks, blockSearch);
  filteredBlocks = getBlocksByCompactionLevel(filteredBlocks, compactionLevel);

  return (
    <div className={styles.row}>
      {filteredBlocks.map<JSX.Element>((b) => (
        <BlockSpan selectBlock={selectBlock} block={b} gridMaxTime={gridMaxTime} gridMinTime={gridMinTime} key={b.ulid} />
      ))}
    </div>
  );
};

export interface SourceViewProps {
  data: BlocksPool;
  title: string;
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  blockSearch: string;
  compactionLevel: number;
}

export const SourceView: FC<SourceViewProps> = ({
  data,
  title,
  gridMaxTime,
  gridMinTime,
  selectBlock,
  blockSearch,
  compactionLevel,
}) => {
  return (
    <>
      <div className={styles.source}>
        <div className={styles.title} title={title}>
          <span>{title}</span>
        </div>
        <div className={styles.rowsContainer}>
          {Object.keys(data).map((k) => (
            <React.Fragment key={k}>
              {data[k].map((b, i) => (
                <BlocksRow
                  selectBlock={selectBlock}
                  blocks={b}
                  key={`${k}-${i}`}
                  gridMaxTime={gridMaxTime}
                  gridMinTime={gridMinTime}
                  blockSearch={blockSearch}
                  compactionLevel={compactionLevel}
                />
              ))}
            </React.Fragment>
          ))}
        </div>
      </div>
      <hr />
    </>
  );
};
