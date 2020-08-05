import React, { FC } from 'react';
import { Block, BlocksPool } from './block';
import { BlockSpan } from './BlockSpan';
import styles from './blocks.module.css';

const BlocksRow: FC<{
  blocks: Block[];
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}> = ({ blocks, gridMinTime, gridMaxTime, selectBlock }) => {
  return (
    <div className={styles.row}>
      {blocks.map<JSX.Element>(b => (
        <BlockSpan selectBlock={selectBlock} block={b} gridMaxTime={gridMaxTime} gridMinTime={gridMinTime} key={b.ulid} />
      ))}
    </div>
  );
};

export const SourceView: FC<{
  data: BlocksPool;
  title: string;
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}> = ({ data, title, gridMaxTime, gridMinTime, selectBlock }) => {
  return (
    <>
      <div className={styles.source}>
        <div className={styles.title}>
          <p>{title}</p>
        </div>
        <div className={styles.rowsContainer}>
          {Object.keys(data).map(k => (
            <BlocksRow
              selectBlock={selectBlock}
              blocks={data[k]}
              key={k}
              gridMaxTime={gridMaxTime}
              gridMinTime={gridMinTime}
            />
          ))}
        </div>
      </div>
      <hr />
    </>
  );
};
