import React, { FC } from 'react';
import { Block } from './block';
import styles from './blocks.module.css';

interface BlockSpanProps {
  block: Block;
  gridMinTime: number;
  gridMaxTime: number;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}

const blockTooltip = (block: Block): string => {
  const labels = Object.entries(block.thanos.labels)
    .map(([k, v]) => `${k}="${v}"`)
    .join(', ');
  return `${block.ulid}${labels ? '\n{' + labels + '}' : ''}`;
};

export const BlockSpan: FC<BlockSpanProps> = ({ block, gridMaxTime, gridMinTime, selectBlock }) => {
  const viewWidth = gridMaxTime - gridMinTime;
  const spanWidth = ((block.maxTime - block.minTime) / viewWidth) * 100;
  const spanOffset = ((block.minTime - gridMinTime) / viewWidth) * 100;

  return (
    <button
      onClick={(): void => selectBlock(block)}
      title={blockTooltip(block)}
      className={`${styles.blockSpan} ${styles[`res-${block.thanos.downsample.resolution}`]} ${
        styles[`level-${block.compaction.level}`]
      }`}
      style={{
        width: `calc(${spanWidth.toFixed(4)}% + 1px)`,
        left: `${spanOffset.toFixed(4)}%`,
      }}
    />
  );
};
