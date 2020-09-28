import React, { FC } from 'react';
import moment from 'moment';
import { formatSize } from './helpers'
import { Block } from './block';
import styles from './blocks.module.css';

export interface BlockDetailsProps {
  block: Block | undefined;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}

export const BlockDetails: FC<BlockDetailsProps> = ({ block, selectBlock }) => {
  const meta = block?.meta;
  const size = block?.size;
  return (
    <div className={`${styles.blockDetails} ${block && styles.open}`}>
      {meta && size && (
        <>
          <div className={styles.detailsTop}>
            <span className={styles.header} data-testid="ulid">
              {meta.ulid}
            </span>
            <button className={styles.closeBtn} onClick={(): void => selectBlock(undefined)}>
              &times;
            </button>
          </div>
          <hr />
          <div data-testid="start-time">
            <b>Start Time:</b> <span>{moment.unix(meta.minTime / 1000).format('LLL')}</span>
          </div>
          <div data-testid="end-time">
            <b>End Time:</b> <span>{moment.unix(meta.maxTime / 1000).format('LLL')}</span>
          </div>
          <div data-testid="duration">
            <b>Duration:</b> <span>{moment.duration(meta.maxTime - meta.minTime, 'ms').humanize()}</span>
          </div>
          <hr />
          <div data-testid="series">
            <b>Series:</b> <span>{meta.stats.numSeries}</span>
          </div>
          <div data-testid="samples">
            <b>Samples:</b> <span>{meta.stats.numSamples}</span>
          </div>
          <div data-testid="chunks">
            <b>Chunks:</b> <span>{meta.stats.numChunks}</span>
          </div>
          <hr />
          <div data-testid="size">
            <b>Index Size:</b> <span>{formatSize(size.indexSize)}</span>
          </div>
          <div data-testid="size">
            <b>Chunk Size:</b> <span>{formatSize(size.chunkSize)}</span>
          </div>
          <hr />
          <div data-testid="resolution">
            <b>Resolution:</b> <span>{meta.thanos.downsample.resolution}</span>
          </div>
          <div data-testid="level">
            <b>Level:</b> <span>{meta.compaction.level}</span>
          </div>
          <div data-testid="source">
            <b>Source:</b> <span>{meta.thanos.source}</span>
          </div>
          <hr />
          <div data-testid="labels">
            <b>Labels:</b>
            <ul>
              {Object.entries(meta.thanos.labels).map(([key, value]) => (
                <li key={key}>
                  <b>{key}: </b>
                  {value}
                </li>
              ))}
            </ul>
          </div>
        </>
      )}
    </div>
  );
};
