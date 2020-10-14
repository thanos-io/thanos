import React, { FC } from 'react';
import { Block } from './block';
import styles from './blocks.module.css';
import moment from 'moment';
import { Button } from 'reactstrap';
import { download } from './helpers';

export interface BlockDetailsProps {
  block: Block | undefined;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}

export const BlockDetails: FC<BlockDetailsProps> = ({ block, selectBlock }) => {
  return (
    <div className={`${styles.blockDetails} ${block && styles.open}`}>
      {block && (
        <>
          <div className={styles.detailsTop}>
            <span className={styles.header} data-testid="ulid">
              {block.ulid}
            </span>
            <button className={styles.closeBtn} onClick={(): void => selectBlock(undefined)}>
              &times;
            </button>
          </div>
          <hr />
          <div data-testid="start-time">
            <b>Start Time:</b> <span>{moment.unix(block.minTime / 1000).format('LLL')}</span>
          </div>
          <div data-testid="end-time">
            <b>End Time:</b> <span>{moment.unix(block.maxTime / 1000).format('LLL')}</span>
          </div>
          <div data-testid="duration">
            <b>Duration:</b> <span>{moment.duration(block.maxTime - block.minTime, 'ms').humanize()}</span>
          </div>
          <hr />
          <div data-testid="series">
            <b>Series:</b> <span>{block.stats.numSeries}</span>
          </div>
          <div data-testid="samples">
            <b>Samples:</b> <span>{block.stats.numSamples}</span>
          </div>
          <div data-testid="chunks">
            <b>Chunks:</b> <span>{block.stats.numChunks}</span>
          </div>
          <hr />
          <div data-testid="resolution">
            <b>Resolution:</b> <span>{block.thanos.downsample.resolution}</span>
          </div>
          <div data-testid="level">
            <b>Level:</b> <span>{block.compaction.level}</span>
          </div>
          <div data-testid="source">
            <b>Source:</b> <span>{block.thanos.source}</span>
          </div>
          <hr />
          <div data-testid="labels">
            <b>Labels:</b>
            <ul>
              {Object.entries(block.thanos.labels).map(([key, value]) => (
                <li key={key}>
                  <b>{key}: </b>
                  {value}
                </li>
              ))}
            </ul>
          </div>
          <hr />
          <div data-testid="download">
            <a href={download(block)} download="meta.json">
              <Button>Download meta.json</Button>
            </a>
          </div>
        </>
      )}
    </div>
  );
};
