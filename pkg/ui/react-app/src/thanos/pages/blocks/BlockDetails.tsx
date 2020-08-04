import React, { FC } from 'react';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTimes } from '@fortawesome/free-solid-svg-icons';
import { Block } from './block';
import styles from './blocks.module.css';
import moment from 'moment';

interface BlockDetailsProps {
  block: Block | undefined;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
}

export const BlockDetails: FC<BlockDetailsProps> = ({ block, selectBlock }) => {
  return (
    <div className={`${styles.blockDetails} ${block && styles.open}`}>
      {block && (
        <>
          <div className={styles.detailsTop}>
            <p>{block.ulid}</p>
            <button className={styles.closeBtn} onClick={(): void => selectBlock(undefined)}>
              <FontAwesomeIcon icon={faTimes} />
            </button>
          </div>
          <hr />
          <div>
            <b>Start Time:</b> {moment.unix(block.minTime / 1000).format('LLL')}
          </div>
          <div>
            <b>End Time:</b> {moment.unix(block.maxTime / 1000).format('LLL')}
          </div>
          <div>
            <b>Duration:</b> {moment.duration(block.maxTime - block.minTime, 'ms').humanize()}
          </div>
          <hr />
          <div>
            <b>Series:</b> {block.stats.numSeries}
          </div>
          <div>
            <b>Samples:</b> {block.stats.numSamples}
          </div>
          <div>
            <b>Chunks:</b> {block.stats.numChunks}
          </div>
          <hr />
          <div>
            <b>Resolution:</b> {block.thanos.downsample.resolution}
          </div>
          <div>
            <b>Level:</b> {block.compaction.level}
          </div>
          <div>
            <b>Source:</b> {block.thanos.source}
          </div>
          <hr />
          <div>
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
        </>
      )}
    </div>
  );
};
