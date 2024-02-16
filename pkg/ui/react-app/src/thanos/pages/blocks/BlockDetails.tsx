import React, { FC, useState } from 'react';
import { Block } from './block';
import styles from './blocks.module.css';
import moment from 'moment';
import PathPrefixProps from '../../../types/PathPrefixProps';
import { Button, Modal, ModalBody, Form, Input, ModalHeader, ModalFooter } from 'reactstrap';
import { download } from './helpers';

export interface BlockDetailsProps {
  block: Block | undefined;
  selectBlock: React.Dispatch<React.SetStateAction<Block | undefined>>;
  disableAdminOperations: boolean;
}

export const BlockDetails: FC<BlockDetailsProps & PathPrefixProps> = ({
  pathPrefix = '',
  block,
  selectBlock,
  disableAdminOperations,
}) => {
  const [modalAction, setModalAction] = useState<string>('');
  const [detailValue, setDetailValue] = useState<string | null>(null);

  const submitMarkBlock = async (action: string, ulid: string, detail: string | null) => {
    try {
      const body = detail
        ? new URLSearchParams({
            id: ulid,
            action,
            detail,
          })
        : new URLSearchParams({
            id: ulid,
            action,
          });

      const response = await fetch(`${pathPrefix}/api/v1/blocks/mark`, {
        method: 'POST',
        body,
      });

      if (!response.ok) {
        throw new Error(response.statusText);
      }
    } finally {
      setModalAction('');
    }
  };

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
          {!disableAdminOperations && (
            <div>
              <div style={{ marginTop: '12px' }}>
                <Button
                  onClick={() => {
                    setModalAction('DELETION');
                    setDetailValue('');
                  }}
                >
                  Mark Deletion
                </Button>
              </div>
              <div style={{ marginTop: '12px' }}>
                <Button
                  onClick={() => {
                    setModalAction('NO_COMPACTION');
                    setDetailValue('');
                  }}
                >
                  Mark No Compaction
                </Button>
              </div>
            </div>
          )}
          <Modal isOpen={!!modalAction}>
            <ModalBody>
              <ModalHeader toggle={() => setModalAction('')}>
                Mark {modalAction === 'DELETION' ? 'Deletion' : 'No Compaction'} Detail (Optional)
              </ModalHeader>
              <Form
                onSubmit={(e) => {
                  e.preventDefault();
                  submitMarkBlock(modalAction, block.ulid, detailValue);
                }}
              >
                <Input
                  placeholder="Reason for marking block..."
                  style={{ marginBottom: '16px', marginTop: '16px' }}
                  onChange={(e) => setDetailValue(e.target.value)}
                />
                <ModalFooter>
                  <Button color="primary" type="submit">
                    Submit
                  </Button>
                </ModalFooter>
              </Form>
            </ModalBody>
          </Modal>
        </>
      )}
    </div>
  );
};
