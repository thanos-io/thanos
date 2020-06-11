import React, { FC } from 'react';
import { Container, Collapse, Table, Badge } from 'reactstrap';
import { now } from 'moment';
import { ToggleMoreLess } from '../../../components/ToggleMoreLess';
import { useLocalStorage } from '../../../hooks/useLocalStorage';
import { Store } from './store';
import StoreLabels from './StoreLabels';
import { getColor } from '../../../pages/targets/target';
import { formatRelative, formatTime } from '../../../utils';

export type StorePoolPanelProps = { title: string; storePool: Store[] };

export const columns = [
  'Endpoint',
  'Status',
  'Announced LabelSets',
  'Min Time',
  'Max Time',
  'Last Successful Health Check',
  'Last Message',
];

export const StorePoolPanel: FC<StorePoolPanelProps> = ({ title, storePool }) => {
  const [{ expanded }, setOptions] = useLocalStorage(`store-pool-${title}-expanded`, { expanded: true });

  return (
    <Container fluid>
      <ToggleMoreLess event={(): void => setOptions({ expanded: !expanded })} showMore={expanded}>
        <span style={{ textTransform: 'capitalize' }}>{title}</span>
      </ToggleMoreLess>
      <Collapse isOpen={expanded}>
        <Table size="sm" bordered hover>
          <thead>
            <tr key="header">
              {columns.map(column => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {storePool.map((store: Store) => {
              const {
                name,
                min_time: minTime,
                max_time: maxTime,
                label_sets: labelSets,
                last_check: lastCheck,
                last_error: lastError,
              } = store;
              const health = lastError ? 'down' : 'up';
              const color = getColor(health);

              return (
                <tr key={name}>
                  <td>{name}</td>
                  <td>
                    <Badge color={color}>{health.toUpperCase()}</Badge>
                  </td>
                  <td>
                    <StoreLabels labelSet={labelSets} />
                  </td>
                  <td>{formatTime(minTime)}</td>
                  <td>{formatTime(maxTime)}</td>
                  <td>{formatRelative(lastCheck, now())} ago</td>
                  <td>{lastError ? <Badge color={color}>{lastError}</Badge> : null}</td>
                </tr>
              );
            })}
          </tbody>
        </Table>
      </Collapse>
    </Container>
  );
};

export default StorePoolPanel;
