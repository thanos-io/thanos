import React, { FC } from 'react';
import { Container, Collapse, Table, Badge } from 'reactstrap';
import { now } from 'moment';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faInfinity } from '@fortawesome/free-solid-svg-icons';
import { ToggleMoreLess } from '../../../components/ToggleMoreLess';
import { useLocalStorage } from '../../../hooks/useLocalStorage';
import { getColor } from '../../../pages/targets/target';
import { formatRelative, formatTime, parseTime } from '../../../utils';
import { Store } from './store';
import StoreLabels from './StoreLabels';

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

export const MAX_TIME = 9223372036854775807;

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
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {storePool.map((store: Store) => {
              const { name, minTime, maxTime, labelSets, lastCheck, lastError } = store;
              const health = lastError ? 'down' : 'up';
              const color = getColor(health);

              return (
                <tr key={name}>
                  <td data-testid="endpoint">{name}</td>
                  <td data-testid="health">
                    <Badge color={color}>{health.toUpperCase()}</Badge>
                  </td>
                  <td data-testid="storeLabels">
                    <StoreLabels labelSets={labelSets} />
                  </td>
                  <td data-testid="minTime">
                    {minTime >= MAX_TIME ? <FontAwesomeIcon icon={faInfinity} /> : formatTime(minTime)}
                  </td>
                  <td data-testid="maxTime">
                    {maxTime >= MAX_TIME ? <FontAwesomeIcon icon={faInfinity} /> : formatTime(maxTime)}
                  </td>
                  <td data-testid="lastCheck">
                    {parseTime(lastCheck) >= MAX_TIME ? (
                      <FontAwesomeIcon icon={faInfinity} />
                    ) : (
                      formatRelative(lastCheck, now())
                    )}{' '}
                    ago
                  </td>
                  <td data-testid="lastError">{lastError ? <Badge color={color}>{lastError}</Badge> : null}</td>
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
