import React, { FC } from 'react';
import { Container, Collapse, Table, Badge } from 'reactstrap';
import { now } from 'moment';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faMinus } from '@fortawesome/free-solid-svg-icons';
import { ToggleMoreLess } from '../../../components/ToggleMoreLess';
import { useLocalStorage } from '../../../hooks/useLocalStorage';
import { getColor } from '../../../pages/targets/target';
import { formatRelative, formatTime, parseTime, isValidTime } from '../../../utils';
import { Store } from './store';
import StoreLabels from './StoreLabels';

export type StorePoolPanelProps = { title: string; storePool: Store[] };

export const columns = [
  'Endpoint',
  'Status',
  'Announced LabelSets',
  'Min Time (UTC)',
  'Max Time (UTC)',
  'Last Successful Health Check',
  'Last Message',
];

export const storeTimeRangeMsg = (validMin: boolean, validMax: boolean): string => {
  if (!validMin && !validMax) {
    return "This store's time range data is not available";
  }
  if (!validMin && validMax) {
    return 'This store has no minimum time limit';
  }
  if (validMin && !validMax) {
    return 'This store has no maximum time limit';
  }

  return '';
};

export const getColorTimeRangeMsg = (validMin: boolean, validMax: boolean): string => {
  if (!validMin || !validMax) {
    return 'yellow';
  }

  return validMin && validMax ? 'green' : 'red';
};

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
              const validMinTime = isValidTime(minTime);
              const validMaxTime = isValidTime(maxTime);

              return (
                <tr key={name}>
                  <td data-testid="endpoint" title={storeTimeRangeMsg(validMinTime, validMaxTime)}>
                    {name}
                  </td>
                  <td data-testid="health">
                    <Badge color={color}>{health.toUpperCase()}</Badge>
                  </td>
                  <td data-testid="storeLabels">
                    <StoreLabels labelSets={labelSets} />
                  </td>
                  <td
                    data-testid="minTime"
                    title={storeTimeRangeMsg(validMinTime, validMaxTime)}
                    style={{ color: getColorTimeRangeMsg(validMinTime, validMaxTime) }}
                  >
                    {validMinTime ? formatTime(minTime) : <FontAwesomeIcon icon={faMinus} />}
                  </td>
                  <td
                    data-testid="maxTime"
                    title={storeTimeRangeMsg(validMinTime, validMaxTime)}
                    style={{ color: getColorTimeRangeMsg(validMinTime, validMaxTime) }}
                  >
                    {validMaxTime ? formatTime(maxTime) : <FontAwesomeIcon icon={faMinus} />}
                  </td>
                  <td data-testid="lastCheck">
                    {isValidTime(parseTime(lastCheck)) ? (
                      formatRelative(lastCheck, now())
                    ) : (
                      <FontAwesomeIcon icon={faMinus} />
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
