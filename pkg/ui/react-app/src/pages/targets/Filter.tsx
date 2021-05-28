import React, { Dispatch, FC, SetStateAction } from 'react';
import { Button, ButtonGroup } from 'reactstrap';

export interface FilterData {
  showHealthy: boolean;
  showUnhealthy: boolean;
}

export interface FilterProps {
  filter: FilterData;
  setFilter: Dispatch<SetStateAction<FilterData>>;
}

const Filter: FC<FilterProps> = ({ filter, setFilter }) => {
  const { showHealthy } = filter;
  const btnProps = {
    all: {
      active: showHealthy,
      className: 'all',
      color: 'primary',
      onClick: (): void => setFilter({ ...filter, showHealthy: true }),
    },
    unhealthy: {
      active: !showHealthy,
      className: 'unhealthy',
      color: 'primary',
      onClick: (): void => setFilter({ ...filter, showHealthy: false }),
    },
  };
  return (
    <ButtonGroup className="mt-3 mb-4">
      <Button {...btnProps.all}>All</Button>
      <Button {...btnProps.unhealthy}>Unhealthy</Button>
    </ButtonGroup>
  );
};

export default Filter;
