import React, { FC, ChangeEvent } from 'react';
import Checkbox from '../../../components/Checkbox';
import { Input } from 'reactstrap';

interface BlockFilterCompactionProps {
  id: string;
  defaultChecked: boolean;
  onChangeCheckbox: ({ target }: ChangeEvent<HTMLInputElement>) => void;
  onChangeInput: ({ target }: ChangeEvent<HTMLInputElement>) => void;
  defaultValue: string;
}

export const BlockFilterCompaction: FC<BlockFilterCompactionProps> = ({
  id,
  defaultChecked,
  onChangeCheckbox,
  onChangeInput,
  defaultValue,
}) => {
  return (
    <>
      <Checkbox
        id={id}
        defaultChecked={defaultChecked}
        onChange={onChangeCheckbox}
        wrapperStyles={{ marginBottom: 0, display: 'inline-flex', alignItems: 'center' }}
      >
        Filter by compaction level
      </Checkbox>
      <Input
        type="number"
        style={{ width: '80px', marginLeft: '10px' }}
        onChange={onChangeInput}
        defaultValue={defaultValue}
        min={0}
        bsSize="sm"
      />
    </>
  );
};
