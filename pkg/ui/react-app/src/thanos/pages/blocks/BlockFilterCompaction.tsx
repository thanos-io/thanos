import React, { FC, ChangeEvent } from 'react';
import Checkbox from '../../../components/Checkbox';
import { Input } from 'reactstrap';
import styles from './blocks.module.css';

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
    <div className={styles.blockFilter} style={{ marginLeft: '24px' }}>
      <Checkbox style={{ marginRight: '4px' }} id={id} defaultChecked={defaultChecked} onChange={onChangeCheckbox} />
      <p style={{ marginRight: '4px' }}>Filter by compaction level</p>
      <Input
        type="number"
        style={{ width: '80px', marginBottom: '1rem' }}
        onChange={onChangeInput}
        defaultValue={defaultValue}
        min={0}
      />
    </div>
  );
};
