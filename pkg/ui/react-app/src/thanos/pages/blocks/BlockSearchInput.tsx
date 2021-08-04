import React, { FC, ChangeEvent } from 'react';
import { Button, InputGroup, InputGroupAddon, InputGroupText, Input, Form } from 'reactstrap';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSearch } from '@fortawesome/free-solid-svg-icons';
import styles from './blocks.module.css';

interface BlockSearchInputProps {
  onChange: ({ target }: ChangeEvent<HTMLInputElement>) => void;
  onClick: () => void;
  defaultValue: string;
}

export const BlockSearchInput: FC<BlockSearchInputProps> = ({ onChange, onClick, defaultValue }) => {
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <InputGroup className={styles.blockInput}>
        <InputGroupAddon addonType="prepend">
          <InputGroupText>
            <FontAwesomeIcon icon={faSearch} />
          </InputGroupText>
        </InputGroupAddon>
        <Input placeholder="Search block by ulid" onChange={onChange} defaultValue={defaultValue} />
        <InputGroupAddon addonType="append">
          <Button className="execute-btn" color="primary" onClick={onClick} type="submit">
            Search
          </Button>
        </InputGroupAddon>
      </InputGroup>
    </Form>
  );
};
