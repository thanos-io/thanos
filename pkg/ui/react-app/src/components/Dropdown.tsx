import React, { FC, memo } from 'react';
import { FormGroup, Label, Input, InputProps } from 'reactstrap';

export interface DropdownOption {
  name: string;
  value: string;
}

interface DropdownProps extends InputProps {
  options: DropdownOption[];
}

const Dropdown: FC<DropdownProps> = ({ children, selected, options, id, ...rest }) => {
  return (
    <FormGroup className="custom-control d-inline">
      <Label for={id} className="control-label">
        {children}
      </Label>
      <Input
        {...rest}
        id={id}
        type="select"
        value={selected}
        className="control-input custom-select"
        style={{
          width: 'auto',
          marginLeft: '10px',
        }}
      >
        {options.map(function (option) {
          return <option value={option.value}>{option.name}</option>;
        })}
      </Input>
    </FormGroup>
  );
};

export default memo(Dropdown);
