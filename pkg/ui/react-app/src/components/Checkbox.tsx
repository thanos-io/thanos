import React, { FC, memo, CSSProperties } from 'react';
import { FormGroup, Label, Input, InputProps } from 'reactstrap';

interface CheckboxProps extends InputProps {
  wrapperStyles?: CSSProperties;
  isExplainCheckbox?: boolean;
}

const Checkbox: FC<CheckboxProps> = ({ children, wrapperStyles, id, disabled, isExplainCheckbox, ...rest }) => {
  return (
    <FormGroup className="custom-control custom-checkbox" style={wrapperStyles}>
      <Input {...rest} id={id} type="checkbox" className="custom-control-input" disabled={disabled} />
      <Label style={{ userSelect: 'none' }} className="custom-control-label" for={id}>
        {children}
        {isExplainCheckbox && disabled && (
          <div className="popup-message">
            Explain allows you to view an expandable query plan of the PromQL query similar to SQL EXPLAIN. This
            functionality is only available when using the Thanos engine
          </div>
        )}
      </Label>
    </FormGroup>
  );
};

export default memo(Checkbox);
