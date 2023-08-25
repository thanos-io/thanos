import React, { FC, memo, CSSProperties, useState } from 'react';
import { FormGroup, Label, Input, InputProps } from 'reactstrap';

interface CheckboxProps extends InputProps {
  wrapperStyles?: CSSProperties;
  isExplainCheckbox?: boolean;
}

const Checkbox: FC<CheckboxProps> = ({ children, wrapperStyles, id, disabled, isExplainCheckbox, ...rest }) => {
  const [showMessage, setShowMessage] = useState(false);

  const handleMouseEnter = () => {
    if (isExplainCheckbox && disabled) {
      setShowMessage(true);
    }
  };

  const handleMouseLeave = () => {
    setShowMessage(false);
  };
  return (
    <FormGroup className="custom-control custom-checkbox" style={wrapperStyles} onMouseEnter={handleMouseEnter} onMouseLeave={handleMouseLeave}>
      <Input {...rest} id={id} type="checkbox" className="custom-control-input" disabled={disabled} />
      <Label style={{ userSelect: 'none' }} className="custom-control-label" for={id}>
        {children}
      </Label>
      {isExplainCheckbox && showMessage && <div className="popup-message">This functionality is only available when using the Thanos engine</div>}
    </FormGroup>
  );
};

export default memo(Checkbox);
