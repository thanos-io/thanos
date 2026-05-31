import React, { FC, ComponentType } from 'react';
import { UncontrolledAlert } from 'reactstrap';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSpinner } from '@fortawesome/free-solid-svg-icons';

interface StatusIndicatorProps {
  error?: Error;
  isLoading?: boolean;
  customErrorMsg?: JSX.Element;
  componentTitle?: string;
}

export const withStatusIndicator =
  <T extends Record<string, any>>(
    Component: ComponentType<T>
  ): FC<Omit<React.ComponentProps<typeof Component>, keyof StatusIndicatorProps> & StatusIndicatorProps> =>
  ({ error, isLoading, customErrorMsg, componentTitle, ...rest }: StatusIndicatorProps) => {
    if (error) {
      return (
        <UncontrolledAlert color="danger">
          {customErrorMsg ? (
            customErrorMsg
          ) : (
            <>
              <strong>Error:</strong> Error fetching {componentTitle || Component.displayName}: {error.message}
            </>
          )}
        </UncontrolledAlert>
      );
    }

    if (isLoading) {
      return (
        <FontAwesomeIcon
          size="3x"
          icon={faSpinner}
          spin
          className="position-absolute"
          style={{ transform: 'translate(-50%, -50%)', top: '50%', left: '50%' }}
        />
      );
    }

    return <Component {...(rest as T)} />;
  };
