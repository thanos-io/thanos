import React, { ErrorInfo, ReactNode } from 'react';
import { Container, UncontrolledCollapse, Button } from 'reactstrap';
import styles from './ErrorBoundary.module.css';

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorState {
  error: Error | null;
  errorInfo: ErrorInfo | null;
}

class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      error: null,
      errorInfo: null,
    };
  }

  componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    this.setState({
      error,
      errorInfo,
    });
  }

  render(): React.ReactNode {
    if (this.state.errorInfo) {
      return (
        <Container fluid className={styles.container}>
          <h1>Aaaah! Something went wrong.</h1>
          <h3>
            Please file an issue in the&nbsp;
            <a href="https://github.com/thanos-io/thanos/issues" target="_blank" rel="noreferrer noopener">
              Thanos issue tracker.
            </a>
          </h3>
          <Button color="link" id="error-details-toggler" className={styles.detailsBtn}>
            View error details.
          </Button>
          <UncontrolledCollapse toggler="#error-details-toggler" className={styles.errorDiv}>
            <span>{this.state.error && this.state.error.toString()}</span>
            {this.state.errorInfo.componentStack}
          </UncontrolledCollapse>
        </Container>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
