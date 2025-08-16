# gRPC Readiness Interceptor Feature

## Overview

The `grpc-readiness-interceptor` feature provides a gRPC interceptor that checks service readiness before processing requests. This is particularly useful when using `publishNotReadyAddresses: true` in Kubernetes services to avoid client timeouts during pod startup.

## Problem Solved

When using `publishNotReadyAddresses: true`:
- Pods are discoverable by clients before they're ready to handle requests
- Clients may send gRPC requests to pods that are still starting up
- Without this feature, clients experience timeouts waiting for responses

## Solution

When the feature is enabled:
- gRPC requests to non-ready pods get empty responses immediately (no timeouts)
- gRPC requests to ready pods process normally
- Clients can gracefully handle empty responses and retry

## Usage

Enable the feature using the `--enable-feature` flag:

```bash
thanos receive \
  --enable-feature=grpc-readiness-interceptor \
  --label=replica="A" \
  # ... other flags
```

## How it Works

1. The feature adds interceptors to both unary and stream gRPC calls
2. Each interceptor checks `httpProbe.IsReady()` before processing
3. If not ready: returns empty response immediately
4. If ready: processes request normally

## Kubernetes Integration

This feature is designed to work with:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: thanos-receive
spec:
  # Allow traffic to non-ready pods
  publishNotReadyAddresses: true
  selector:
    app: thanos-receive
  ports:
  - name: grpc
    port: 10901
    targetPort: 10901
  - name: http
    port: 10902
    targetPort: 10902
```

## Testing

The feature includes comprehensive tests:
- Unit tests for interceptor behavior
- Integration tests with mock gRPC servers  
- Feature flag parsing tests

Run tests with:
```bash
go test ./pkg/receive -run TestReadiness
```

## Implementation Details

- Feature is disabled by default
- Uses existing HTTP probe for readiness state
- Minimal changes to existing codebase
- Self-contained in `pkg/receive/readiness.go`