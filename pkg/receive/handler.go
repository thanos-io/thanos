// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"

	"github.com/go-kit/kit/log"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/errutil"

	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tracing"
)

const (
	// DefaultTenantHeader is the default header used to designate the tenant making a write request.
	DefaultTenantHeader = "THANOS-TENANT"
	// DefaultTenant is the default value used for when no tenant is passed via the tenant header.
	DefaultTenant = "default-tenant"
	// DefaultTenantLabel is the default label-name used for when no tenant is passed via the tenant header.
	DefaultTenantLabel = "tenant_id"
)

var (
	// conflictErr is returned whenever an operation fails due to any conflict-type error.
	conflictErr    = errors.New("conflict")
	errBadReplica  = errors.New("replica count exceeds replication factor")
	errNotReady    = errors.New("target not ready")
	errUnavailable = errors.New("target not available")
)

// Handler serves a Prometheus remote write receiving HTTP endpoint.
type Handler struct {
	writer          *Writer
	registry        prometheus.Registerer
	defaultTenantID string
	tracer          opentracing.Tracer
	logger          log.Logger
}

func NewHandler(
	writer *Writer,
	registry prometheus.Registerer,
	defaultTenantID string,
	tracer opentracing.Tracer,
	logger log.Logger,
) *Handler {
	return &Handler{
		writer:          writer,
		registry:        registry,
		defaultTenantID: defaultTenantID,
		tracer:          tracer,
		logger:          logger,
	}
}

func (h *Handler) handleRequest(ctx context.Context, tenant string, wreq *prompb.WriteRequest) error {
	var err error
	tracing.DoInSpan(ctx, "receive_tsdb_write", func(_ context.Context) {
		err = h.writer.Write(ctx, tenant, wreq)
	})
	if err != nil {
		// When a MultiError is added to another MultiError, the error slices are concatenated, not nested.
		// To avoid breaking the counting logic, we need to flatten the error.
		if errs, ok := err.(errutil.MultiError); ok {
			if countCause(errs, isConflict) > 0 {
				err = errors.Wrap(conflictErr, errs.Error())
			} else if countCause(errs, isNotReady) > 0 {
				err = errNotReady
			} else {
				err = errors.New(errs.Error())
			}
		}
		return err
	}
	return nil
}

// RemoteWrite implements the gRPC remote write handler for storepb.WriteableStore.
func (h *Handler) RemoteWrite(ctx context.Context, r *storepb.WriteRequest) (*storepb.WriteResponse, error) {
	span, ctx := tracing.StartSpan(ctx, "receive_grpc")
	defer span.Finish()

	err := h.handleRequest(ctx, r.Tenant, &prompb.WriteRequest{Timeseries: r.Timeseries})
	switch errors.Cause(err) {
	case nil:
		return &storepb.WriteResponse{}, nil
	case errNotReady:
		return nil, status.Error(codes.Unavailable, err.Error())
	case errUnavailable:
		return nil, status.Error(codes.Unavailable, err.Error())
	case conflictErr:
		return nil, status.Error(codes.AlreadyExists, err.Error())
	case errBadReplica:
		return nil, status.Error(codes.InvalidArgument, err.Error())
	default:
		return nil, status.Error(codes.Internal, err.Error())
	}
}

// countCause counts the number of errors within the given error
// whose causes satisfy the given function.
// countCause will inspect the error's cause or, if the error is a MultiError,
// the cause of each contained error but will not traverse any deeper.
func countCause(err error, f func(error) bool) int {
	errs, ok := err.(errutil.MultiError)
	if !ok {
		errs = []error{err}
	}
	var n int
	for i := range errs {
		if f(errors.Cause(errs[i])) {
			n++
		}
	}
	return n
}

// isConflict returns whether or not the given error represents a conflict.
func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return err == conflictErr ||
		err == storage.ErrDuplicateSampleForTimestamp ||
		err == storage.ErrOutOfOrderSample ||
		err == storage.ErrOutOfBounds ||
		status.Code(err) == codes.AlreadyExists
}

// isNotReady returns whether or not the given error represents a not ready error.
func isNotReady(err error) bool {
	return err == errNotReady ||
		err == tsdb.ErrNotReady ||
		status.Code(err) == codes.Unavailable
}
