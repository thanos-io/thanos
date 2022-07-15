// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package grpcutil

import (
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

// IsGRPCContextCanceled returns whether the input error is a GRPC error wrapping
// the context.Canceled error.
func IsGRPCContextCanceled(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	return s.Code() == codes.Canceled
}
