// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"time"
)

// StepAlignMiddleware aligns the start and end of request to the step to
// improved the cacheability of the query results.
var StepAlignMiddleware = NewStepAlignMiddleware(time.UTC)

// NewStepAlignMiddleware returns middleware that aligns query ranges to their
// step. For steps that are a whole number of days, alignment is relative to
// midnight in location.
func NewStepAlignMiddleware(location *time.Location) Middleware {
	if location == nil {
		location = time.UTC
	}

	return MiddlewareFunc(func(next Handler) Handler {
		return stepAlign{
			next:     next,
			location: location,
		}
	})
}

type stepAlign struct {
	next     Handler
	location *time.Location
}

func (s stepAlign) Do(ctx context.Context, r Request) (Response, error) {
	step := r.GetStep()
	offset := int64(0)
	if step%(24*time.Hour.Milliseconds()) == 0 {
		location := s.location
		if location == nil {
			location = time.UTC
		}
		_, offsetSeconds := time.UnixMilli(r.GetStart()).In(location).Zone()
		offset = int64(offsetSeconds) * 1000
	}

	start := alignToStep(r.GetStart(), step, offset)
	end := alignToStep(r.GetEnd(), step, offset)
	return s.next.Do(ctx, r.WithStartEnd(start, end))
}

func alignToStep(timestamp, step, offset int64) int64 {
	return ((timestamp + offset) / step * step) - offset
}
