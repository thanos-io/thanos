// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/318d6bc4bfe2d81af88697940740a378b503335a/storage/remote/otlptranslator/prometheusremotewrite/context.go#L18
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright The Prometheus Authors.

package otlptranslator

import "context"

// everyNTimes supports checking for context error every n times.
type everyNTimes struct {
	n   int
	i   int
	err error
}

// checkContext calls ctx.Err() every e.n times and returns an eventual error.
func (e *everyNTimes) checkContext(ctx context.Context) error {
	if e.err != nil {
		return e.err
	}

	e.i++
	if e.i >= e.n {
		e.i = 0
		e.err = ctx.Err()
	}

	return e.err
}
