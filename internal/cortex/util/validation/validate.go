// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package validation

const (
	// ErrQueryTooLong is used in chunk store, querier and query frontend.
	ErrQueryTooLong = "the query time range exceeds the limit (query length: %s, limit: %s)"
)
