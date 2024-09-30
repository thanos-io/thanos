// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filter

import "github.com/prometheus/prometheus/model/labels"

type StoreFilter interface {
	// Matches returns true if the filter matches the given matchers.
	Matches(matchers []*labels.Matcher) bool

	// ResetAndSet resets the filter and sets it to the given values.
	ResetAndSet(values ...string)
}

type AllowAllStoreFilter struct{}

func (f AllowAllStoreFilter) Matches(matchers []*labels.Matcher) bool {
	return true
}

func (f AllowAllStoreFilter) ResetAndSet(values ...string) {}
