// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestDetectCorruptLabels_EQ(t *testing.T) {
	good := labels.New(labels.Label{Name: labels.MetricName, Value: "up"})
	bad := labels.New(labels.Label{Name: labels.MetricName, Value: "kube_proxy_corrupt"})
	eq := []storepb.LabelMatcher{{Name: "__name__", Type: storepb.LabelMatcher_EQ, Value: "up"}}
	neq := []storepb.LabelMatcher{{Name: "__name__", Type: storepb.LabelMatcher_NEQ, Value: "up"}}
	assert.False(t, detectCorruptLabels(good, eq))
	assert.True(t, detectCorruptLabels(bad, eq))
	assert.False(t, detectCorruptLabels(bad, neq))
	assert.True(t, detectCorruptLabels(good, neq))
}

func TestDetectCorruptLabels_RE(t *testing.T) {
	good := labels.New(labels.Label{Name: labels.MetricName, Value: "usage_database_pool"})
	bad := labels.New(labels.Label{Name: labels.MetricName, Value: "kube_proxy_corrupt"})
	re := []storepb.LabelMatcher{{Name: "__name__", Type: storepb.LabelMatcher_RE, Value: "usage_.+_pool"}}
	nre := []storepb.LabelMatcher{{Name: "__name__", Type: storepb.LabelMatcher_NRE, Value: "usage_.+_pool"}}
	assert.False(t, detectCorruptLabels(good, re))
	assert.True(t, detectCorruptLabels(bad, re))
	assert.False(t, detectCorruptLabels(bad, nre))
	assert.True(t, detectCorruptLabels(good, nre))
}
