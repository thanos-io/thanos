// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// So far we found there is a bug in prometheus tsdb code when OOO is enabled.
// This is a workaround to detect the issue when tsdb selects irrelevant matched data.
// See https://github.com/thanos-io/thanos/issues/7481
func detectCorruptLabels(lbls labels.Labels, matchers []storepb.LabelMatcher) bool {
	for _, m := range matchers {
		v := lbls.Get(m.Name)
		if v == "" {
			continue
		}
		if (m.Type == storepb.LabelMatcher_EQ && v != m.Value) ||
			(m.Type == storepb.LabelMatcher_NEQ && v == m.Value) {
			return true
		} else if m.Name == labels.MetricName &&
			(m.Type == storepb.LabelMatcher_RE || m.Type == storepb.LabelMatcher_NRE) {
			matcher, err := labels.NewFastRegexMatcher(m.Value)
			return err != nil ||
				(m.Type == storepb.LabelMatcher_RE && !matcher.MatchString(v)) ||
				(m.Type == storepb.LabelMatcher_NRE && matcher.MatchString(v))
		}
	}
	return false
}

func requestMatches(matchers []storepb.LabelMatcher) string {
	var b strings.Builder
	for _, m := range matchers {
		b.WriteString(m.String())
	}
	return b.String()
}
