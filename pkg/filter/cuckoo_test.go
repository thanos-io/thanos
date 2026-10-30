// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package filter

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func TestCuckooMetricNameStoreFilter_Matches(t *testing.T) {
	filter := NewCuckooMetricNameStoreFilter(100)
	filter.ResetAndSet("http_requests_total", "up", "go_goroutines")

	tests := []struct {
		name     string
		matchers []*labels.Matcher
		want     bool
	}{
		{
			name:     "exact present",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up")},
			want:     true,
		},
		{
			name:     "exact absent",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "missing")},
			want:     false,
		},
		{
			name:     "regex set with one present",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "up|down")},
			want:     true,
		},
		{
			name:     "regex set with none present",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "a|b")},
			want:     false,
		},
		{
			name:     "non-enumerable regex",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "up.*")},
			want:     true,
		},
		{
			name:     "no metric name matcher",
			matchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "x")},
			want:     true,
		},
		{
			name:     "empty matcher slice",
			matchers: nil,
			want:     true,
		},
		{
			name: "two metric name constraints",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
				labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "a|b"),
			},
			want: false,
		},
		{
			name: "non-metric regex ignored",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "x|y"),
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := filter.Matches(test.matchers); got != test.want {
				t.Fatalf("Matches() = %v, want %v", got, test.want)
			}
		})
	}
}

func BenchmarkCuckooMetricNameStoreFilter_Matches(b *testing.B) {
	filter := NewCuckooMetricNameStoreFilter(20000)
	values := make([]string, 10000)
	for i := range values {
		values[i] = fmt.Sprintf("name_%d", i)
	}
	filter.ResetAndSet(values...)

	b.Run("equal", func(b *testing.B) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "name_9999")}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !filter.Matches(matchers) {
				b.Fatal("expected matcher to match")
			}
		}
	})

	b.Run("regexp_set", func(b *testing.B) {
		matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, "name_1|name_999999")}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if !filter.Matches(matchers) {
				b.Fatal("expected matcher to match")
			}
		}
	})
}
