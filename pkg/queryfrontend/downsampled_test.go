// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"testing"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestDownsampled_MinResponseTime(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		sampleStreams []queryrange.SampleStream
		expected      int64
	}{
		{
			desc:          "empty []sampleStream",
			sampleStreams: []queryrange.SampleStream{},
			expected:      -1,
		},
		{
			desc: "one SampleStream with zero samples",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{},
				},
			},
			expected: -1,
		},
		{
			desc: "one SampleStream with one sample at zero time",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 0},
					},
				},
			},
			expected: 0,
		},
		{
			desc: "one SampleStream with one sample",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 1},
					},
				},
			},
			expected: 1,
		},
		{
			desc: "two SampleStreams, first is the earliest",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 1},
					},
				},
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 2},
					},
				},
			},
			expected: 1,
		},
		{
			desc: "three SampleStreams, second is earliest",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 2},
						{TimestampMs: 3},
					},
				},
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 1},
					},
				},
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 2},
					},
				},
			},
			expected: 1,
		},
		{
			desc: "three SampleStreams, last is earliest",
			sampleStreams: []queryrange.SampleStream{
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 2},
						{TimestampMs: 3},
					},
				},
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 2},
					},
				},
				{
					Samples: []cortexpb.Sample{
						{TimestampMs: 1},
					},
				},
			},
			expected: 1,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			pr := queryrange.NewEmptyPrometheusResponse()
			pr.Data.Result = tc.sampleStreams
			res := minResponseTime(pr)
			testutil.Equals(t, tc.expected, res)
		})
	}
}
