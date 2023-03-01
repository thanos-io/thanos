// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package queryfrontend

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/weaveworks/common/httpgrpc"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/internal/cortex/querier/queryrange"
)

func TestSplitQuery(t *testing.T) {
	for i, tc := range []struct {
		input    queryrange.Request
		expected []queryrange.Request
		interval time.Duration
	}{
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 60 * 60 * seconds,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 60 * 60 * seconds,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   60 * 60 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   60 * 60 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   2 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo @ start()",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo @ 0.000",
				},
				&ThanosQueryRangeRequest{
					Start: 24 * 3600 * seconds,
					End:   2 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo @ 0.000",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   2 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo @ end()",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo @ 172800.000",
				},
				&ThanosQueryRangeRequest{
					Start: 24 * 3600 * seconds,
					End:   2 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo @ 172800.000",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 0,
				End:   2 * 3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 0,
					End:   (3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&ThanosQueryRangeRequest{
					Start: 3 * 3600 * seconds,
					End:   2 * 3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 3 * 3600 * seconds,
				End:   3 * 24 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 3 * 3600 * seconds,
					End:   (24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&ThanosQueryRangeRequest{
					Start: 24 * 3600 * seconds,
					End:   (2 * 24 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&ThanosQueryRangeRequest{
					Start: 2 * 24 * 3600 * seconds,
					End:   3 * 24 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: day,
		},
		{
			input: &ThanosQueryRangeRequest{
				Start: 2 * 3600 * seconds,
				End:   3 * 3 * 3600 * seconds,
				Step:  15 * seconds,
				Query: "foo",
			},
			expected: []queryrange.Request{
				&ThanosQueryRangeRequest{
					Start: 2 * 3600 * seconds,
					End:   (3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&ThanosQueryRangeRequest{
					Start: 3 * 3600 * seconds,
					End:   (2 * 3 * 3600 * seconds) - (15 * seconds),
					Step:  15 * seconds,
					Query: "foo",
				},
				&ThanosQueryRangeRequest{
					Start: 2 * 3 * 3600 * seconds,
					End:   3 * 3 * 3600 * seconds,
					Step:  15 * seconds,
					Query: "foo",
				},
			},
			interval: 3 * time.Hour,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			queries, err := splitQuery(tc.input, tc.interval)
			require.NoError(t, err)
			require.Equal(t, tc.expected, queries)
		})
	}
}

func TestSplitQuery_PromQLErrorReturnsJson(t *testing.T) {
	input := &ThanosQueryRangeRequest{
		Start: 2 * 3600 * seconds,
		End:   3 * 3 * 3600 * seconds,
		Step:  15 * seconds,
		Query: "foo{",
	}
	queries, err := splitQuery(input, 1*time.Hour)
	require.Error(t, err)
	require.Nil(t, queries)

	resp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "could not assemble httpgrpc.HTTPResponse, is not status.Status")

	require.True(t, json.Valid(resp.Body), "error message is not valid JSON: %s", resp.Body)
}
