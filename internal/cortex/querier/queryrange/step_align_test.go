// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package queryrange

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStepAlign(t *testing.T) {
	for i, tc := range []struct {
		input, expected *PrometheusRequest
	}{
		{
			input: &PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			expected: &PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},

		{
			input: &PrometheusRequest{
				Start: 2,
				End:   102,
				Step:  10,
			},
			expected: &PrometheusRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var result *PrometheusRequest
			s := stepAlign{
				next: HandlerFunc(func(_ context.Context, req Request) (Response, error) {
					result = req.(*PrometheusRequest)
					return nil, nil
				}),
			}
			_, err := s.Do(context.Background(), tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestStepAlignTimezone(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	day := int64((24 * time.Hour) / time.Millisecond)
	input := &PrometheusRequest{
		Start: 2*day + 10*60*60*1000,
		End:   4*day + 10*60*60*1000,
		Step:  day,
	}

	var result *PrometheusRequest
	s := NewStepAlignMiddleware(location).Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		result = req.(*PrometheusRequest)
		return nil, nil
	}))
	_, err := s.Do(context.Background(), input)
	require.NoError(t, err)

	// UTC+8 midnight is UTC 16:00 on the previous day.
	require.Equal(t, int64(day+16*60*60*1000), result.Start)
	require.Equal(t, int64(3*day+16*60*60*1000), result.End)
}

func TestStepAlignTimezoneOnlyAppliesToWholeDays(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	input := &PrometheusRequest{
		Start: 2*60*60*1000 + 1,
		End:   4*60*60*1000 + 1,
		Step:  60 * 60 * 1000,
	}

	var result *PrometheusRequest
	s := NewStepAlignMiddleware(location).Wrap(HandlerFunc(func(_ context.Context, req Request) (Response, error) {
		result = req.(*PrometheusRequest)
		return nil, nil
	}))
	_, err := s.Do(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, int64(2*60*60*1000), result.Start)
	require.Equal(t, int64(4*60*60*1000), result.End)
}
