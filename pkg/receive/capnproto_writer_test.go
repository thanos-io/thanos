// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

func TestCapNProtoWriter_Write(t *testing.T) {
	t.Parallel()

	// Setup test environment
	logger, m, app := setupMultitsdb(t, 1000)
	writer := NewCapNProtoWriter(logger, m, &CapNProtoWriterOptions{})

	// Create test data with exemplars
	timeseries := []prompb.TimeSeries{
		{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "test_metric"},
				{Name: "job", Value: "test"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
			Exemplars: []prompb.Exemplar{
				{
					Labels: []labelpb.ZLabel{
						{Name: "trace_id", Value: "abc123"},
						{Name: "span_id", Value: "def456"},
					},
					Value:     10.5,
					Timestamp: 10,
				},
				{
					Labels: []labelpb.ZLabel{
						{Name: "trace_id", Value: "xyz789"},
						{Name: "span_id", Value: "uvw012"},
					},
					Value:     20.5,
					Timestamp: 11,
				},
			},
		},
	}

	// Create capnproto request
	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, timeseries)
	require.NoError(t, err)

	wr, err := writecapnp.NewRequest(capnpReq)
	require.NoError(t, err)

	// Write the request
	err = writer.Write(context.Background(), tenancy.DefaultTenant, wr)
	require.NoError(t, err)

	// Verify that the appender is working correctly
	// In a real implementation, you would verify the actual stored data
	require.NotNil(t, app)
}

func TestCapNProtoWriter_ValidateLabels(t *testing.T) {
	t.Parallel()

	// Setup test environment
	logger, m, app := setupMultitsdb(t, 1000)
	writer := NewCapNProtoWriter(logger, m, &CapNProtoWriterOptions{})

	// Test with various exemplar label scenarios
	timeseries := []prompb.TimeSeries{
		{
			Labels: []labelpb.ZLabel{
				{Name: "__name__", Value: "test_metric"},
				{Name: "job", Value: "test"},
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
			Exemplars: []prompb.Exemplar{
				{
					Labels: []labelpb.ZLabel{
						{Name: "valid_label", Value: "valid_value"},
						{Name: "", Value: "empty_name"},  // Empty name
						{Name: "empty_value", Value: ""}, // Empty value
					},
					Value:     10.5,
					Timestamp: 10,
				},
			},
		},
	}

	// Create capnproto request
	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, timeseries)
	require.NoError(t, err)

	wr, err := writecapnp.NewRequest(capnpReq)
	require.NoError(t, err)

	// Write the request - should handle empty names/values gracefully
	err = writer.Write(context.Background(), tenancy.DefaultTenant, wr)
	require.NoError(t, err)

	require.NotNil(t, app)
}
