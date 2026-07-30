// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"testing"

	"capnproto.org/go/capnp/v3"
	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/pkg/receive/writecapnp"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"github.com/thanos-io/thanos/pkg/tenancy"
)

// newOldFormatWrite builds a request in the deprecated format where the tenant and
// time series list are set directly on the WriteRequest instead of in the data tuples.
func newOldFormatWrite(t *testing.T, tenant string, tsreq []prompb.TimeSeries) *writecapnp.Request {
	arena := capnp.SingleSegment(nil)
	_, seg, err := capnp.NewMessage(arena)
	require.NoError(t, err)

	wr, err := writecapnp.NewRootWriteRequest(seg)
	require.NoError(t, err)
	require.NoError(t, writecapnp.BuildIntoSingleTenantWriteRequest(wr, tenant, tsreq))

	req, err := writecapnp.NewSingleTenantRequest(wr, tenant)
	require.NoError(t, err)
	return req
}

func TestCapNProtoWriter_Write(t *testing.T) {
	t.Parallel()

	logger, m, app := setupMultitsdb(t, 1000)
	writer := NewCapNProtoWriter(logger, m, &CapNProtoWriterOptions{})

	// Create test data with valid exemplars
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

	capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, timeseries)
	require.NoError(t, err)

	syms, err := capnpReq.Symbols()
	require.NoError(t, err)

	data, err := capnpReq.Data()
	require.NoError(t, err)

	wr, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
	require.NoError(t, err)

	// Write the request
	err = writer.Write(context.Background(), wr)
	require.NoError(t, err)

	require.NotNil(t, app)

	// Query exemplars back from TSDB to verify they were stored correctly
	exemplarClients := m.TSDBExemplars()
	require.Contains(t, exemplarClients, tenancy.DefaultTenant, "Should have exemplar client for default tenant")

	exemplarClient := exemplarClients[tenancy.DefaultTenant]
	require.NotNil(t, exemplarClient, "Exemplar client should not be nil")

	srv := &exemplarsServer{ctx: context.Background()}

	// get matching exemplar
	err = exemplarClient.Exemplars(
		[][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric")}},
		0,  // start time
		20, // end time
		srv,
	)
	require.NoError(t, err, "Should be able to query exemplars")

	// Verify we got exemplar data back
	require.Len(t, srv.Data, 1, "Should have one series with exemplars")

	seriesData := srv.Data[0]
	require.Len(t, seriesData.Exemplars, 2, "Should have 2 exemplars")

	// Verify exemplar labels
	firstExemplar := seriesData.Exemplars[0]
	require.Equal(t, 10.5, firstExemplar.Value, "First exemplar value should match")
	require.Equal(t, int64(10), firstExemplar.Ts, "First exemplar timestamp should match")

	// Convert ZLabels to map for easier comparison
	firstLabels := make(map[string]string)
	for _, label := range firstExemplar.Labels.Labels {
		firstLabels[label.Name] = label.Value
	}

	require.Equal(t, "abc123", firstLabels["trace_id"], "First exemplar trace_id should match")
	require.Equal(t, "def456", firstLabels["span_id"], "First exemplar span_id should match")

	// Verify the second exemplar labels
	secondExemplar := seriesData.Exemplars[1]
	require.Equal(t, 20.5, secondExemplar.Value, "Second exemplar value should match")
	require.Equal(t, int64(11), secondExemplar.Ts, "Second exemplar timestamp should match")

	secondLabels := make(map[string]string)
	for _, label := range secondExemplar.Labels.Labels {
		secondLabels[label.Name] = label.Value
	}

	require.Equal(t, "xyz789", secondLabels["trace_id"], "Second exemplar trace_id should match")
	require.Equal(t, "uvw012", secondLabels["span_id"], "Second exemplar span_id should match")

	// A request in the deprecated format should still create the tenant.
	oldReq := newOldFormatWrite(t, "old-format-tenant", timeseries)
	require.NoError(t, writer.Write(context.Background(), oldReq))
	require.NoError(t, oldReq.Close())
	require.NotNil(t, m.testGetTenant("old-format-tenant"), "old format write should create the tenant")
}

func TestCapNProtoWriter_ValidateExemplarLabels(t *testing.T) {
	t.Parallel()

	lbls := []labelpb.ZLabel{{Name: "__name__", Value: "test"}}
	tests := map[string]struct {
		reqs             []*prompb.WriteRequest
		expectedErr      error
		expectedIngested []prompb.TimeSeries
		maxExemplars     int64
		opts             *WriterOptions
	}{
		"should succeed on valid series with exemplars": {
			reqs: []*prompb.WriteRequest{{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: lbls,
						// Ingesting an exemplar requires a sample to create the series first.
						Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						Exemplars: []prompb.Exemplar{
							{
								Labels:    []labelpb.ZLabel{{Name: "trace_id", Value: "123"}},
								Value:     11,
								Timestamp: 12,
							},
						},
					},
				},
			}},
			expectedErr:  nil,
			maxExemplars: 2,
		},
		"should fail on empty exemplar label name": {
			reqs: []*prompb.WriteRequest{{
				Timeseries: []prompb.TimeSeries{
					{
						Labels:  lbls,
						Samples: []prompb.Sample{{Value: 1, Timestamp: 10}},
						Exemplars: []prompb.Exemplar{
							{
								Labels:    []labelpb.ZLabel{{Name: "", Value: "123"}},
								Value:     11,
								Timestamp: 12,
							},
						},
					},
				},
			}},
			expectedErr:  errors.Wrapf(labelpb.ErrEmptyLabels, "add 1 series"),
			maxExemplars: 2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Run("capnproto_writer", func(t *testing.T) {
				logger, m, app := setupMultitsdb(t, testData.maxExemplars)

				opts := &CapNProtoWriterOptions{}
				if testData.opts != nil {
					opts.TooFarInFutureTimeWindow = testData.opts.TooFarInFutureTimeWindow
				}
				w := NewCapNProtoWriter(logger, m, opts)

				for idx, req := range testData.reqs {
					capnpReq, err := writecapnp.Build(tenancy.DefaultTenant, req.Timeseries)
					testutil.Ok(t, err)

					syms, err := capnpReq.Symbols()
					testutil.Ok(t, err)

					data, err := capnpReq.Data()
					testutil.Ok(t, err)

					wr, err := writecapnp.NewRequest(data.At(0), syms, tenancy.DefaultTenant)
					testutil.Ok(t, err)
					err = w.Write(context.Background(), wr)

					if testData.expectedErr == nil || idx < len(testData.reqs)-1 {
						testutil.Ok(t, err)
					} else {
						testutil.NotOk(t, err)
						testutil.Equals(t, testData.expectedErr.Error(), err.Error())
					}
				}

				assertWrittenData(t, app, testData.expectedIngested)
			})
		})
	}
}
