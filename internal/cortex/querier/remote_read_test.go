// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

package querier

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/thanos/internal/cortex/cortexpb"
	"github.com/thanos-io/thanos/internal/cortex/ingester/client"
	"github.com/thanos-io/thanos/internal/cortex/querier/series"
)

func TestRemoteReadHandler(t *testing.T) {
	q := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return mockQuerier{
			matrix: model.Matrix{
				{
					Metric: model.Metric{"foo": "bar"},
					Values: []model.SamplePair{
						{Timestamp: 0, Value: 0},
						{Timestamp: 1, Value: 1},
						{Timestamp: 2, Value: 2},
						{Timestamp: 3, Value: 3},
					},
				},
			},
		}, nil
	})
	handler := RemoteReadHandler(q, log.NewNopLogger())

	requestBody, err := proto.Marshal(&client.ReadRequest{
		Queries: []*client.QueryRequest{
			{StartTimestampMs: 0, EndTimestampMs: 10},
		},
	})
	require.NoError(t, err)
	requestBody = snappy.Encode(nil, requestBody)
	request, err := http.NewRequest("GET", "/query", bytes.NewReader(requestBody))
	require.NoError(t, err)
	request.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, 200, recorder.Result().StatusCode)
	require.Equal(t, []string([]string{"application/x-protobuf"}), recorder.Result().Header["Content-Type"])
	responseBody, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	responseBody, err = snappy.Decode(nil, responseBody)
	require.NoError(t, err)
	var response client.ReadResponse
	err = proto.Unmarshal(responseBody, &response)
	require.NoError(t, err)

	expected := client.ReadResponse{
		Results: []*client.QueryResponse{
			{
				Timeseries: []cortexpb.TimeSeries{
					{
						Labels: []cortexpb.LabelAdapter{
							{Name: "foo", Value: "bar"},
						},
						Samples: []cortexpb.Sample{
							{Value: 0, TimestampMs: 0},
							{Value: 1, TimestampMs: 1},
							{Value: 2, TimestampMs: 2},
							{Value: 3, TimestampMs: 3},
						},
					},
				},
			},
		},
	}
	require.Equal(t, expected, response)
}

type mockQuerier struct {
	matrix model.Matrix
}

func (m mockQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if sp == nil {
		panic(fmt.Errorf("select params must be set"))
	}
	return series.MatrixToSeriesSet(m.matrix)
}

func (m mockQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m mockQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (mockQuerier) Close() error {
	return nil
}
