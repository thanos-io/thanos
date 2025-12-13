// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storecapnp

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestSeriesRequestRoundTrip(t *testing.T) {
	req := &storepb.SeriesRequest{
		MinTime:             1000,
		MaxTime:             2000,
		MaxResolutionWindow: 5000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "http_requests_total"},
			{Type: storepb.LabelMatcher_RE, Name: "job", Value: ".*api.*"},
		},
		Aggregates:              []storepb.Aggr{storepb.Aggr_RAW, storepb.Aggr_COUNT},
		PartialResponseStrategy: storepb.PartialResponseStrategy_WARN,
		SkipChunks:              true,
		Step:                    15000,
		Range:                   60000,
		QueryHints: &storepb.QueryHints{
			StepMillis: 15000,
			Func:       &storepb.Func{Name: "rate"},
			Grouping:   &storepb.Grouping{By: true, Labels: []string{"job", "instance"}},
			Range:      &storepb.Range{Millis: 60000},
		},
		ShardInfo: &storepb.ShardInfo{
			ShardIndex:  1,
			TotalShards: 4,
			By:          true,
			Labels:      []string{"__name__"},
		},
		WithoutReplicaLabels: []string{"replica", "receive_replica"},
		Limit:                1000,
	}

	// Marshal to Cap'n Proto
	data, err := MarshalSeriesRequest(req)
	testutil.Ok(t, err)

	// Unmarshal back
	got, err := UnmarshalSeriesRequest(data)
	testutil.Ok(t, err)

	// Compare
	testutil.Equals(t, req.MinTime, got.MinTime)
	testutil.Equals(t, req.MaxTime, got.MaxTime)
	testutil.Equals(t, req.MaxResolutionWindow, got.MaxResolutionWindow)
	testutil.Equals(t, len(req.Matchers), len(got.Matchers))
	for i, m := range req.Matchers {
		testutil.Equals(t, m.Type, got.Matchers[i].Type)
		testutil.Equals(t, m.Name, got.Matchers[i].Name)
		testutil.Equals(t, m.Value, got.Matchers[i].Value)
	}
	testutil.Equals(t, req.Aggregates, got.Aggregates)
	testutil.Equals(t, req.PartialResponseStrategy, got.PartialResponseStrategy)
	testutil.Equals(t, req.SkipChunks, got.SkipChunks)
	testutil.Equals(t, req.Step, got.Step)
	testutil.Equals(t, req.Range, got.Range)
	testutil.Equals(t, req.QueryHints.StepMillis, got.QueryHints.StepMillis)
	testutil.Equals(t, req.QueryHints.Func.Name, got.QueryHints.Func.Name)
	testutil.Equals(t, req.QueryHints.Grouping.By, got.QueryHints.Grouping.By)
	testutil.Equals(t, req.QueryHints.Grouping.Labels, got.QueryHints.Grouping.Labels)
	testutil.Equals(t, req.QueryHints.Range.Millis, got.QueryHints.Range.Millis)
	testutil.Equals(t, req.ShardInfo.ShardIndex, got.ShardInfo.ShardIndex)
	testutil.Equals(t, req.ShardInfo.TotalShards, got.ShardInfo.TotalShards)
	testutil.Equals(t, req.ShardInfo.By, got.ShardInfo.By)
	testutil.Equals(t, req.ShardInfo.Labels, got.ShardInfo.Labels)
	testutil.Equals(t, req.WithoutReplicaLabels, got.WithoutReplicaLabels)
	testutil.Equals(t, req.Limit, got.Limit)
}

func TestSeriesRequestPackedRoundTrip(t *testing.T) {
	req := &storepb.SeriesRequest{
		MinTime: 1000,
		MaxTime: 2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}

	// Marshal to packed Cap'n Proto
	data, err := MarshalSeriesRequestPacked(req)
	testutil.Ok(t, err)

	// Unmarshal back
	got, err := UnmarshalSeriesRequestPacked(data)
	testutil.Ok(t, err)

	testutil.Equals(t, req.MinTime, got.MinTime)
	testutil.Equals(t, req.MaxTime, got.MaxTime)
	testutil.Equals(t, len(req.Matchers), len(got.Matchers))
}

func TestSeriesResponseRoundTrip_Series(t *testing.T) {
	resp := storepb.NewSeriesResponse(&storepb.Series{
		Labels: labelpb.ZLabelsFromPromLabels(labels.FromStrings(
			"__name__", "http_requests_total",
			"job", "api-server",
			"instance", "localhost:8080",
		)),
		Chunks: []storepb.AggrChunk{
			{
				MinTime: 1000,
				MaxTime: 2000,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_XOR,
					Data: []byte{1, 2, 3, 4, 5},
					Hash: 12345,
				},
			},
			{
				MinTime: 2000,
				MaxTime: 3000,
				Raw: &storepb.Chunk{
					Type: storepb.Chunk_XOR,
					Data: []byte{6, 7, 8, 9, 10},
					Hash: 67890,
				},
			},
		},
	})

	// Marshal to Cap'n Proto
	data, err := MarshalSeriesResponse(resp)
	testutil.Ok(t, err)

	// Unmarshal back
	got, err := UnmarshalSeriesResponse(data)
	testutil.Ok(t, err)

	// Compare
	gotSeries := got.GetSeries()
	testutil.Assert(t, gotSeries != nil, "expected series response")

	expSeries := resp.GetSeries()
	testutil.Equals(t, len(expSeries.Labels), len(gotSeries.Labels))
	for i, l := range expSeries.Labels {
		testutil.Equals(t, l.Name, gotSeries.Labels[i].Name)
		testutil.Equals(t, l.Value, gotSeries.Labels[i].Value)
	}

	testutil.Equals(t, len(expSeries.Chunks), len(gotSeries.Chunks))
	for i, c := range expSeries.Chunks {
		testutil.Equals(t, c.MinTime, gotSeries.Chunks[i].MinTime)
		testutil.Equals(t, c.MaxTime, gotSeries.Chunks[i].MaxTime)
		testutil.Equals(t, c.Raw.Type, gotSeries.Chunks[i].Raw.Type)
		testutil.Equals(t, c.Raw.Data, gotSeries.Chunks[i].Raw.Data)
		testutil.Equals(t, c.Raw.Hash, gotSeries.Chunks[i].Raw.Hash)
	}
}

func TestSeriesResponseRoundTrip_Warning(t *testing.T) {
	resp := storepb.NewWarnSeriesResponse(errors.New("some warning message"))

	// Marshal to Cap'n Proto
	data, err := MarshalSeriesResponse(resp)
	testutil.Ok(t, err)

	// Unmarshal back
	got, err := UnmarshalSeriesResponse(data)
	testutil.Ok(t, err)

	testutil.Equals(t, resp.GetWarning(), got.GetWarning())
}

func TestLabelNamesRequestRoundTrip(t *testing.T) {
	req := &storepb.LabelNamesRequest{
		PartialResponseDisabled: true,
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
		Start:                   1000,
		End:                     2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"},
		},
		WithoutReplicaLabels: []string{"replica"},
		Limit:                100,
	}

	data, err := MarshalLabelNamesRequest(req)
	testutil.Ok(t, err)

	got, err := UnmarshalLabelNamesRequest(data)
	testutil.Ok(t, err)

	testutil.Equals(t, req.PartialResponseDisabled, got.PartialResponseDisabled)
	testutil.Equals(t, req.PartialResponseStrategy, got.PartialResponseStrategy)
	testutil.Equals(t, req.Start, got.Start)
	testutil.Equals(t, req.End, got.End)
	testutil.Equals(t, len(req.Matchers), len(got.Matchers))
	testutil.Equals(t, req.WithoutReplicaLabels, got.WithoutReplicaLabels)
	testutil.Equals(t, req.Limit, got.Limit)
}

func TestLabelNamesResponseRoundTrip(t *testing.T) {
	resp := &storepb.LabelNamesResponse{
		Names:    []string{"__name__", "job", "instance", "method", "status"},
		Warnings: []string{"partial data from store-1"},
	}

	data, err := MarshalLabelNamesResponse(resp)
	testutil.Ok(t, err)

	got, err := UnmarshalLabelNamesResponse(data)
	testutil.Ok(t, err)

	testutil.Equals(t, resp.Names, got.Names)
	testutil.Equals(t, resp.Warnings, got.Warnings)
}

func TestLabelValuesRequestRoundTrip(t *testing.T) {
	req := &storepb.LabelValuesRequest{
		Label: "job",
		Start: 1000,
		End:   2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
		Limit: 500,
	}

	data, err := MarshalLabelValuesRequest(req)
	testutil.Ok(t, err)

	got, err := UnmarshalLabelValuesRequest(data)
	testutil.Ok(t, err)

	testutil.Equals(t, req.Label, got.Label)
	testutil.Equals(t, req.Start, got.Start)
	testutil.Equals(t, req.End, got.End)
	testutil.Equals(t, len(req.Matchers), len(got.Matchers))
	testutil.Equals(t, req.Limit, got.Limit)
}

func TestLabelValuesResponseRoundTrip(t *testing.T) {
	resp := &storepb.LabelValuesResponse{
		Values:   []string{"api-server", "web-server", "database", "cache"},
		Warnings: []string{"some warning"},
	}

	data, err := MarshalLabelValuesResponse(resp)
	testutil.Ok(t, err)

	got, err := UnmarshalLabelValuesResponse(data)
	testutil.Ok(t, err)

	testutil.Equals(t, resp.Values, got.Values)
	testutil.Equals(t, resp.Warnings, got.Warnings)
}
