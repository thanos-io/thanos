package query

import (
	"context"
	"testing"

	"github.com/prometheus/tsdb/chunks"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc"
)

func TestQuerier_LabelValues(t *testing.T) {
	a := &testStoreClient{
		values: map[string][]string{
			"test": []string{"a", "b", "c", "d"},
		},
	}
	b := &testStoreClient{
		values: map[string][]string{
			// The contract is that label values are sorted but we should be resilient
			// to misbehaving clients.
			"test": []string{"a", "out-of-order", "d", "x", "y"},
		},
	}
	c := &testStoreClient{
		values: map[string][]string{
			"test": []string{"e"},
		},
	}
	expected := []string{"a", "b", "c", "d", "e", "out-of-order", "x", "y"}

	q := newQuerier(nil, context.Background(), []StoreInfo{
		testStoreInfo{client: a},
		testStoreInfo{client: b},
		testStoreInfo{client: c},
	}, 0, 10000)
	defer q.Close()

	vals, err := q.LabelValues("test")
	testutil.Ok(t, err)
	testutil.Equals(t, expected, vals)
}

// TestQuerier_Series catches common edge cases encountered when querying multiple store nodes.
// It is not a subtitute for testing fanin/merge procedures in depth.
func TestQuerier_Series(t *testing.T) {
	a := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "a"), []sample{{0, 0}, {2, 1}, {3, 2}}),
			testStoreSeries(t, labels.FromStrings("a", "b"), []sample{{2, 2}, {3, 3}, {4, 4}}),
		},
	}
	b := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "b"), []sample{{1, 1}, {2, 2}, {3, 3}}),
		},
	}
	c := &testStoreClient{
		series: []storepb.Series{
			testStoreSeries(t, labels.FromStrings("a", "c"), []sample{{100, 1}, {300, 3}, {400, 4}}),
		},
	}
	// Querier clamps the range to [1,300], which should drop some samples of the result above.
	// The store API allows endpoints to send more data then initially requested.
	q := newQuerier(nil, context.Background(), []StoreInfo{
		testStoreInfo{client: a},
		testStoreInfo{client: b},
		testStoreInfo{client: c},
	}, 1, 300)
	defer q.Close()

	res := q.Select()

	expected := []struct {
		lset    labels.Labels
		samples []sample
	}{
		{
			lset:    labels.FromStrings("a", "a"),
			samples: []sample{{2, 1}, {3, 2}},
		},
		{
			lset:    labels.FromStrings("a", "b"),
			samples: []sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
		},
		{
			lset:    labels.FromStrings("a", "c"),
			samples: []sample{{100, 1}, {300, 3}},
		},
	}

	i := 0
	for res.Next() {
		testutil.Equals(t, expected[i].lset, res.At().Labels())

		samples := expandSeries(t, res.At().Iterator())
		testutil.Equals(t, expected[i].samples, samples)

		i++
	}
	testutil.Ok(t, res.Err())
}

type testStoreInfo struct {
	labels []storepb.Label
	client storepb.StoreClient
}

func (s testStoreInfo) Labels() []storepb.Label {
	return s.labels
}

func (s testStoreInfo) Client() storepb.StoreClient {
	return s.client
}

type sample struct {
	t int64
	v float64
}

func expandSeries(t testing.TB, it storage.SeriesIterator) (res []sample) {
	for it.Next() {
		t, v := it.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, it.Err())
	return res
}

func testStoreSeries(t testing.TB, lset labels.Labels, smpls []sample) (s storepb.Series) {
	for _, l := range lset {
		s.Labels = append(s.Labels, storepb.Label{Name: l.Name, Value: l.Value})
	}
	c := chunks.NewXORChunk()
	a, err := c.Appender()
	testutil.Ok(t, err)

	for _, smpl := range smpls {
		a.Append(smpl.t, smpl.v)
	}
	s.Chunks = append(s.Chunks, storepb.Chunk{
		Type:    storepb.Chunk_XOR,
		MinTime: smpls[0].t,
		MaxTime: smpls[len(smpls)-1].t,
		Data:    c.Bytes(),
	})
	return s
}

type testStoreClient struct {
	values map[string][]string
	series []storepb.Series
}

func (s *testStoreClient) Info(ctx context.Context, req *storepb.InfoRequest, _ ...grpc.CallOption) (*storepb.InfoResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) Series(ctx context.Context, req *storepb.SeriesRequest, _ ...grpc.CallOption) (*storepb.SeriesResponse, error) {
	return &storepb.SeriesResponse{Series: s.series}, nil
}

func (s *testStoreClient) LabelNames(ctx context.Context, req *storepb.LabelNamesRequest, _ ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (s *testStoreClient) LabelValues(ctx context.Context, req *storepb.LabelValuesRequest, _ ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return &storepb.LabelValuesResponse{Values: s.values[req.Label]}, nil
}
