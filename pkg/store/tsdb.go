package store

import (
	"context"
	"math"
	"sort"

	"github.com/pkg/errors"

	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/prometheus/client_golang/prometheus"
)

type TSDBStore struct {
	logger log.Logger
	db     *tsdb.DB
	labels labels.Labels
}

// NewTSDBStore implements the store API against a local TSDB instance.
// It attaches the provided external labels to all results. It only responds with raw data
// and does not support downsampling.
func NewTSDBStore(logger log.Logger, reg prometheus.Registerer, db *tsdb.DB, externalLabels labels.Labels) *TSDBStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &TSDBStore{
		logger: logger,
		db:     db,
		labels: externalLabels,
	}
}

// Info returns store information about the Prometheus instance.
func (s *TSDBStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	res := &storepb.InfoResponse{
		MinTime: 0,
		MaxTime: math.MaxInt64,
		Labels:  make([]storepb.Label, 0, len(s.labels)),
	}
	if blocks := s.db.Blocks(); len(blocks) > 0 {
		res.MinTime = blocks[0].Meta().MinTime
	}
	for _, l := range s.labels {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

// Series returns all series for a requested time range and label matcher. The returned data may
// exceed the requested time bounds.
func (s *TSDBStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, newMatchers, err := labelsMatches(s.labels, r.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	matchers, err := translateMatchers(newMatchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	// TODO(fabxc): An improvement over this trivial approach would be to directly
	// use the chunks provided by TSDB in the response.
	// But since the sidecar has a similar approach, optimizing here has only
	// limited benefit for now.
	q, err := s.db.Querier(r.MinTime, r.MaxTime)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer q.Close()

	set, err := q.Select(matchers...)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var respSeries storepb.Series

	for set.Next() {
		series := set.At()

		c, err := s.encodeChunk(series.Iterator())
		if err != nil {
			return status.Errorf(codes.Internal, "encode chunk: %s", err)
		}

		respSeries.Labels = s.translateAndExtendLabels(series.Labels(), s.labels)
		respSeries.Chunks = append(respSeries.Chunks[:0], c)

		if err := srv.Send(storepb.NewSeriesResponse(&respSeries)); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
	}
	return nil
}

func (s *TSDBStore) encodeChunk(it tsdb.SeriesIterator) (storepb.AggrChunk, error) {
	chk := chunkenc.NewXORChunk()

	app, err := chk.Appender()
	if err != nil {
		return storepb.AggrChunk{}, err
	}
	var mint int64

	for i := 0; it.Next(); i++ {
		if i == 0 {
			mint, _ = it.At()
		}
		app.Append(it.At())
	}
	if it.Err() != nil {
		return storepb.AggrChunk{}, errors.Wrap(it.Err(), "read series")
	}
	maxt, _ := it.At()

	return storepb.AggrChunk{
		MinTime: mint,
		MaxTime: maxt,
		Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chk.Bytes()},
	}, nil
}

// translateAndExtendLabels transforms a metrics into a protobuf label set. It additionally
// attaches the given labels to it, overwriting existing ones on collision.
func (s *TSDBStore) translateAndExtendLabels(m, extend labels.Labels) []storepb.Label {
	lset := make([]storepb.Label, 0, len(m)+len(extend))

	for _, l := range m {
		if extend.Get(l.Name) != "" {
			continue
		}
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	for _, l := range extend {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Slice(lset, func(i, j int) bool {
		return lset[i].Name < lset[j].Name
	})
	return lset
}

// LabelNames returns all known label names.
func (s *TSDBStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues returns all known label values for a given label name.
func (s *TSDBStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	q, err := s.db.Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer q.Close()

	res, err := q.LabelValues(r.Label)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelValuesResponse{Values: res}, nil
}
