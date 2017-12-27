package store

import (
	"context"
	"sync"

	"io"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/query"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// QueryStore implements the store API for query layer.
type QueryStore struct {
	logger    log.Logger
	stores    func() []*query.StoreInfo
	extLabels labels.Labels
}

// NewQueryStore returns a new QueryStore that uses the given clients that implements storeAPI to fan-in all series to the client.
// This store is meant to be used for query federation, thus does not have support for deduplication. Deduplication
// should be done on the highest level (just before PromQL)
// It attaches the provided external labels to all results.
func NewQueryStore(
	logger log.Logger,
	stores func() []*query.StoreInfo,
	extLabels labels.Labels,
) *QueryStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	q := &QueryStore{
		logger:    logger,
		stores:    stores,
		extLabels: extLabels,
	}
	return q
}

// Info returns store information about the external labels this store have.
func (q *QueryStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	res := &storepb.InfoResponse{
		Labels: make([]storepb.Label, 0, len(q.extLabels)),
	}
	for _, l := range q.extLabels {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

// seriesSet returns all series for a requested time range and label matcher. Requested series are taken from other
// stores and proxied to RPC client.
func (q *QueryStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, newMatchers, err := extLabelsMatches(q.extLabels, r.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}

	var (
		respCh = make(chan *storepb.SeriesResponse)
		g      errgroup.Group
	)
	for _, s := range q.stores() {
		// We might be able to skip the store if its meta information indicates
		// it cannot have series matching our query.
		// NOTE: all matchers are validated in extLabelsMatches method so we explicitly ignore error.
		if ok, _ := storeMatches(s, r.MinTime, r.MaxTime, newMatchers...); !ok {
			continue
		}
		store := s

		g.Go(func() error {
			sc, err := store.Client.Series(srv.Context(), &storepb.SeriesRequest{
				MinTime:  r.MinTime,
				MaxTime:  r.MaxTime,
				Matchers: newMatchers,
			})
			if err != nil {
				respCh <- storepb.NewWarnSeriesResponse(errors.Wrap(err, "fetch series"))
				return nil
			}

			for {
				r, err := sc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					respCh <- storepb.NewWarnSeriesResponse(errors.Wrap(err, "receive series"))
					return nil
				}

				if w := r.GetWarning(); w != "" {
					respCh <- storepb.NewWarnSeriesResponse(errors.New(w))
					continue
				}

				series := r.GetSeries()
				series.Labels = extendLset(series.Labels, q.extLabels)
				respCh <- storepb.NewSeriesResponse(series)
			}

			return nil
		})
	}

	go func() {
		_ = g.Wait()
		close(respCh)
	}()

	for resp := range respCh {
		if err := srv.Send(resp); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	return nil
}

// matchStore returns true if the given store may hold data for the given label matchers.
func storeMatches(s *query.StoreInfo, mint, maxt int64, matchers ...storepb.LabelMatcher) (bool, error) {
	if mint > s.MaxTime || maxt < s.MinTime {
		return false, nil
	}
	for _, m := range matchers {
		for _, l := range s.Labels {
			if l.Name != m.Name {
				continue
			}

			m, err := translateMatcher(m)
			if err != nil {
				return false, err
			}

			if !m.Matches(l.Value) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (q *QueryStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues returns all known label values for a given label name.
func (q *QueryStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	var (
		g        errgroup.Group
		warnings []string
		all      [][]string
		mtx      sync.Mutex
	)
	for _, s := range q.stores() {
		store := s

		g.Go(func() error {
			resp, err := store.Client.LabelValues(ctx, &storepb.LabelValuesRequest{
				Label: r.Label,
			})
			if err != nil {
				mtx.Lock()
				warnings = append(warnings, errors.Wrap(err, "fetch label values").Error())
				mtx.Unlock()
				return nil
			}

			mtx.Lock()
			warnings = append(warnings, resp.Warnings...)
			all = append(all, resp.Values)
			mtx.Unlock()

			return nil
		})
	}

	_ = g.Wait()
	return &storepb.LabelValuesResponse{
		Values:   strutil.MergeUnsortedSlices(all...),
		Warnings: warnings,
	}, nil
}
