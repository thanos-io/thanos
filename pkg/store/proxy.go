package store

import (
	"context"
	"sync"

	"io"

	"math"

	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Info holds meta information about a store.
type Info struct {
	Addr string

	// Client to access the store.
	Client storepb.StoreClient

	// Labels that apply to all date exposed by the backing store.
	Labels []storepb.Label

	// Minimum and maximum time range of data in the store.
	MinTime, MaxTime int64
}

func (i *Info) String() string {
	return i.Addr
}

// ProxyStore implements the store API that proxies request to all given underlying stores.
type ProxyStore struct {
	logger         log.Logger
	stores         func() []*Info
	selectorLabels labels.Labels
}

// NewProxyStore returns a new ProxyStore that uses the given clients that implements storeAPI to fan-in all series to the client.
// Note that there is no deduplication support. Deduplication should be done on the highest level (just before PromQL)
func NewProxyStore(
	logger log.Logger,
	stores func() []*Info,
	selectorLabels labels.Labels,
) *ProxyStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	s := &ProxyStore{
		logger:         logger,
		stores:         stores,
		selectorLabels: selectorLabels,
	}
	return s
}

// Info returns store information about the external labels this store have.
func (s *ProxyStore) Info(ctx context.Context, r *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	res := &storepb.InfoResponse{
		MinTime: 0,
		MaxTime: math.MaxInt64,
		Labels:  make([]storepb.Label, 0, len(s.selectorLabels)),
	}
	for _, l := range s.selectorLabels {
		res.Labels = append(res.Labels, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return res, nil
}

// Series returns all series for a requested time range and label matcher. Requested series are taken from other
// stores and proxied to RPC client. NOTE: Resulted data are not trimmed exactly to min and max time range.
func (s *ProxyStore) Series(r *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	match, newMatchers, err := labelsMatches(s.selectorLabels, r.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}

	var (
		respCh    = make(chan *storepb.SeriesResponse, 10)
		seriesSet []storepb.SeriesSet
		g         errgroup.Group
	)

	for _, store := range s.stores() {
		// We might be able to skip the store if its meta information indicates
		// it cannot have series matching our query.
		// NOTE: all matchers are validated in labelsMatches method so we explicitly ignore error.
		if ok, _ := storeMatches(store, r.MinTime, r.MaxTime, newMatchers...); !ok {
			continue
		}
		sc, err := store.Client.Series(srv.Context(), &storepb.SeriesRequest{
			MinTime:            r.MinTime,
			MaxTime:            r.MaxTime,
			Matchers:           newMatchers,
			Aggregates:         r.Aggregates,
			MaxAggregateWindow: r.MaxAggregateWindow,
		})
		if err != nil {
			respCh <- storepb.NewWarnSeriesResponse(errors.Wrap(err, "fetch series"))
			continue
		}

		seriesSet = append(seriesSet, startStreamSeriesSet(sc, respCh, 10))
	}

	g.Go(func() error {
		defer close(respCh)

		mergedSet := storepb.MergeSeriesSets(seriesSet...)
		for mergedSet.Next() {
			var series storepb.Series
			series.Labels, series.Chunks = mergedSet.At()
			respCh <- storepb.NewSeriesResponse(&series)
		}
		return mergedSet.Err()
	})

	for resp := range respCh {
		if err := srv.Send(resp); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	return g.Wait()
}

// streamSeriesSet iterates over incoming stream of series.
// All errors are sent out of band via warning channel.
type streamSeriesSet struct {
	stream storepb.Store_SeriesClient
	warnCh chan<- *storepb.SeriesResponse

	currSeries *storepb.Series
	recvCh     chan *storepb.Series
}

func startStreamSeriesSet(
	stream storepb.Store_SeriesClient,
	warnCh chan<- *storepb.SeriesResponse,
	bufferSize int,
) *streamSeriesSet {
	s := &streamSeriesSet{
		stream: stream,
		warnCh: warnCh,
		recvCh: make(chan *storepb.Series, bufferSize),
	}
	go s.fetchLoop()
	return s
}

func (s *streamSeriesSet) fetchLoop() {
	defer close(s.recvCh)
	for {
		r, err := s.stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			s.warnCh <- storepb.NewWarnSeriesResponse(errors.Wrap(err, "receive series"))
			return
		}

		if w := r.GetWarning(); w != "" {
			s.warnCh <- storepb.NewWarnSeriesResponse(errors.New(w))
			continue
		}
		s.recvCh <- r.GetSeries()
	}
}

// Next blocks until new message is received or stream is closed.
func (s *streamSeriesSet) Next() (ok bool) {
	s.currSeries, ok = <-s.recvCh
	return ok
}

func (s *streamSeriesSet) At() ([]storepb.Label, []storepb.AggrChunk) {
	if s.currSeries == nil {
		return nil, nil
	}
	return s.currSeries.Labels, s.currSeries.Chunks
}
func (s *streamSeriesSet) Err() error {
	return nil
}

// matchStore returns true if the given store may hold data for the given label matchers.
func storeMatches(s *Info, mint, maxt int64, matchers ...storepb.LabelMatcher) (bool, error) {
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

func (s *ProxyStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// LabelValues returns all known label values for a given label name.
func (s *ProxyStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	var (
		warnings []string
		all      [][]string
		mtx      sync.Mutex
		wg       sync.WaitGroup
	)
	for _, s := range s.stores() {
		wg.Add(1)
		go func(s *Info) {
			defer wg.Done()
			resp, err := s.Client.LabelValues(ctx, &storepb.LabelValuesRequest{
				Label: r.Label,
			})
			if err != nil {
				mtx.Lock()
				warnings = append(warnings, errors.Wrap(err, "fetch label values").Error())
				mtx.Unlock()
				return
			}

			mtx.Lock()
			warnings = append(warnings, resp.Warnings...)
			all = append(all, resp.Values)
			mtx.Unlock()

			return
		}(s)
	}

	wg.Wait()
	return &storepb.LabelValuesResponse{
		Values:   strutil.MergeUnsortedSlices(all...),
		Warnings: warnings,
	}, nil
}
