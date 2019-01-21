package store

import (
	"context"
	"io"
	"math"
	"strings"
	"sync"

	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/improbable-eng/thanos/pkg/strutil"
	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Client holds meta information about a store.
type Client interface {
	// Client to access the store.
	storepb.StoreClient

	// Labels that apply to all data exposed by the backing store.
	Labels() []storepb.Label

	// Minimum and maximum time range of data in the store.
	TimeRange() (mint int64, maxt int64)

	String() string
}

// ProxyStore implements the store API that proxies request to all given underlying stores.
type ProxyStore struct {
	logger         log.Logger
	stores         func(context.Context) ([]Client, error)
	selectorLabels labels.Labels
}

// NewProxyStore returns a new ProxyStore that uses the given clients that implements storeAPI to fan-in all series to the client.
// Note that there is no deduplication support. Deduplication should be done on the highest level (just before PromQL)
func NewProxyStore(
	logger log.Logger,
	stores func(context.Context) ([]Client, error),
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

type ctxRespSender struct {
	ctx context.Context
	ch  chan<- *storepb.SeriesResponse
}

func newRespCh(ctx context.Context, buffer int) (*ctxRespSender, <-chan *storepb.SeriesResponse, func()) {
	respCh := make(chan *storepb.SeriesResponse, buffer)
	return &ctxRespSender{ctx: ctx, ch: respCh}, respCh, func() { close(respCh) }
}

func (s ctxRespSender) send(r *storepb.SeriesResponse) {
	select {
	case <-s.ctx.Done():
		return
	case s.ch <- r:
		return
	}
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

	stores, err := s.stores(srv.Context())
	if err != nil {
		err = errors.Wrap(err, "failed to get store APIs")
		level.Error(s.logger).Log("err", err)
		return status.Errorf(codes.Unknown, err.Error())
	}

	var (
		g, gctx = errgroup.WithContext(srv.Context())

		// Allow to buffer max 10 series response.
		// Each might be quite large (multi chunk long series given by sidecar).
		respSender, respRecv, closeFn = newRespCh(gctx, 10)
	)

	g.Go(func() error {
		var (
			seriesSet      []storepb.SeriesSet
			storeDebugMsgs []string
			r              = &storepb.SeriesRequest{
				MinTime:                 r.MinTime,
				MaxTime:                 r.MaxTime,
				Matchers:                newMatchers,
				Aggregates:              r.Aggregates,
				MaxResolutionWindow:     r.MaxResolutionWindow,
				PartialResponseDisabled: r.PartialResponseDisabled,
			}
			wg = &sync.WaitGroup{}
		)

		defer func() {
			wg.Wait()
			closeFn()
		}()

		for _, st := range stores {
			// We might be able to skip the store if its meta information indicates
			// it cannot have series matching our query.
			// NOTE: all matchers are validated in labelsMatches method so we explicitly ignore error.
			if ok, _ := storeMatches(st, r.MinTime, r.MaxTime, r.Matchers...); !ok {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s filtered out", st))
				continue
			}
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s queried", st))

			sc, err := st.Series(gctx, r)
			if err != nil {
				storeID := fmt.Sprintf("%v", storepb.LabelsToString(st.Labels()))
				if storeID == "" {
					storeID = "Store Gateway"
				}
				err = errors.Wrapf(err, "fetch series for %s %s", storeID, st)
				if r.PartialResponseDisabled {
					level.Error(s.logger).Log("err", err, "msg", "partial response disabled; aborting request")
					return err
				}
				respSender.send(storepb.NewWarnSeriesResponse(err))
				continue
			}

			// Schedule streamSeriesSet that translates gRPC streamed response into seriesSet (if series) or respCh if warnings.
			seriesSet = append(seriesSet, startStreamSeriesSet(gctx, wg, sc, respSender, st.String(), !r.PartialResponseDisabled))
		}

		level.Debug(s.logger).Log("msg", strings.Join(storeDebugMsgs, ";"))

		if len(seriesSet) == 0 {
			// This is indicates that configured StoreAPIs are not the ones end user expects
			err := errors.New("No store matched for this query")
			level.Warn(s.logger).Log("err", err, "stores", strings.Join(storeDebugMsgs, ";"))
			respSender.send(storepb.NewWarnSeriesResponse(err))
			return nil
		}

		mergedSet := storepb.MergeSeriesSets(seriesSet...)
		for mergedSet.Next() {
			var series storepb.Series
			series.Labels, series.Chunks = mergedSet.At()
			respSender.send(storepb.NewSeriesResponse(&series))
		}
		return mergedSet.Err()
	})

	for resp := range respRecv {
		if err := srv.Send(resp); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	if err := g.Wait(); err != nil {
		level.Error(s.logger).Log("err", err)
		return err
	}
	return nil

}

type warnSender interface {
	send(*storepb.SeriesResponse)
}

// streamSeriesSet iterates over incoming stream of series.
// All errors are sent out of band via warning channel.
type streamSeriesSet struct {
	stream storepb.Store_SeriesClient
	warnCh warnSender

	currSeries *storepb.Series
	recvCh     chan *storepb.Series

	errMtx sync.Mutex
	err    error

	name string
}

func startStreamSeriesSet(
	ctx context.Context,
	wg *sync.WaitGroup,
	stream storepb.Store_SeriesClient,
	warnCh warnSender,
	name string,
	partialResponse bool,
) *streamSeriesSet {
	s := &streamSeriesSet{
		stream: stream,
		warnCh: warnCh,
		recvCh: make(chan *storepb.Series, 10),
		name:   name,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(s.recvCh)
		for {
			r, err := s.stream.Recv()
			if err == io.EOF {
				return
			}

			if ctx.Err() != nil {
				return
			}

			if err != nil {
				if partialResponse {
					s.warnCh.send(storepb.NewWarnSeriesResponse(errors.Wrap(err, "receive series")))
					return
				}

				s.errMtx.Lock()
				defer s.errMtx.Unlock()
				s.err = err
				return
			}

			if w := r.GetWarning(); w != "" {
				s.warnCh.send(storepb.NewWarnSeriesResponse(errors.New(w)))
				continue
			}
			s.recvCh <- r.GetSeries()
		}
	}()
	return s
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
	s.errMtx.Lock()
	defer s.errMtx.Unlock()
	return errors.Wrap(s.err, s.name)
}

// matchStore returns true if the given store may hold data for the given label matchers.
func storeMatches(s Client, mint, maxt int64, matchers ...storepb.LabelMatcher) (bool, error) {
	storeMinTime, storeMaxTime := s.TimeRange()
	if mint > storeMaxTime || maxt < storeMinTime {
		return false, nil
	}
	for _, m := range matchers {
		for _, l := range s.Labels() {
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

// LabelNames returns all known label names.
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
		g, gctx  = errgroup.WithContext(ctx)
	)

	stores, err := s.stores(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	for _, st := range stores {
		store := st
		g.Go(func() error {
			resp, err := store.LabelValues(gctx, &storepb.LabelValuesRequest{
				Label: r.Label,
				PartialResponseDisabled: r.PartialResponseDisabled,
			})
			if err != nil {
				err = errors.Wrapf(err, "fetch label values from store %s", store)
				if r.PartialResponseDisabled {
					return err
				}

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

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return &storepb.LabelValuesResponse{
		Values:   strutil.MergeUnsortedSlices(all...),
		Warnings: warnings,
	}, nil
}
