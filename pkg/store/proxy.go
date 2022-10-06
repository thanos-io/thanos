// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type ctxKey int

// StoreMatcherKey is the context key for the store's allow list.
const StoreMatcherKey = ctxKey(0)

// Client holds meta information about a store.
type Client interface {
	// StoreClient to access the store.
	storepb.StoreClient

	// LabelSets that each apply to some data exposed by the backing store.
	LabelSets() []labels.Labels

	// TimeRange returns minimum and maximum time range of data in the store.
	TimeRange() (mint int64, maxt int64)

	// SupportsSharding returns true if sharding is supported by the underlying store.
	SupportsSharding() bool

	// SendsSortedSeries returns true if the underlying store sends series sorded by
	// their labels.
	// The field can be used to indicate to the querier whether it needs to sort
	// received series before deduplication.
	SendsSortedSeries() bool

	// String returns the string representation of the store client.
	String() string

	// Addr returns address of the store client. If second parameter is true, the client
	// represents a local client (server-as-client) and has no remote address.
	Addr() (addr string, isLocalClient bool)
}

// ProxyStore implements the store API that proxies request to all given underlying stores.
type ProxyStore struct {
	logger         log.Logger
	stores         func() []Client
	component      component.StoreAPI
	selectorLabels labels.Labels
	buffers        sync.Pool

	responseTimeout   time.Duration
	metrics           *proxyStoreMetrics
	retrievalStrategy RetrievalStrategy
}

type proxyStoreMetrics struct {
	emptyStreamResponses prometheus.Counter
}

func newProxyStoreMetrics(reg prometheus.Registerer) *proxyStoreMetrics {
	var m proxyStoreMetrics

	m.emptyStreamResponses = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_proxy_store_empty_stream_responses_total",
		Help: "Total number of empty responses received.",
	})

	return &m
}

func RegisterStoreServer(storeSrv storepb.StoreServer) func(*grpc.Server) {
	return func(s *grpc.Server) {
		storepb.RegisterStoreServer(s, storeSrv)
	}
}

// NewProxyStore returns a new ProxyStore that uses the given clients that implements storeAPI to fan-in all series to the client.
// Note that there is no deduplication support. Deduplication should be done on the highest level (just before PromQL).
func NewProxyStore(
	logger log.Logger,
	reg prometheus.Registerer,
	stores func() []Client,
	component component.StoreAPI,
	selectorLabels labels.Labels,
	responseTimeout time.Duration,
	retrievalStrategy RetrievalStrategy,
) *ProxyStore {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	metrics := newProxyStoreMetrics(reg)
	s := &ProxyStore{
		logger:         logger,
		stores:         stores,
		component:      component,
		selectorLabels: selectorLabels,
		buffers: sync.Pool{New: func() interface{} {
			b := make([]byte, 0, initialBufSize)
			return &b
		}},
		responseTimeout:   responseTimeout,
		metrics:           metrics,
		retrievalStrategy: retrievalStrategy,
	}
	return s
}

// Info returns store information about the external labels this store have.
func (s *ProxyStore) Info(_ context.Context, _ *storepb.InfoRequest) (*storepb.InfoResponse, error) {
	res := &storepb.InfoResponse{
		StoreType: s.component.ToProto(),
		Labels:    labelpb.ZLabelsFromPromLabels(s.selectorLabels),
	}

	minTime := int64(math.MaxInt64)
	maxTime := int64(0)
	stores := s.stores()

	// Edge case: we have no data if there are no stores.
	if len(stores) == 0 {
		res.MaxTime = 0
		res.MinTime = 0

		return res, nil
	}

	for _, s := range stores {
		mint, maxt := s.TimeRange()
		if mint < minTime {
			minTime = mint
		}
		if maxt > maxTime {
			maxTime = maxt
		}
	}

	res.MaxTime = maxTime
	res.MinTime = minTime

	labelSets := make(map[uint64]labelpb.ZLabelSet, len(stores))
	for _, st := range stores {
		for _, lset := range st.LabelSets() {
			mergedLabelSet := labelpb.ExtendSortedLabels(lset, s.selectorLabels)
			labelSets[mergedLabelSet.Hash()] = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(mergedLabelSet)}
		}
	}

	res.LabelSets = make([]labelpb.ZLabelSet, 0, len(labelSets))
	for _, v := range labelSets {
		res.LabelSets = append(res.LabelSets, v)
	}

	// We always want to enforce announcing the subset of data that
	// selector-labels represents. If no label-sets are announced by the
	// store-proxy's discovered stores, then we still want to enforce
	// announcing this subset by announcing the selector as the label-set.
	if len(res.LabelSets) == 0 && len(res.Labels) > 0 {
		res.LabelSets = append(res.LabelSets, labelpb.ZLabelSet{Labels: res.Labels})
	}

	return res, nil
}

func (s *ProxyStore) LabelSet() []labelpb.ZLabelSet {
	stores := s.stores()
	if len(stores) == 0 {
		return []labelpb.ZLabelSet{}
	}

	mergedLabelSets := make(map[uint64]labelpb.ZLabelSet, len(stores))
	for _, st := range stores {
		for _, lset := range st.LabelSets() {
			mergedLabelSet := labelpb.ExtendSortedLabels(lset, s.selectorLabels)
			mergedLabelSets[mergedLabelSet.Hash()] = labelpb.ZLabelSet{Labels: labelpb.ZLabelsFromPromLabels(mergedLabelSet)}
		}
	}

	labelSets := make([]labelpb.ZLabelSet, 0, len(mergedLabelSets))
	for _, v := range mergedLabelSets {
		labelSets = append(labelSets, v)
	}

	// We always want to enforce announcing the subset of data that
	// selector-labels represents. If no label-sets are announced by the
	// store-proxy's discovered stores, then we still want to enforce
	// announcing this subset by announcing the selector as the label-set.
	selectorLabels := labelpb.ZLabelsFromPromLabels(s.selectorLabels)
	if len(labelSets) == 0 && len(selectorLabels) > 0 {
		labelSets = append(labelSets, labelpb.ZLabelSet{Labels: selectorLabels})
	}

	return labelSets
}
func (s *ProxyStore) TimeRange() (int64, int64) {
	stores := s.stores()
	if len(stores) == 0 {
		return math.MinInt64, math.MaxInt64
	}

	var minTime, maxTime int64 = math.MaxInt64, math.MinInt64
	for _, s := range stores {
		storeMinTime, storeMaxTime := s.TimeRange()
		if storeMinTime < minTime {
			minTime = storeMinTime
		}
		if storeMaxTime > maxTime {
			maxTime = storeMaxTime
		}
	}

	return minTime, maxTime
}

func (s *ProxyStore) Series(originalRequest *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	// TODO(bwplotka): This should be part of request logger, otherwise it does not make much sense. Also, could be
	// tiggered by tracing span to reduce cognitive load.
	reqLogger := log.With(s.logger, "component", "proxy", "request", originalRequest.String())

	match, matchers, err := matchesExternalLabels(originalRequest.Matchers, s.selectorLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, errors.New("no matchers specified (excluding selector labels)").Error())
	}
	storeMatchers, _ := storepb.PromMatchersToMatchers(matchers...) // Error would be returned by matchesExternalLabels, so skip check.

	storeDebugMsgs := []string{}
	r := &storepb.SeriesRequest{
		MinTime:                 originalRequest.MinTime,
		MaxTime:                 originalRequest.MaxTime,
		Matchers:                storeMatchers,
		Aggregates:              originalRequest.Aggregates,
		MaxResolutionWindow:     originalRequest.MaxResolutionWindow,
		SkipChunks:              originalRequest.SkipChunks,
		QueryHints:              originalRequest.QueryHints,
		PartialResponseDisabled: originalRequest.PartialResponseDisabled,
		PartialResponseStrategy: originalRequest.PartialResponseStrategy,
		ShardInfo:               originalRequest.ShardInfo,
	}

	stores := []Client{}
	for _, st := range s.stores() {
		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(srv.Context(), st, originalRequest.MinTime, originalRequest.MaxTime, matchers...); !ok {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s filtered out: %v", st, reason))
			continue
		}

		stores = append(stores, st)
	}

	if len(stores) == 0 {
		err := errors.New("No StoreAPIs matched for this query")
		level.Warn(reqLogger).Log("err", err, "stores", strings.Join(storeDebugMsgs, ";"))
		if sendErr := srv.Send(storepb.NewWarnSeriesResponse(err)); sendErr != nil {
			level.Error(reqLogger).Log("err", sendErr)
			return status.Error(codes.Unknown, errors.Wrap(sendErr, "send series response").Error())
		}
		return nil
	}

	storeResponses := make([]respSet, 0, len(stores))

	for _, st := range stores {
		st := st

		storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("store %s queried", st))

		respSet, err := newAsyncRespSet(srv.Context(), st, r, s.responseTimeout, s.retrievalStrategy, st.SupportsSharding(), &s.buffers, r.ShardInfo, reqLogger, s.metrics.emptyStreamResponses)
		if err != nil {
			level.Error(reqLogger).Log("err", err)

			if !r.PartialResponseDisabled || r.PartialResponseStrategy == storepb.PartialResponseStrategy_WARN {
				if err := srv.Send(storepb.NewWarnSeriesResponse(err)); err != nil {
					return err
				}
				continue
			} else {
				return err
			}
		}

		storeResponses = append(storeResponses, respSet)
		defer respSet.Close()
	}

	level.Debug(reqLogger).Log("msg", "Series: started fanout streams", "status", strings.Join(storeDebugMsgs, ";"))

	respHeap := NewDedupResponseHeap(NewProxyResponseHeap(storeResponses...))
	for respHeap.Next() {
		resp := respHeap.At()

		if resp.GetWarning() != "" && (r.PartialResponseDisabled || r.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT) {
			return status.Error(codes.Aborted, resp.GetWarning())
		}

		if err := srv.Send(resp); err != nil {
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	return nil
}

// storeMatches returns boolean if the given store may hold data for the given label matchers, time ranges and debug store matches gathered from context.
// It also produces tracing span.
func storeMatches(ctx context.Context, s Client, mint, maxt int64, matchers ...*labels.Matcher) (ok bool, reason string) {
	span, ctx := tracing.StartSpan(ctx, "store_matches")
	defer span.Finish()

	var storeDebugMatcher [][]*labels.Matcher
	if ctxVal := ctx.Value(StoreMatcherKey); ctxVal != nil {
		if value, ok := ctxVal.([][]*labels.Matcher); ok {
			storeDebugMatcher = value
		}
	}

	storeMinTime, storeMaxTime := s.TimeRange()
	if mint > storeMaxTime || maxt < storeMinTime {
		return false, fmt.Sprintf("does not have data within this time period: [%v,%v]. Store time ranges: [%v,%v]", mint, maxt, storeMinTime, storeMaxTime)
	}

	if ok, reason := storeMatchDebugMetadata(s, storeDebugMatcher); !ok {
		return false, reason
	}

	extLset := s.LabelSets()
	if !labelSetsMatch(matchers, extLset...) {
		return false, fmt.Sprintf("external labels %v does not match request label matchers: %v", extLset, matchers)
	}
	return true, ""
}

// storeMatchDebugMetadata return true if the store's address match the storeDebugMatchers.
func storeMatchDebugMetadata(s Client, storeDebugMatchers [][]*labels.Matcher) (ok bool, reason string) {
	if len(storeDebugMatchers) == 0 {
		return true, ""
	}

	addr, isLocal := s.Addr()
	if isLocal {
		return false, "the store is not remote, cannot match __address__"
	}

	match := false
	for _, sm := range storeDebugMatchers {
		match = match || labelSetsMatch(sm, labels.FromStrings("__address__", addr))
	}
	if !match {
		return false, fmt.Sprintf("__address__ %v does not match debug store metadata matchers: %v", addr, storeDebugMatchers)
	}
	return true, ""
}

// labelSetsMatch returns false if all label-set do not match the matchers (aka: OR is between all label-sets).
func labelSetsMatch(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		notMatched := false
		for _, m := range matchers {
			if lv := ls.Get(m.Name); lv != "" && !m.Matches(lv) {
				notMatched = true
				break
			}
		}
		if !notMatched {
			return true
		}
	}
	return false
}

// LabelNames returns all known label names.
func (s *ProxyStore) LabelNames(ctx context.Context, r *storepb.LabelNamesRequest) (
	*storepb.LabelNamesResponse, error,
) {
	var (
		warnings       []string
		names          [][]string
		mtx            sync.Mutex
		g, gctx        = errgroup.WithContext(ctx)
		storeDebugMsgs []string
	)

	for _, st := range s.stores() {
		st := st

		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(gctx, st, r.Start, r.End); !ok {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to %v", st, reason))
			continue
		}
		storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s queried", st))

		g.Go(func() error {
			resp, err := st.LabelNames(gctx, &storepb.LabelNamesRequest{
				PartialResponseDisabled: r.PartialResponseDisabled,
				Start:                   r.Start,
				End:                     r.End,
				Matchers:                r.Matchers,
			})
			if err != nil {
				err = errors.Wrapf(err, "fetch label names from store %s", st)
				if r.PartialResponseDisabled {
					return err
				}

				mtx.Lock()
				warnings = append(warnings, err.Error())
				mtx.Unlock()
				return nil
			}

			mtx.Lock()
			warnings = append(warnings, resp.Warnings...)
			names = append(names, resp.Names)
			mtx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	level.Debug(s.logger).Log("msg", strings.Join(storeDebugMsgs, ";"))
	return &storepb.LabelNamesResponse{
		Names:    strutil.MergeUnsortedSlices(names...),
		Warnings: warnings,
	}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *ProxyStore) LabelValues(ctx context.Context, r *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	var (
		warnings       []string
		all            [][]string
		mtx            sync.Mutex
		g, gctx        = errgroup.WithContext(ctx)
		storeDebugMsgs []string
		span           opentracing.Span
	)

	for _, st := range s.stores() {
		st := st

		storeAddr, isLocalStore := st.Addr()
		storeID := labelpb.PromLabelSetsToString(st.LabelSets())
		if storeID == "" {
			storeID = "Store Gateway"
		}
		span, gctx = tracing.StartSpan(gctx, "proxy.label_values", tracing.Tags{
			"store.id":       storeID,
			"store.addr":     storeAddr,
			"store.is_local": isLocalStore,
		})
		defer span.Finish()

		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(gctx, st, r.Start, r.End); !ok {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to %v", st, reason))
			continue
		}
		storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s queried", st))

		g.Go(func() error {
			resp, err := st.LabelValues(gctx, &storepb.LabelValuesRequest{
				Label:                   r.Label,
				PartialResponseDisabled: r.PartialResponseDisabled,
				Start:                   r.Start,
				End:                     r.End,
				Matchers:                r.Matchers,
			})
			if err != nil {
				msg := "fetch label values from store %s"
				err = errors.Wrapf(err, msg, st)
				if r.PartialResponseDisabled {
					return err
				}

				mtx.Lock()
				warnings = append(warnings, errors.Wrapf(err, msg, st).Error())
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

	level.Debug(s.logger).Log("msg", strings.Join(storeDebugMsgs, ";"))
	return &storepb.LabelValuesResponse{
		Values:   strutil.MergeUnsortedSlices(all...),
		Warnings: warnings,
	}, nil
}
