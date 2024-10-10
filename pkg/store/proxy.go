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
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/strutil"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"github.com/thanos-io/thanos/pkg/tracing"
)

type ctxKey int

// UninitializedTSDBTime is the TSDB start time of an uninitialized TSDB instance.
const UninitializedTSDBTime = math.MaxInt64

// StoreMatcherKey is the context key for the store's allow list.
const StoreMatcherKey = ctxKey(0)

// ErrorNoStoresMatched is returned if the query does not match any data.
// This can happen with Query servers trees and external labels.
var ErrorNoStoresMatched = errors.New("No StoreAPIs matched for this query")

// Client holds meta information about a store.
type Client interface {
	// StoreClient to access the store.
	storepb.StoreClient

	// LabelSets that each apply to some data exposed by the backing store.
	LabelSets() []labels.Labels

	// TimeRange returns minimum and maximum time range of data in the store.
	TimeRange() (mint int64, maxt int64)

	// TSDBInfos returns metadata about each TSDB backed by the client.
	TSDBInfos() []infopb.TSDBInfo

	// SupportsSharding returns true if sharding is supported by the underlying store.
	SupportsSharding() bool

	// SupportsWithoutReplicaLabels returns true if trimming replica labels
	// and sorted response is supported by the underlying store.
	SupportsWithoutReplicaLabels() bool

	// String returns the string representation of the store client.
	String() string

	// Addr returns address of the store client. If second parameter is true, the client
	// represents a local client (server-as-client) and has no remote address.
	Addr() (addr string, isLocalClient bool)

	// Matches returns true if provided label matchers are allowed in the store.
	Matches(matches []*labels.Matcher) bool
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
	debugLogging      bool
	tsdbSelector      *TSDBSelector
	enableDedup       bool
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

func RegisterStoreServer(storeSrv storepb.StoreServer, logger log.Logger) func(*grpc.Server) {
	return func(s *grpc.Server) {
		storepb.RegisterStoreServer(s, NewRecoverableStoreServer(logger, storeSrv))
	}
}

// BucketStoreOption are functions that configure BucketStore.
type ProxyStoreOption func(s *ProxyStore)

// WithProxyStoreDebugLogging toggles debug logging.
func WithProxyStoreDebugLogging(enable bool) ProxyStoreOption {
	return func(s *ProxyStore) {
		s.debugLogging = enable
	}
}

// WithTSDBSelector sets the TSDB selector for the proxy.
func WithTSDBSelector(selector *TSDBSelector) ProxyStoreOption {
	return func(s *ProxyStore) {
		s.tsdbSelector = selector
	}
}

// WithoutDedup disabled chunk deduplication when streaming series.
func WithoutDedup() ProxyStoreOption {
	return func(s *ProxyStore) {
		s.enableDedup = false
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
	options ...ProxyStoreOption,
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
		tsdbSelector:      DefaultSelector,
		enableDedup:       true,
	}

	for _, option := range options {
		option(s)
	}

	return s
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

func (s *ProxyStore) TSDBInfos() []infopb.TSDBInfo {
	infos := make([]infopb.TSDBInfo, 0)
	for _, st := range s.stores() {
		matches, _ := s.tsdbSelector.MatchLabelSets(st.LabelSets()...)
		if !matches {
			continue
		}
		infos = append(infos, st.TSDBInfos()...)
	}
	return infos
}

func (s *ProxyStore) Series(originalRequest *storepb.SeriesRequest, srv storepb.Store_SeriesServer) error {
	// TODO(bwplotka): This should be part of request logger, otherwise it does not make much sense. Also, could be
	// triggered by tracing span to reduce cognitive load.
	reqLogger := log.With(s.logger, "component", "proxy")
	if s.debugLogging {
		reqLogger = log.With(reqLogger, "request", originalRequest.String())
	}

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

	// We may arrive here either via the promql engine
	// or as a result of a grpc call in layered queries
	ctx := srv.Context()
	tenant, foundTenant := tenancy.GetTenantFromGRPCMetadata(ctx)
	if !foundTenant {
		if ctx.Value(tenancy.TenantKey) != nil {
			tenant = ctx.Value(tenancy.TenantKey).(string)
		}
	}

	ctx = metadata.AppendToOutgoingContext(ctx, tenancy.DefaultTenantHeader, tenant)
	level.Debug(s.logger).Log("msg", "Tenant info in Series()", "tenant", tenant)

	stores, storeLabelSets, storeDebugMsgs := s.matchingStores(ctx, originalRequest.MinTime, originalRequest.MaxTime, matchers)
	if len(stores) == 0 {
		level.Debug(reqLogger).Log("err", ErrorNoStoresMatched, "stores", strings.Join(storeDebugMsgs, ";"))
		return nil
	}

	storeMatchers, _ := storepb.PromMatchersToMatchers(matchers...) // Error would be returned by matchesExternalLabels, so skip check.
	r := &storepb.SeriesRequest{
		MinTime:                 originalRequest.MinTime,
		MaxTime:                 originalRequest.MaxTime,
		Limit:                   originalRequest.Limit,
		Matchers:                append(storeMatchers, MatchersForLabelSets(storeLabelSets)...),
		Aggregates:              originalRequest.Aggregates,
		MaxResolutionWindow:     originalRequest.MaxResolutionWindow,
		SkipChunks:              originalRequest.SkipChunks,
		QueryHints:              originalRequest.QueryHints,
		PartialResponseDisabled: originalRequest.PartialResponseDisabled,
		PartialResponseStrategy: originalRequest.PartialResponseStrategy,
		ShardInfo:               originalRequest.ShardInfo,
		WithoutReplicaLabels:    originalRequest.WithoutReplicaLabels,
	}

	storeResponses := make([]respSet, 0, len(stores))
	for _, st := range stores {
		st := st

		respSet, err := newAsyncRespSet(ctx, st, r, s.responseTimeout, s.retrievalStrategy, &s.buffers, r.ShardInfo, reqLogger, s.metrics.emptyStreamResponses)
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

	var respHeap seriesStream = NewProxyResponseLoserTree(storeResponses...)
	if s.enableDedup {
		respHeap = NewResponseDeduplicator(respHeap)
	}

	i := 0
	for respHeap.Next() {
		i++
		if r.Limit > 0 && i > int(r.Limit) {
			break
		}
		resp := respHeap.At()

		if resp.GetWarning() != "" && (r.PartialResponseDisabled || r.PartialResponseStrategy == storepb.PartialResponseStrategy_ABORT) {
			return status.Error(codes.Aborted, resp.GetWarning())
		}

		if err := srv.Send(resp); err != nil {
			level.Error(reqLogger).Log("msg", "failed to stream response", "error", err)
			return status.Error(codes.Unknown, errors.Wrap(err, "send series response").Error())
		}
	}

	return nil
}

// LabelNames returns all known label names.
func (s *ProxyStore) LabelNames(ctx context.Context, originalRequest *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	// TODO(bwplotka): This should be part of request logger, otherwise it does not make much sense. Also, could be
	// triggered by tracing span to reduce cognitive load.
	reqLogger := log.With(s.logger, "component", "proxy")
	if s.debugLogging {
		reqLogger = log.With(reqLogger, "request", originalRequest.String())
	}
	match, matchers, err := matchesExternalLabels(originalRequest.Matchers, s.selectorLabels)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelNamesResponse{}, nil
	}

	// We may arrive here either via the promql engine
	// or as a result of a grpc call in layered queries
	tenant, foundTenant := tenancy.GetTenantFromGRPCMetadata(ctx)
	if !foundTenant {
		level.Debug(reqLogger).Log("msg", "using tenant from context instead of metadata")
		if ctx.Value(tenancy.TenantKey) != nil {
			tenant = ctx.Value(tenancy.TenantKey).(string)
		}
	}

	ctx = metadata.AppendToOutgoingContext(ctx, tenancy.DefaultTenantHeader, tenant)
	level.Debug(s.logger).Log("msg", "Tenant info in LabelNames()", "tenant", tenant)

	stores, storeLabelSets, storeDebugMsgs := s.matchingStores(ctx, originalRequest.Start, originalRequest.End, matchers)
	if len(stores) == 0 {
		level.Debug(reqLogger).Log("err", ErrorNoStoresMatched, "stores", strings.Join(storeDebugMsgs, ";"))
		return &storepb.LabelNamesResponse{}, nil
	}
	storeMatchers, _ := storepb.PromMatchersToMatchers(matchers...) // Error would be returned by matchesExternalLabels, so skip check.
	r := &storepb.LabelNamesRequest{
		PartialResponseDisabled: originalRequest.PartialResponseDisabled,
		Start:                   originalRequest.Start,
		End:                     originalRequest.End,
		Matchers:                append(storeMatchers, MatchersForLabelSets(storeLabelSets)...),
		WithoutReplicaLabels:    originalRequest.WithoutReplicaLabels,
		Hints:                   originalRequest.Hints,
	}

	var (
		warnings []string
		names    [][]string
		mtx      sync.Mutex
		g, gctx  = errgroup.WithContext(ctx)
	)
	for _, st := range stores {
		st := st

		storeID, storeAddr, isLocalStore := storeInfo(st)
		g.Go(func() error {
			span, spanCtx := tracing.StartSpan(gctx, "proxy.label_names", tracing.Tags{
				"store.id":       storeID,
				"store.addr":     storeAddr,
				"store.is_local": isLocalStore,
			})
			defer span.Finish()
			resp, err := st.LabelNames(spanCtx, r)
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
	level.Debug(reqLogger).Log("msg", "LabelNames: started fanout streams", "status", strings.Join(storeDebugMsgs, ";"))

	if err := g.Wait(); err != nil {
		return nil, err
	}

	result := strutil.MergeUnsortedSlices(int(originalRequest.Limit), names...)

	return &storepb.LabelNamesResponse{
		Names:    result,
		Warnings: warnings,
	}, nil
}

// LabelValues returns all known label values for a given label name.
func (s *ProxyStore) LabelValues(ctx context.Context, originalRequest *storepb.LabelValuesRequest) (
	*storepb.LabelValuesResponse, error,
) {
	// TODO(bwplotka): This should be part of request logger, otherwise it does not make much sense. Also, could be
	// triggered by tracing span to reduce cognitive load.
	reqLogger := log.With(s.logger, "component", "proxy")
	if s.debugLogging {
		reqLogger = log.With(reqLogger, "request", originalRequest.String())
	}

	if originalRequest.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}

	match, matchers, err := matchesExternalLabels(originalRequest.Matchers, s.selectorLabels)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{}, nil
	}

	// We may arrive here either via the promql engine
	// or as a result of a grpc call in layered queries
	tenant, foundTenant := tenancy.GetTenantFromGRPCMetadata(ctx)
	if !foundTenant {
		level.Debug(reqLogger).Log("msg", "using tenant from context instead of metadata")
		if ctx.Value(tenancy.TenantKey) != nil {
			tenant = ctx.Value(tenancy.TenantKey).(string)
		}
	}

	ctx = metadata.AppendToOutgoingContext(ctx, tenancy.DefaultTenantHeader, tenant)
	level.Debug(reqLogger).Log("msg", "Tenant info in LabelValues()", "tenant", tenant)

	stores, storeLabelSets, storeDebugMsgs := s.matchingStores(ctx, originalRequest.Start, originalRequest.End, matchers)
	if len(stores) == 0 {
		level.Debug(reqLogger).Log("err", ErrorNoStoresMatched, "stores", strings.Join(storeDebugMsgs, ";"))
		return &storepb.LabelValuesResponse{}, nil
	}
	storeMatchers, _ := storepb.PromMatchersToMatchers(matchers...) // Error would be returned by matchesExternalLabels, so skip check.
	r := &storepb.LabelValuesRequest{
		Label:                   originalRequest.Label,
		PartialResponseDisabled: originalRequest.PartialResponseDisabled,
		Start:                   originalRequest.Start,
		End:                     originalRequest.End,
		Matchers:                append(storeMatchers, MatchersForLabelSets(storeLabelSets)...),
		WithoutReplicaLabels:    originalRequest.WithoutReplicaLabels,
		Limit:                   originalRequest.Limit,
	}

	var (
		warnings []string
		all      [][]string
		mtx      sync.Mutex
		g, gctx  = errgroup.WithContext(ctx)
	)
	for _, st := range stores {
		st := st

		storeID, storeAddr, isLocalStore := storeInfo(st)
		g.Go(func() error {
			span, spanCtx := tracing.StartSpan(gctx, "proxy.label_values", tracing.Tags{
				"store.id":       storeID,
				"store.addr":     storeAddr,
				"store.is_local": isLocalStore,
			})
			defer span.Finish()

			resp, err := st.LabelValues(spanCtx, r)
			if err != nil {
				err = errors.Wrapf(err, "fetch label values from store %s", st)
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
			all = append(all, resp.Values)
			mtx.Unlock()

			return nil
		})
	}
	level.Debug(reqLogger).Log("msg", "LabelValues: started fanout streams", "status", strings.Join(storeDebugMsgs, ";"))

	if err := g.Wait(); err != nil {
		return nil, err
	}

	vals := strutil.MergeUnsortedSlices(int(originalRequest.Limit), all...)

	return &storepb.LabelValuesResponse{
		Values:   vals,
		Warnings: warnings,
	}, nil
}

func storeInfo(st Client) (storeID string, storeAddr string, isLocalStore bool) {
	storeAddr, isLocalStore = st.Addr()
	storeID = labelpb.PromLabelSetsToString(st.LabelSets())
	if storeID == "" {
		storeID = "Store Gateway"
	}
	return storeID, storeAddr, isLocalStore
}

// TODO: consider moving the following functions into something like "pkg/pruneutils" since it is also used for exemplars.

func (s *ProxyStore) matchingStores(ctx context.Context, minTime, maxTime int64, matchers []*labels.Matcher) ([]Client, []labels.Labels, []string) {
	var (
		stores         []Client
		storeLabelSets []labels.Labels
		storeDebugMsgs []string
	)
	for _, st := range s.stores() {
		// We might be able to skip the store if its meta information indicates it cannot have series matching our query.
		if ok, reason := storeMatches(ctx, s.debugLogging, st, minTime, maxTime, matchers...); !ok {
			if s.debugLogging {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to: %v", st, reason))
			}
			continue
		}
		matches, extraMatchers := s.tsdbSelector.MatchLabelSets(st.LabelSets()...)
		if !matches {
			if s.debugLogging {
				storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s filtered out due to: %v", st, "tsdb selector"))
			}
			continue
		}
		storeLabelSets = append(storeLabelSets, extraMatchers...)

		stores = append(stores, st)
		if s.debugLogging {
			storeDebugMsgs = append(storeDebugMsgs, fmt.Sprintf("Store %s queried", st))
		}
	}

	return stores, storeLabelSets, storeDebugMsgs
}

// storeMatches returns boolean if the given store may hold data for the given label matchers, time ranges and debug store matches gathered from context.
func storeMatches(ctx context.Context, debugLogging bool, s Client, mint, maxt int64, matchers ...*labels.Matcher) (ok bool, reason string) {
	var storeDebugMatcher [][]*labels.Matcher
	if ctxVal := ctx.Value(StoreMatcherKey); ctxVal != nil {
		if value, ok := ctxVal.([][]*labels.Matcher); ok {
			storeDebugMatcher = value
		}
	}

	storeMinTime, storeMaxTime := s.TimeRange()
	if mint > storeMaxTime || maxt < storeMinTime {
		const s string = "does not have data within this time period"
		if debugLogging {
			return false, fmt.Sprintf("%s: [%v,%v]. Store time ranges: [%v,%v]", s, mint, maxt, storeMinTime, storeMaxTime)
		}
		return false, s
	}

	if ok, reason := storeMatchDebugMetadata(s, debugLogging, storeDebugMatcher); !ok {
		return false, reason
	}

	extLset := s.LabelSets()
	if !LabelSetsMatch(matchers, extLset...) {
		const s string = "external labels does not match request label matchers"
		if debugLogging {
			return false, fmt.Sprintf("external labels %v does not match request label matchers: %v", extLset, matchers)
		}
		return false, s
	}

	if !s.Matches(matchers) {
		return false, fmt.Sprintf("store does not match filter for matchers: %v", matchers)
	}

	return true, ""
}

// storeMatchDebugMetadata return true if the store's address match the storeDebugMatchers.
func storeMatchDebugMetadata(s Client, debugLogging bool, storeDebugMatchers [][]*labels.Matcher) (ok bool, reason string) {
	if len(storeDebugMatchers) == 0 {
		return true, ""
	}

	addr, isLocal := s.Addr()
	if isLocal {
		return false, "the store is not remote, cannot match __address__"
	}

	match := false
	for _, sm := range storeDebugMatchers {
		match = match || LabelSetsMatch(sm, labels.FromStrings("__address__", addr))
	}
	if !match {
		const s string = "__address__ does not match debug store metadata matchers"
		if debugLogging {
			return false, fmt.Sprintf("__address__ %v does not match debug store metadata matchers: %v", addr, storeDebugMatchers)
		}
		return false, s
	}
	return true, ""
}

// LabelSetsMatch returns false if all label-set do not match the matchers (aka: OR is between all label-sets).
func LabelSetsMatch(matchers []*labels.Matcher, lset ...labels.Labels) bool {
	if len(lset) == 0 {
		return true
	}

	for _, ls := range lset {
		notMatched := false
		for _, m := range matchers {
			if lv := ls.Get(m.Name); ls.Has(m.Name) && !m.Matches(lv) {
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
