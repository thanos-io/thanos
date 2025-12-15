// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/status/statuspb"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

const (
	unhealthyEndpointMessage  = "removing endpoint because it's unhealthy or does not exist"
	noMetadataEndpointMessage = "cannot obtain metadata: neither info nor store client found"
)

type queryConnMetricLabel string

const (
	ExternalLabels queryConnMetricLabel = "external_labels"
	StoreType      queryConnMetricLabel = "store_type"
	IPPort         queryConnMetricLabel = "ip_port"
)

type GRPCEndpointSpec struct {
	addr           string
	isStrictStatic bool
	dialOpts       []grpc.DialOption
}

const externalLabelLimit = 1000

// NewGRPCEndpointSpec creates gRPC endpoint spec.
// It uses InfoAPI to get Metadata.
func NewGRPCEndpointSpec(addr string, isStrictStatic bool, dialOpts ...grpc.DialOption) *GRPCEndpointSpec {
	return &GRPCEndpointSpec{
		addr:           addr,
		isStrictStatic: isStrictStatic,
		dialOpts:       dialOpts,
	}
}

func (es *GRPCEndpointSpec) Addr() string {
	// API address should not change between state changes.
	return es.addr
}

// Metadata method for gRPC endpoint tries to call InfoAPI exposed by Thanos components until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (es *endpointRef) Metadata(ctx context.Context, infoClient infopb.InfoClient, storeClient storepb.StoreClient) (*endpointMetadata, error) {
	if infoClient != nil {
		resp, err := infoClient.Info(ctx, &infopb.InfoRequest{}, grpc.WaitForReady(true))
		if err != nil {
			if status.Convert(err).Code() != codes.Unimplemented {
				return nil, err
			}
		} else {
			return &endpointMetadata{resp}, nil
		}
	}
	return nil, errors.New(noMetadataEndpointMessage)
}

// stringError forces the error to be a string
// when marshaled into a JSON.
type stringError struct {
	originalErr error
}

// MarshalJSON marshals the error into a string form.
func (e *stringError) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.originalErr.Error())
}

// Error returns the original underlying error.
func (e *stringError) Error() string {
	return e.originalErr.Error()
}

type EndpointStatus struct {
	Name          string              `json:"name"`
	LastCheck     time.Time           `json:"lastCheck"`
	LastError     *stringError        `json:"lastError"`
	LabelSets     []labels.Labels     `json:"labelSets"`
	ComponentType component.Component `json:"-"`
	MinTime       int64               `json:"minTime"`
	MaxTime       int64               `json:"maxTime"`
}

// endpointSetNodeCollector is a metric collector reporting the number of available storeAPIs for Querier.
// A Collector is required as we want atomic updates for all 'thanos_store_nodes_grpc_connections' series.
// TODO(hitanshu-mehta) Currently,only collecting metrics of storeEndpoints. Make this struct generic.
type endpointSetNodeCollector struct {
	mtx        sync.Mutex
	storeNodes endpointStats

	logger          log.Logger
	connectionsDesc *prometheus.Desc
	labels          []string
	labelsMap       map[string]struct{}

	hasherPool sync.Pool
}

func newEndpointSetNodeCollector(logger log.Logger, labels ...string) *endpointSetNodeCollector {
	if len(labels) == 0 {
		labels = []string{string(ExternalLabels), string(StoreType)}
	}

	labelsMap := make(map[string]struct{})
	for _, lbl := range labels {
		labelsMap[lbl] = struct{}{}
	}
	return &endpointSetNodeCollector{
		logger:     logger,
		storeNodes: endpointStats{},
		connectionsDesc: prometheus.NewDesc(
			"thanos_store_nodes_grpc_connections",
			"Number of gRPC connection to Store APIs. Opened connection means healthy store APIs available for Querier.",
			labels, nil,
		),
		labels:    labels,
		labelsMap: labelsMap,
		hasherPool: sync.Pool{
			New: func() any {
				return xxhash.New()
			},
		},
	}
}

// truncateExtLabels truncates the stringify external labels with the format of {labels..}.
func truncateExtLabels(s string, threshold int) string {
	if len(s) > threshold {
		for cut := 1; cut < 4; cut++ {
			for cap := 1; cap < 4; cap++ {
				if utf8.ValidString(s[threshold-cut-cap : threshold-cut]) {
					return fmt.Sprintf("%s}", s[:threshold-cut])
				}
			}
		}
	}
	return s
}
func (c *endpointSetNodeCollector) Update(stats endpointStats) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.storeNodes = stats
}

func (c *endpointSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsDesc
}

func (c *endpointSetNodeCollector) hash(e endpointStat) uint64 {
	h := c.hasherPool.Get().(*xxhash.Digest)
	defer func() {
		h.Reset()
		c.hasherPool.Put(h)
	}()

	if _, ok := c.labelsMap[string(IPPort)]; ok {
		_, _ = h.Write([]byte(e.ip))
	}
	if _, ok := c.labelsMap[string(ExternalLabels)]; ok {
		_, _ = h.Write([]byte(e.extLset))
	}
	if _, ok := c.labelsMap[string(StoreType)]; ok {
		_, _ = h.Write([]byte(e.component))
	}

	return h.Sum64()
}

func (c *endpointSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var occurrences = make(map[uint64]int)
	for _, e := range c.storeNodes {
		h := c.hash(e)
		occurrences[h]++
	}

	for _, n := range c.storeNodes {
		h := c.hash(n)
		lbls := make([]string, 0, len(c.labels))
		for _, lbl := range c.labels {
			switch lbl {
			case string(ExternalLabels):
				lbls = append(lbls, n.extLset)
			case string(StoreType):
				lbls = append(lbls, n.component)
			case string(IPPort):
				lbls = append(lbls, n.ip)
			}
		}

		select {
		case ch <- prometheus.MustNewConstMetric(c.connectionsDesc, prometheus.GaugeValue, float64(occurrences[h]), lbls...):
		case <-time.After(1 * time.Second):
			level.Warn(c.logger).Log("msg", "failed to collect endpointset metrics", "timeout", 1*time.Second)
			return
		}
	}
}

// EndpointSet maintains a set of active Thanos endpoints. It is backed up by Endpoint Specifications that are dynamically fetched on
// every Update() call.
type EndpointSet struct {
	now    nowFunc
	logger log.Logger

	// Endpoint specifications can change dynamically. If some component is missing from the list, we assume it is no longer
	// accessible and we close gRPC client for it, unless it is strict.
	endpointSpecs            func() map[string]*GRPCEndpointSpec
	endpointInfoTimeout      time.Duration
	unhealthyEndpointTimeout time.Duration
	gcTimeout                time.Duration

	updateMtx sync.Mutex

	endpointsMtx    sync.RWMutex
	endpoints       map[string]*endpointRef
	endpointsMetric *endpointSetNodeCollector

	// Track if the first update has completed
	firstUpdateOnce sync.Once
	firstUpdateChan chan struct{}
}

// nowFunc is a function that returns time.Time.
// Test code can inject a function through which
// time can be modified before updating the EndpointSet.
// Production code can use time.Time.
type nowFunc func() time.Time

// NewEndpointSet returns a new set of Thanos APIs.
func NewEndpointSet(
	now nowFunc,
	logger log.Logger,
	reg prometheus.Registerer,
	endpointSpecs func() []*GRPCEndpointSpec,
	unhealthyEndpointTimeout time.Duration,
	endpointInfoTimeout time.Duration,
	queryTimeout time.Duration,
	endpointMetricLabels ...string,
) *EndpointSet {
	endpointsMetric := newEndpointSetNodeCollector(logger, endpointMetricLabels...)
	if reg != nil {
		reg.MustRegister(endpointsMetric)
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}

	if endpointSpecs == nil {
		endpointSpecs = func() []*GRPCEndpointSpec { return nil }
	}

	return &EndpointSet{
		now:                      now,
		logger:                   log.With(logger, "component", "endpointset"),
		endpointsMetric:          endpointsMetric,
		endpointInfoTimeout:      endpointInfoTimeout,
		gcTimeout:                max(queryTimeout, endpointInfoTimeout, unhealthyEndpointTimeout),
		unhealthyEndpointTimeout: unhealthyEndpointTimeout,
		endpointSpecs: func() map[string]*GRPCEndpointSpec {
			res := make(map[string]*GRPCEndpointSpec)
			for _, s := range endpointSpecs() {
				res[s.addr] = s
			}
			return res
		},
		endpoints:       make(map[string]*endpointRef),
		firstUpdateChan: make(chan struct{}),
	}
}

// WaitForFirstUpdate blocks until the first endpoint update has completed.
// It returns immediately if the first update has already been done.
// The context can be used to set a timeout for waiting.
func (e *EndpointSet) WaitForFirstUpdate(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-e.firstUpdateChan:
		if !ok {
			// Channel is closed, first update already completed
			return nil
		}
		return nil
	}
}

// Update updates the endpoint set. It fetches current list of endpoint specs from function and updates the fresh metadata
// from all endpoints. Keeps around statically defined nodes that were defined with the strict mode.
func (e *EndpointSet) Update(ctx context.Context) {
	e.updateMtx.Lock()
	defer e.updateMtx.Unlock()
	level.Debug(e.logger).Log("msg", "starting to update API endpoints", "cachedEndpoints", len(e.endpoints))

	var (
		newRefs      = make(map[string]*endpointRef)
		existingRefs = make(map[string]*endpointRef)
		staleRefs    = make(map[string]*endpointRef)

		wg sync.WaitGroup
		mu sync.Mutex
	)

	for _, spec := range e.endpointSpecs() {

		if er, existingRef := e.endpoints[spec.Addr()]; existingRef {
			wg.Add(1)
			go func(spec *GRPCEndpointSpec) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, e.endpointInfoTimeout)
				defer cancel()
				e.updateEndpoint(ctx, spec, er)

				mu.Lock()
				defer mu.Unlock()
				existingRefs[spec.Addr()] = er
			}(spec)

			continue
		}

		wg.Add(1)
		go func(spec *GRPCEndpointSpec) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(ctx, e.endpointInfoTimeout)
			defer cancel()
			newRef, err := e.newEndpointRef(spec)
			if err != nil {
				level.Warn(e.logger).Log("msg", "new endpoint creation failed", "err", err, "address", spec.Addr())
				return
			}

			e.updateEndpoint(ctx, spec, newRef)
			if !newRef.isQueryable() {
				newRef.Close(e.gcTimeout)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			newRefs[spec.Addr()] = newRef
		}(spec)
	}
	wg.Wait()

	timedOutRefs := e.getTimedOutRefs()
	e.endpointsMtx.RLock()
	for addr, er := range e.endpoints {
		_, isNew := newRefs[addr]
		_, isExisting := existingRefs[addr]
		_, isTimedOut := timedOutRefs[addr]
		if !isNew && !isExisting || isTimedOut {
			staleRefs[addr] = er
		}
	}
	e.endpointsMtx.RUnlock()

	e.endpointsMtx.Lock()
	defer e.endpointsMtx.Unlock()
	for addr, er := range newRefs {
		extLset := labelpb.PromLabelSetsToString(er.LabelSets())
		level.Info(e.logger).Log("msg", fmt.Sprintf("adding new %v with %+v", er.ComponentType(), er.apisPresent()), "address", addr, "extLset", extLset)
		e.endpoints[addr] = er
	}
	for addr, er := range staleRefs {
		level.Info(er.logger).Log("msg", unhealthyEndpointMessage, "address", er.addr, "extLset", labelpb.PromLabelSetsToString(er.LabelSets()))
		er.Close(e.gcTimeout)
		delete(e.endpoints, addr)
	}
	level.Debug(e.logger).Log("msg", "updated endpoints", "activeEndpoints", len(e.endpoints))

	nodes := make(map[string]map[string]int, len(component.All))
	for _, comp := range component.All {
		nodes[comp.String()] = map[string]int{}
	}

	// Update stats.
	stats := newEndpointAPIStats()

	endpointIPs := make([]string, 0, len(e.endpoints))
	for addr := range e.endpoints {
		endpointIPs = append(endpointIPs, addr)
	}
	sort.Strings(endpointIPs)
	for _, addr := range endpointIPs {
		er := e.endpoints[addr]
		if !er.isQueryable() {
			continue
		}

		extLset := labelpb.PromLabelSetsToString(er.LabelSets())

		// All producers that expose StoreAPI should have unique external labels. Check all which connect to our Querier.
		if er.HasStoreAPI() && (er.ComponentType() == component.Sidecar || er.ComponentType() == component.Rule) &&
			nodes[component.Sidecar.String()][extLset]+nodes[component.Rule.String()][extLset] > 0 {

			level.Warn(e.logger).Log("msg", "found duplicate storeEndpoints producer (sidecar or ruler). This is not advised as it will malform data in in the same bucket",
				"address", addr, "extLset", extLset, "duplicates", fmt.Sprintf("%v", nodes[component.Sidecar.String()][extLset]+nodes[component.Rule.String()][extLset]+1))
		}
		nodes[er.ComponentType().String()][extLset]++

		stats = stats.append(er.addr, extLset, er.ComponentType().String())
	}

	e.endpointsMetric.Update(stats)

	// Signal that the first update has completed
	e.firstUpdateOnce.Do(func() {
		close(e.firstUpdateChan)
	})
}

func (e *EndpointSet) updateEndpoint(ctx context.Context, spec *GRPCEndpointSpec, er *endpointRef) {
	metadata, err := er.Metadata(ctx, infopb.NewInfoClient(er.cc), storepb.NewStoreClient(er.cc))
	if err != nil {
		level.Warn(e.logger).Log("msg", "update of endpoint failed", "err", errors.Wrap(err, "getting metadata"), "address", spec.Addr())
	}
	er.update(e.now, metadata, err)
}

// getTimedOutRefs returns unhealthy endpoints for which the last
// successful health check is older than the unhealthyEndpointTimeout.
// Strict endpoints are never considered as timed out.
func (e *EndpointSet) getTimedOutRefs() map[string]*endpointRef {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()
	result := make(map[string]*endpointRef)

	endpoints := e.endpoints
	now := e.now()
	for _, er := range endpoints {
		if er.isStrict {
			continue
		}

		if now.Sub(er.created) < e.unhealthyEndpointTimeout {
			continue
		}

		er.mtx.RLock()
		lastCheck := er.status.LastCheck
		er.mtx.RUnlock()

		if now.Sub(lastCheck) >= e.unhealthyEndpointTimeout {
			result[er.addr] = er
		}
	}

	return result
}

func (e *EndpointSet) getQueryableRefs() map[string]*endpointRef {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	endpoints := make(map[string]*endpointRef)
	for addr, er := range e.endpoints {
		if er.isQueryable() {
			endpoints[addr] = er
		}
	}

	return endpoints
}

// GetStoreClients returns a list of all active stores.
func (e *EndpointSet) GetStoreClients() []store.Client {
	endpoints := e.getQueryableRefs()

	stores := make([]store.Client, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasStoreAPI() {
			er.mtx.RLock()
			// Make a new endpointRef with store client.
			stores = append(stores, &endpointRef{
				StoreClient: storepb.NewStoreClient(er.cc),
				addr:        er.addr,
				metadata:    er.metadata,
				status:      er.status,
			})
			er.mtx.RUnlock()
		}
	}
	return stores
}

// GetQueryAPIClients returns a list of all active query API clients.
func (e *EndpointSet) GetQueryAPIClients() []Client {
	endpoints := e.getQueryableRefs()

	queryClients := make([]Client, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasQueryAPI() {
			client := querypb.NewQueryClient(er.cc)
			queryClients = append(queryClients, NewClient(client, er.addr, er.TSDBInfos()))
		}
	}
	return queryClients
}

// GetRulesClients returns a list of all active rules clients.
func (e *EndpointSet) GetRulesClients() []rulespb.RulesClient {
	endpoints := e.getQueryableRefs()

	rules := make([]rulespb.RulesClient, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasRulesAPI() {
			rules = append(rules, rulespb.NewRulesClient(er.cc))
		}
	}
	return rules
}

// GetTargetsClients returns a list of all active targets clients.
func (e *EndpointSet) GetTargetsClients() []targetspb.TargetsClient {
	endpoints := e.getQueryableRefs()

	targets := make([]targetspb.TargetsClient, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasTargetsAPI() {
			targets = append(targets, targetspb.NewTargetsClient(er.cc))
		}
	}
	return targets
}

// GetMetricMetadataClients returns a list of all active metadata clients.
func (e *EndpointSet) GetMetricMetadataClients() []metadatapb.MetadataClient {
	endpoints := e.getQueryableRefs()

	metadataClients := make([]metadatapb.MetadataClient, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasMetricMetadataAPI() {
			metadataClients = append(metadataClients, metadatapb.NewMetadataClient(er.cc))
		}
	}
	return metadataClients
}

// GetExemplarsStores returns a list of all active exemplars stores.
func (e *EndpointSet) GetExemplarsStores() []*exemplarspb.ExemplarStore {
	endpoints := e.getQueryableRefs()

	exemplarStores := make([]*exemplarspb.ExemplarStore, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasExemplarsAPI() {
			exemplarStores = append(exemplarStores, &exemplarspb.ExemplarStore{
				ExemplarsClient: exemplarspb.NewExemplarsClient(er.cc),
				LabelSets:       labelpb.LabelSetsToPromLabelSets(er.metadata.LabelSets),
			})
		}
	}
	return exemplarStores
}

// GetStatusClients returns a list of all active status clients.
func (e *EndpointSet) GetStatusClients() []statuspb.StatusClient {
	endpoints := e.getQueryableRefs()

	statusClients := make([]statuspb.StatusClient, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasStatusAPI() {
			statusClients = append(statusClients, statuspb.NewStatusClient(er.cc))
		}
	}
	return statusClients
}

func (e *EndpointSet) Close() {
	e.endpointsMtx.Lock()
	defer e.endpointsMtx.Unlock()

	for _, ef := range e.endpoints {
		ef.Close(e.gcTimeout)
	}
	e.endpoints = map[string]*endpointRef{}
}

func (e *EndpointSet) GetEndpointStatus() []EndpointStatus {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	statuses := make([]EndpointStatus, 0, len(e.endpoints))
	for _, v := range e.endpoints {
		v.mtx.RLock()
		defer v.mtx.RUnlock()

		status := v.status
		if status != nil {
			statuses = append(statuses, *status)
		}
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

type endpointRef struct {
	storepb.StoreClient

	mtx      sync.RWMutex
	cc       *grpc.ClientConn
	addr     string
	isStrict bool

	created  time.Time
	metadata *endpointMetadata
	status   *EndpointStatus

	logger log.Logger
}

// newEndpointRef creates a new endpointRef with a gRPC channel to the given the IP address.
// The call to newEndpointRef will return an error if establishing the channel fails.
func (e *EndpointSet) newEndpointRef(spec *GRPCEndpointSpec) (*endpointRef, error) {
	conn, err := grpc.NewClient(spec.Addr(), spec.dialOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "dialing connection")
	}

	return &endpointRef{
		logger:   e.logger,
		created:  e.now(),
		addr:     spec.Addr(),
		isStrict: spec.isStrictStatic,
		cc:       conn,
	}, nil
}

// update sets the metadata and status of the endpoint ref based on the info response value and error.
func (er *endpointRef) update(now nowFunc, metadata *endpointMetadata, err error) {
	er.mtx.Lock()
	defer er.mtx.Unlock()

	er.updateMetadata(metadata, err)
	er.updateStatus(now, err)
}

// updateStatus updates the endpointRef status based on the info call error.
func (er *endpointRef) updateStatus(now nowFunc, err error) {
	mint, maxt := er.timeRange()
	if er.status == nil {
		er.status = &EndpointStatus{Name: er.addr}
	}

	if err == nil {
		er.status.LastCheck = now()
		er.status.LabelSets = er.labelSets()
		er.status.ComponentType = er.componentType()
		er.status.MinTime = mint
		er.status.MaxTime = maxt
		er.status.LastError = nil
	} else {
		er.status.LastError = &stringError{originalErr: err}
	}
}

// updateMetadata sets the metadata for an endpoint ref based on the info call result and the info call error.
// When an info call for an endpoint fails, we preserve metadata from the previous state.
// If the is new and has no previous state, we assume it is a Store covering the complete time range.
func (er *endpointRef) updateMetadata(metadata *endpointMetadata, err error) {
	if err == nil {
		er.metadata = metadata
	}

	if err != nil && er.metadata == nil {
		er.metadata = maxRangeStoreMetadata()
	}
}

// isQueryable returns true if an endpointRef should be used for querying.
// A strict endpointRef is always queryable. A non-strict endpointRef
// is queryable if the last health check (info call) succeeded.
func (er *endpointRef) isQueryable() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.isStrict || er.status.LastError == nil
}

func (er *endpointRef) ComponentType() component.Component {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.componentType()
}

func (er *endpointRef) componentType() component.Component {
	if er.metadata == nil {
		return component.UnknownStoreAPI
	}

	return component.FromString(er.metadata.ComponentType)
}

func (er *endpointRef) HasStoreAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Store != nil
}

func (er *endpointRef) HasQueryAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Query != nil
}

func (er *endpointRef) HasRulesAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Rules != nil
}

func (er *endpointRef) HasTargetsAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Targets != nil
}

func (er *endpointRef) HasMetricMetadataAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.MetricMetadata != nil
}

func (er *endpointRef) HasExemplarsAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Exemplars != nil
}

func (er *endpointRef) HasStatusAPI() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.metadata != nil && er.metadata.Status != nil
}

func (er *endpointRef) LabelSets() []labels.Labels {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.status.LabelSets
}

func (er *endpointRef) labelSets() []labels.Labels {
	if er.metadata == nil {
		return make([]labels.Labels, 0)
	}

	labelSet := make([]labels.Labels, 0, len(er.metadata.LabelSets))
	for _, ls := range labelpb.LabelSetsToPromLabelSets(er.metadata.LabelSets) {
		if ls.Len() == 0 {
			continue
		}
		labelSet = append(labelSet, ls.Copy())
	}
	return labelSet
}

func (er *endpointRef) TimeRange() (mint, maxt int64) {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.timeRange()
}

func (er *endpointRef) TSDBInfos() []*infopb.TSDBInfo {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil || er.metadata.Store == nil {
		return nil
	}

	// Currently, min/max time of only StoreAPI is being updated by all components.
	return er.metadata.Store.TsdbInfos
}

func (er *endpointRef) timeRange() (int64, int64) {
	if er.metadata == nil || er.metadata.Store == nil {
		return math.MinInt64, math.MaxInt64
	}

	// Currently, min/max time of only StoreAPI is being updated by all components.
	return er.metadata.Store.MinTime, er.metadata.Store.MaxTime
}

func (er *endpointRef) SupportsSharding() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil || er.metadata.Store == nil {
		return false
	}

	return er.metadata.Store.SupportsSharding
}

func (er *endpointRef) SupportsWithoutReplicaLabels() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil || er.metadata.Store == nil {
		return false
	}

	return er.metadata.Store.SupportsWithoutReplicaLabels
}

func (er *endpointRef) String() string {
	mint, maxt := er.TimeRange()
	return fmt.Sprintf(
		"Addr: %s MinTime: %d MaxTime: %d",
		er.addr, mint, maxt,
	)
}

func (er *endpointRef) Addr() (string, bool) {
	return er.addr, false
}

func (er *endpointRef) Close(gcDelay time.Duration) {
	// NOTE(GiedriusS): We cannot close the gRPC connection easily. Someone might still be using it even if we do locking.
	// I think there are two possibilities:
	// 1. Do garbage collection in the background. Question is WHEN to close it.
	// 2. We need to ensure no more calls are made to this endpointRef before Close is called. Cannot do this because SendMsg() is async and we might still be
	// using it even if Series() has returned (I think?). It would be a lot of work to refactor all clients to use reference counting.
	// So, in reality, only one works for now.
	// Hence, we need to let the last calls finish. Use the maximum timeout as the garbage collection delay.
	level.Info(er.logger).Log("msg", "waiting for gRPC calls to finish before closing", "addr", er.addr, "gcDelay", gcDelay)
	time.AfterFunc(gcDelay, func() {
		runutil.CloseWithLogOnErr(er.logger, er.cc, "endpoint %v connection closed", er.addr)
	})
}

func (er *endpointRef) apisPresent() []string {
	var apisPresent []string

	if er.HasStoreAPI() {
		apisPresent = append(apisPresent, "storeEndpoints")
	}

	if er.HasRulesAPI() {
		apisPresent = append(apisPresent, "rulesAPI")
	}

	if er.HasExemplarsAPI() {
		apisPresent = append(apisPresent, "exemplarsAPI")
	}

	if er.HasTargetsAPI() {
		apisPresent = append(apisPresent, "targetsAPI")
	}

	if er.HasMetricMetadataAPI() {
		apisPresent = append(apisPresent, "MetricMetadataAPI")
	}

	if er.HasQueryAPI() {
		apisPresent = append(apisPresent, "QueryAPI")
	}

	return apisPresent
}

func (er *endpointRef) Matches(matchers []*labels.Matcher) bool {
	return true
}

type endpointMetadata struct {
	*infopb.InfoResponse
}

type endpointStat struct {
	ip        string
	extLset   string
	component string
}

func newEndpointAPIStats() endpointStats {
	return []endpointStat{}
}

type endpointStats []endpointStat

func (s *endpointStats) Sort() endpointStats {
	sort.Slice(*s, func(i, j int) bool {
		ipc := strings.Compare((*s)[i].ip, (*s)[j].ip)
		if ipc != 0 {
			return ipc < 0
		}

		extLsetc := strings.Compare((*s)[i].extLset, (*s)[j].extLset)
		if extLsetc != 0 {
			return extLsetc < 0
		}

		return strings.Compare((*s)[i].component, (*s)[j].component) < 0
	})

	return *s
}

func (es *endpointStats) append(ip, extLset, component string) endpointStats {
	truncatedExtLabels := truncateExtLabels(extLset, externalLabelLimit)

	return append(*es, endpointStat{
		ip:        ip,
		extLset:   truncatedExtLabels,
		component: component,
	})
}

func maxRangeStoreMetadata() *endpointMetadata {
	return &endpointMetadata{
		InfoResponse: &infopb.InfoResponse{
			Store: &infopb.StoreInfo{
				MinTime: math.MinInt64,
				MaxTime: math.MaxInt64,
			},
		},
	}
}
