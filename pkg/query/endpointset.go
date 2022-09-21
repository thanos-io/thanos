// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/thanos-io/thanos/pkg/api/query/querypb"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/targets/targetspb"
)

const (
	unhealthyEndpointMessage  = "removing endpoint because it's unhealthy or does not exist"
	noMetadataEndpointMessage = "cannot obtain metadata: neither info nor store client found"
)

type GRPCEndpointSpec struct {
	addr           string
	isStrictStatic bool
}

// NewGRPCEndpointSpec creates gRPC endpoint spec.
// It uses InfoAPI to get Metadata.
func NewGRPCEndpointSpec(addr string, isStrictStatic bool) *GRPCEndpointSpec {
	return &GRPCEndpointSpec{addr: addr, isStrictStatic: isStrictStatic}
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
		if err == nil {
			return &endpointMetadata{resp}, nil
		}
	}

	// Call Info method of StoreAPI, this way querier will be able to discovery old components not exposing InfoAPI.
	if storeClient != nil {
		metadata, err := es.getMetadataUsingStoreAPI(ctx, storeClient)
		if err != nil {
			return nil, errors.Wrapf(err, "fallback fetching info from %s", es.addr)
		}
		return metadata, nil
	}

	return nil, errors.New(noMetadataEndpointMessage)
}

func (es *endpointRef) getMetadataUsingStoreAPI(ctx context.Context, client storepb.StoreClient) (*endpointMetadata, error) {
	resp, err := client.Info(ctx, &storepb.InfoRequest{})
	if err != nil {
		return nil, err
	}

	infoResp := fillExpectedAPIs(component.FromProto(resp.StoreType), resp.MinTime, resp.MaxTime)
	infoResp.LabelSets = resp.LabelSets
	infoResp.ComponentType = component.FromProto(resp.StoreType).String()

	return &endpointMetadata{
		&infoResp,
	}, nil
}

func fillExpectedAPIs(componentType component.Component, mintime, maxTime int64) infopb.InfoResponse {
	switch componentType {
	case component.Sidecar:
		return infopb.InfoResponse{
			Store: &infopb.StoreInfo{
				MinTime: mintime,
				MaxTime: maxTime,
			},
			Rules:          &infopb.RulesInfo{},
			Targets:        &infopb.TargetsInfo{},
			MetricMetadata: &infopb.MetricMetadataInfo{},
			Exemplars:      &infopb.ExemplarsInfo{},
		}
	case component.Query:
		{
			return infopb.InfoResponse{
				Store: &infopb.StoreInfo{
					MinTime: mintime,
					MaxTime: maxTime,
				},
				Rules:          &infopb.RulesInfo{},
				Targets:        &infopb.TargetsInfo{},
				MetricMetadata: &infopb.MetricMetadataInfo{},
				Exemplars:      &infopb.ExemplarsInfo{},
				Query:          &infopb.QueryAPIInfo{},
			}
		}
	case component.Receive:
		{
			return infopb.InfoResponse{
				Store: &infopb.StoreInfo{
					MinTime: mintime,
					MaxTime: maxTime,
				},
				Exemplars: &infopb.ExemplarsInfo{},
			}
		}
	case component.Store:
		return infopb.InfoResponse{
			Store: &infopb.StoreInfo{
				MinTime: mintime,
				MaxTime: maxTime,
			},
		}
	case component.Rule:
		return infopb.InfoResponse{
			Store: &infopb.StoreInfo{
				MinTime: mintime,
				MaxTime: maxTime,
			},
			Rules: &infopb.RulesInfo{},
		}
	default:
		return infopb.InfoResponse{}
	}
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
// TODO(hitanshu-mehta) Currently,only collecting metrics of storeAPI. Make this struct generic.
type endpointSetNodeCollector struct {
	mtx             sync.Mutex
	storeNodes      map[component.Component]map[string]int
	storePerExtLset map[string]int

	connectionsDesc *prometheus.Desc
}

func newEndpointSetNodeCollector() *endpointSetNodeCollector {
	return &endpointSetNodeCollector{
		storeNodes: map[component.Component]map[string]int{},
		connectionsDesc: prometheus.NewDesc(
			"thanos_store_nodes_grpc_connections",
			"Number of gRPC connection to Store APIs. Opened connection means healthy store APIs available for Querier.",
			[]string{"external_labels", "store_type"}, nil,
		),
	}
}

func (c *endpointSetNodeCollector) Update(nodes map[component.Component]map[string]int) {
	storeNodes := make(map[component.Component]map[string]int, len(nodes))
	storePerExtLset := map[string]int{}

	for k, v := range nodes {
		storeNodes[k] = make(map[string]int, len(v))
		for kk, vv := range v {
			storePerExtLset[kk] += vv
			storeNodes[k][kk] = vv
		}
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.storeNodes = storeNodes
	c.storePerExtLset = storePerExtLset
}

func (c *endpointSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsDesc
}

func (c *endpointSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for storeType, occurrencesPerExtLset := range c.storeNodes {
		for externalLabels, occurrences := range occurrencesPerExtLset {
			var storeTypeStr string
			if storeType != nil {
				storeTypeStr = storeType.String()
			}
			ch <- prometheus.MustNewConstMetric(c.connectionsDesc, prometheus.GaugeValue, float64(occurrences), externalLabels, storeTypeStr)
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
	endpointSpec             func() map[string]*GRPCEndpointSpec
	dialOpts                 []grpc.DialOption
	endpointInfoTimeout      time.Duration
	unhealthyEndpointTimeout time.Duration

	updateMtx sync.Mutex

	endpointsMtx    sync.RWMutex
	endpoints       map[string]*endpointRef
	endpointsMetric *endpointSetNodeCollector
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
	reg *prometheus.Registry,
	endpointSpecs func() []*GRPCEndpointSpec,
	dialOpts []grpc.DialOption,
	unhealthyEndpointTimeout time.Duration,
	endpointInfoTimeout time.Duration,
) *EndpointSet {
	endpointsMetric := newEndpointSetNodeCollector()
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
		now:             now,
		logger:          log.With(logger, "component", "endpointset"),
		endpointsMetric: endpointsMetric,

		dialOpts:                 dialOpts,
		endpointInfoTimeout:      endpointInfoTimeout,
		unhealthyEndpointTimeout: unhealthyEndpointTimeout,
		endpointSpec: func() map[string]*GRPCEndpointSpec {
			specs := make(map[string]*GRPCEndpointSpec)
			for _, s := range endpointSpecs() {
				specs[s.addr] = s
			}
			return specs
		},
		endpoints: make(map[string]*endpointRef),
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

	for _, spec := range e.endpointSpec() {
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

			newRef, err := e.newEndpointRef(ctx, spec)
			if err != nil {
				level.Warn(e.logger).Log("msg", "new endpoint creation failed", "err", err, "address", spec.Addr())
				return
			}

			e.updateEndpoint(ctx, spec, newRef)
			if !newRef.isQueryable() {
				newRef.Close()
				return
			}

			mu.Lock()
			defer mu.Unlock()
			newRefs[spec.Addr()] = newRef
		}(spec)
	}
	wg.Wait()

	timedOutRefs := e.getTimedOutRefs()
	for addr, er := range e.endpoints {
		_, isNew := newRefs[addr]
		_, isExisting := existingRefs[addr]
		_, isTimedOut := timedOutRefs[addr]
		if !isNew && !isExisting || isTimedOut {
			staleRefs[addr] = er
		}
	}

	e.endpointsMtx.Lock()
	defer e.endpointsMtx.Unlock()
	for addr, er := range newRefs {
		extLset := labelpb.PromLabelSetsToString(er.LabelSets())
		level.Info(e.logger).Log("msg", fmt.Sprintf("adding new %v with %+v", er.ComponentType(), er.apisPresent()), "address", addr, "extLset", extLset)
		e.endpoints[addr] = er
	}
	for addr, er := range staleRefs {
		level.Info(er.logger).Log("msg", unhealthyEndpointMessage, "address", er.addr, "extLset", labelpb.PromLabelSetsToString(er.LabelSets()))
		er.Close()
		delete(e.endpoints, addr)
	}
	level.Debug(e.logger).Log("msg", "updated endpoints", "activeEndpoints", len(e.endpoints))

	// Update stats.
	stats := newEndpointAPIStats()
	for addr, er := range e.endpoints {
		if !er.isQueryable() {
			continue
		}

		extLset := labelpb.PromLabelSetsToString(er.LabelSets())

		// All producers that expose StoreAPI should have unique external labels. Check all which connect to our Querier.
		if er.HasStoreAPI() && (er.ComponentType() == component.Sidecar || er.ComponentType() == component.Rule) &&
			stats[component.Sidecar][extLset]+stats[component.Rule][extLset] > 0 {

			level.Warn(e.logger).Log("msg", "found duplicate storeAPI producer (sidecar or ruler). This is not advices as it will malform data in in the same bucket",
				"address", addr, "extLset", extLset, "duplicates", fmt.Sprintf("%v", stats[component.Sidecar][extLset]+stats[component.Rule][extLset]+1))
		}
		stats[er.ComponentType()][extLset]++
	}

	e.endpointsMetric.Update(stats)
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

		lastCheck := er.getStatus().LastCheck
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
			// Make a new endpointRef with store client.
			stores = append(stores, &endpointRef{
				StoreClient: storepb.NewStoreClient(er.cc),
				addr:        er.addr,
				metadata:    er.metadata,
			})
		}
	}
	return stores
}

// GetQueryAPIClients returns a list of all active query API clients.
func (e *EndpointSet) GetQueryAPIClients() []querypb.QueryClient {
	endpoints := e.getQueryableRefs()

	stores := make([]querypb.QueryClient, 0, len(endpoints))
	for _, er := range endpoints {
		if er.HasQueryAPI() {
			stores = append(stores, querypb.NewQueryClient(er.cc))
		}
	}
	return stores
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
				LabelSets:       labelpb.ZLabelSetsToPromLabelSets(er.metadata.LabelSets...),
			})
		}
	}
	return exemplarStores
}

func (e *EndpointSet) Close() {
	e.endpointsMtx.Lock()
	defer e.endpointsMtx.Unlock()

	for _, ef := range e.endpoints {
		ef.Close()
	}
	e.endpoints = map[string]*endpointRef{}
}

func (e *EndpointSet) GetEndpointStatus() []EndpointStatus {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	statuses := make([]EndpointStatus, 0, len(e.endpoints))
	for _, v := range e.endpoints {
		status := v.getStatus()
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
func (e *EndpointSet) newEndpointRef(ctx context.Context, spec *GRPCEndpointSpec) (*endpointRef, error) {
	conn, err := grpc.DialContext(ctx, spec.Addr(), e.dialOpts...)
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
	er.mtx.RLock()
	defer er.mtx.RUnlock()

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

func (er *endpointRef) getStatus() *EndpointStatus {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.status
}

// isQueryable returns true if an endpointRef should be used for querying.
// A strict endpointRef is always queriable. A non-strict endpointRef
// is queryable if the last health check (info call) succeeded.
func (er *endpointRef) isQueryable() bool {
	return er.isStrict || er.getStatus().LastError == nil
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

func (er *endpointRef) LabelSets() []labels.Labels {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	return er.labelSets()
}

func (er *endpointRef) labelSets() []labels.Labels {
	if er.metadata == nil {
		return make([]labels.Labels, 0)
	}

	labelSet := make([]labels.Labels, 0, len(er.metadata.LabelSets))
	for _, ls := range labelpb.ZLabelSetsToPromLabelSets(er.metadata.LabelSets...) {
		if len(ls) == 0 {
			continue
		}
		// Compatibility label for Queriers pre 0.8.1. Filter it out now.
		if ls[0].Name == store.CompatibilityTypeLabelName {
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

func (er *endpointRef) SendsSortedSeries() bool {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

	if er.metadata == nil || er.metadata.Store == nil {
		return false
	}

	return er.metadata.Store.SendsSortedSeries
}

func (er *endpointRef) String() string {
	mint, maxt := er.TimeRange()
	return fmt.Sprintf(
		"Addr: %s LabelSets: %v Mint: %d Maxt: %d",
		er.addr, labelpb.PromLabelSetsToString(er.LabelSets()), mint, maxt,
	)
}

func (er *endpointRef) Addr() (string, bool) {
	return er.addr, false
}

func (er *endpointRef) Close() {
	runutil.CloseWithLogOnErr(er.logger, er.cc, fmt.Sprintf("endpoint %v connection closed", er.addr))
}

func (er *endpointRef) apisPresent() []string {
	var apisPresent []string

	if er.HasStoreAPI() {
		apisPresent = append(apisPresent, "storeAPI")
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

type endpointMetadata struct {
	*infopb.InfoResponse
}

func newEndpointAPIStats() map[component.Component]map[string]int {
	nodes := make(map[component.Component]map[string]int, len(storepb.StoreType_name))
	for i := range storepb.StoreType_name {
		nodes[component.FromProto(storepb.StoreType(i))] = map[string]int{}
	}
	return nodes
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
