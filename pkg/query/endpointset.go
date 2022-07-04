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

// IsStrictStatic returns true if the endpoint has been statically defined and it is under a strict mode.
func (es *GRPCEndpointSpec) IsStrictStatic() bool {
	return es.isStrictStatic
}

func (es *GRPCEndpointSpec) Addr() string {
	// API address should not change between state changes.
	return es.addr
}

// Metadata method for gRPC endpoint tries to call InfoAPI exposed by Thanos components until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (es *GRPCEndpointSpec) Metadata(ctx context.Context, infoClient infopb.InfoClient, storeClient storepb.StoreClient) (*endpointMetadata, error) {
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

func (es *GRPCEndpointSpec) getMetadataUsingStoreAPI(ctx context.Context, client storepb.StoreClient) (*endpointMetadata, error) {
	resp, err := client.Info(ctx, &storepb.InfoRequest{})
	if err != nil {
		return nil, err
	}

	infoResp := es.fillExpectedAPIs(component.FromProto(resp.StoreType), resp.MinTime, resp.MaxTime)
	infoResp.LabelSets = resp.LabelSets
	infoResp.ComponentType = component.FromProto(resp.StoreType).String()

	return &endpointMetadata{
		&infoResp,
	}, nil
}

func (es *GRPCEndpointSpec) fillExpectedAPIs(componentType component.Component, mintime, maxTime int64) infopb.InfoResponse {
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
	logger log.Logger

	// Endpoint specifications can change dynamically. If some component is missing from the list, we assume it is no longer
	// accessible and we close gRPC client for it, unless it is strict.
	endpointSpec        func() []*GRPCEndpointSpec
	dialOpts            []grpc.DialOption
	gRPCInfoCallTimeout time.Duration

	updateMtx            sync.Mutex
	endpointsMtx         sync.RWMutex
	endpointsStatusesMtx sync.RWMutex

	// Main map of stores currently used for fanout.
	endpoints       map[string]*endpointRef
	endpointsMetric *endpointSetNodeCollector

	// Map of statuses used only by UI.
	endpointStatuses         map[string]*EndpointStatus
	unhealthyEndpointTimeout time.Duration
}

// NewEndpointSet returns a new set of Thanos APIs.
func NewEndpointSet(
	logger log.Logger,
	reg *prometheus.Registry,
	endpointSpecs func() []*GRPCEndpointSpec,
	dialOpts []grpc.DialOption,
	unhealthyEndpointTimeout time.Duration,
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

	es := &EndpointSet{
		logger:                   log.With(logger, "component", "endpointset"),
		dialOpts:                 dialOpts,
		endpointsMetric:          endpointsMetric,
		gRPCInfoCallTimeout:      5 * time.Second,
		endpoints:                make(map[string]*endpointRef),
		endpointStatuses:         make(map[string]*EndpointStatus),
		unhealthyEndpointTimeout: unhealthyEndpointTimeout,
		endpointSpec:             endpointSpecs,
	}
	return es
}

// Update updates the endpoint set. It fetches current list of endpoint specs from function and updates the fresh metadata
// from all endpoints. Keeps around statically defined nodes that were defined with the strict mode.
func (e *EndpointSet) Update(ctx context.Context) {
	e.updateMtx.Lock()
	defer e.updateMtx.Unlock()

	e.endpointsMtx.RLock()
	endpoints := make(map[string]*endpointRef, len(e.endpoints))
	for addr, er := range e.endpoints {
		endpoints[addr] = er
	}
	e.endpointsMtx.RUnlock()

	level.Debug(e.logger).Log("msg", "starting to update API endpoints", "cachedEndpoints", len(endpoints))

	activeEndpoints := e.getActiveEndpoints(ctx, endpoints)
	level.Debug(e.logger).Log("msg", "checked requested endpoints", "activeEndpoints", len(activeEndpoints), "cachedEndpoints", len(endpoints))

	stats := newEndpointAPIStats()

	// Close endpoints which are not active this time (are not in active endpoints map).
	for addr, er := range endpoints {
		if _, ok := activeEndpoints[addr]; ok {
			stats[er.ComponentType()][labelpb.PromLabelSetsToString(er.LabelSets())]++
			continue
		}

		er.Close()
		delete(endpoints, addr)
		e.updateEndpointStatus(er, errors.New(unhealthyEndpointMessage))
		level.Info(er.logger).Log("msg", unhealthyEndpointMessage, "address", addr, "extLset", labelpb.PromLabelSetsToString(er.LabelSets()))
	}

	// Add endpoints that are not yet in activeEndpoints map.
	for addr, er := range activeEndpoints {
		if _, ok := endpoints[addr]; ok {
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

		endpoints[addr] = er
		e.updateEndpointStatus(er, nil)

		level.Info(e.logger).Log("msg", fmt.Sprintf("adding new %v with %+v", er.ComponentType(), er.apisPresent()), "address", addr, "extLset", extLset)
	}

	e.endpointsMetric.Update(stats)
	e.endpointsMtx.Lock()
	e.endpoints = endpoints
	e.endpointsMtx.Unlock()

	e.cleanUpEndpointStatuses(endpoints)
}

// GetStoreClients returns a list of all active stores.
func (e *EndpointSet) GetStoreClients() []store.Client {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	stores := make([]store.Client, 0, len(e.endpoints))
	for _, er := range e.endpoints {
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
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	stores := make([]querypb.QueryClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasQueryAPI() {
			stores = append(stores, querypb.NewQueryClient(er.cc))
		}
	}
	return stores
}

// GetRulesClients returns a list of all active rules clients.
func (e *EndpointSet) GetRulesClients() []rulespb.RulesClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	rules := make([]rulespb.RulesClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasRulesAPI() {
			rules = append(rules, rulespb.NewRulesClient(er.cc))
		}
	}
	return rules
}

// GetTargetsClients returns a list of all active targets clients.
func (e *EndpointSet) GetTargetsClients() []targetspb.TargetsClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	targets := make([]targetspb.TargetsClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasTargetsAPI() {
			targets = append(targets, targetspb.NewTargetsClient(er.cc))
		}
	}
	return targets
}

// GetMetricMetadataClients returns a list of all active metadata clients.
func (e *EndpointSet) GetMetricMetadataClients() []metadatapb.MetadataClient {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	metadataClients := make([]metadatapb.MetadataClient, 0, len(e.endpoints))
	for _, er := range e.endpoints {
		if er.HasMetricMetadataAPI() {
			metadataClients = append(metadataClients, metadatapb.NewMetadataClient(er.cc))
		}
	}
	return metadataClients
}

// GetExemplarsStores returns a list of all active exemplars stores.
func (e *EndpointSet) GetExemplarsStores() []*exemplarspb.ExemplarStore {
	e.endpointsMtx.RLock()
	defer e.endpointsMtx.RUnlock()

	exemplarStores := make([]*exemplarspb.ExemplarStore, 0, len(e.endpoints))
	for _, er := range e.endpoints {
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

func (e *EndpointSet) getActiveEndpoints(ctx context.Context, endpoints map[string]*endpointRef) map[string]*endpointRef {
	var (
		activeEndpoints = make(map[string]*endpointRef, len(endpoints))
		mtx             sync.Mutex
		wg              sync.WaitGroup

		endpointAddrSet = make(map[string]struct{})
	)

	// Gather healthy endpoints map concurrently using info API. Build new clients if does not exist already.
	for _, es := range e.endpointSpec() {
		if _, ok := endpointAddrSet[es.Addr()]; ok {
			continue
		}
		endpointAddrSet[es.Addr()] = struct{}{}

		wg.Add(1)
		go func(spec *GRPCEndpointSpec) {
			defer wg.Done()

			addr := spec.Addr()

			ctx, cancel := context.WithTimeout(ctx, e.gRPCInfoCallTimeout)
			defer cancel()

			er, seenAlready := endpoints[addr]
			if !seenAlready {
				// New endpoint or was unactive and was removed in the past - create the new one.
				conn, err := grpc.DialContext(ctx, addr, e.dialOpts...)
				if err != nil {
					e.updateEndpointStatus(&endpointRef{addr: addr}, err)
					level.Warn(e.logger).Log("msg", "update of node failed", "err", errors.Wrap(err, "dialing connection"), "address", addr)
					return
				}

				// Assume that StoreAPI is also exposed because if call to info service fails we will call info method of storeAPI.
				// It will be overwritten to null if not present.
				er = &endpointRef{
					cc:     conn,
					addr:   addr,
					logger: e.logger,
				}
			}

			metadata, err := spec.Metadata(ctx, infopb.NewInfoClient(er.cc), storepb.NewStoreClient(er.cc))
			if err != nil {
				if !seenAlready && !spec.IsStrictStatic() {
					// Close only if new and not a strict static node.
					// Inactive `e.endpoints` will be closed later on.
					er.Close()
				}

				e.updateEndpointStatus(er, err)
				level.Warn(e.logger).Log("msg", "update of node failed", "err", errors.Wrap(err, "getting metadata"), "address", addr)

				if !spec.IsStrictStatic() {
					return
				}

				// Still keep it around if static & strict mode enabled.
				// Assume that it expose storeAPI and cover all complete possible time range.
				if !seenAlready {
					metadata = &endpointMetadata{
						&infopb.InfoResponse{
							Store: &infopb.StoreInfo{
								MinTime: math.MinInt64,
								MaxTime: math.MaxInt64,
							},
						},
					}
					er.Update(metadata)
				}

				mtx.Lock()
				defer mtx.Unlock()

				activeEndpoints[addr] = er
				return
			}

			er.Update(metadata)
			e.updateEndpointStatus(er, nil)

			mtx.Lock()
			defer mtx.Unlock()

			activeEndpoints[addr] = er
		}(es)
	}
	wg.Wait()

	return activeEndpoints
}

func (e *EndpointSet) updateEndpointStatus(er *endpointRef, err error) {
	e.endpointsStatusesMtx.Lock()
	defer e.endpointsStatusesMtx.Unlock()

	status := EndpointStatus{Name: er.addr}
	prev, ok := e.endpointStatuses[er.addr]
	if ok {
		status = *prev
	} else {
		mint, maxt := er.TimeRange()
		status.MinTime = mint
		status.MaxTime = maxt
	}

	if err == nil {
		status.LastCheck = time.Now()
		mint, maxt := er.TimeRange()
		status.LabelSets = er.LabelSets()
		status.ComponentType = er.ComponentType()
		status.MinTime = mint
		status.MaxTime = maxt
		status.LastError = nil
	} else {
		status.LastError = &stringError{originalErr: err}
	}

	e.endpointStatuses[er.addr] = &status
}

func (e *EndpointSet) GetEndpointStatus() []EndpointStatus {
	e.endpointsStatusesMtx.RLock()
	defer e.endpointsStatusesMtx.RUnlock()

	statuses := make([]EndpointStatus, 0, len(e.endpointStatuses))
	for _, v := range e.endpointStatuses {
		statuses = append(statuses, *v)
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

func (e *EndpointSet) cleanUpEndpointStatuses(endpoints map[string]*endpointRef) {
	e.endpointsStatusesMtx.Lock()
	defer e.endpointsStatusesMtx.Unlock()

	now := time.Now()
	for addr, status := range e.endpointStatuses {
		if _, ok := endpoints[addr]; ok {
			continue
		}

		if now.Sub(status.LastCheck) >= e.unhealthyEndpointTimeout {
			delete(e.endpointStatuses, addr)
		}
	}
}

type endpointRef struct {
	storepb.StoreClient

	mtx  sync.RWMutex
	cc   *grpc.ClientConn
	addr string

	// Metadata can change during runtime.
	metadata *endpointMetadata

	logger log.Logger
}

func (er *endpointRef) Update(metadata *endpointMetadata) {
	er.mtx.Lock()
	defer er.mtx.Unlock()

	er.metadata = metadata
}

func (er *endpointRef) ComponentType() component.Component {
	er.mtx.RLock()
	defer er.mtx.RUnlock()

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

	if er.metadata == nil || er.metadata.Store == nil {
		return math.MinInt64, math.MaxInt64
	}

	// Currently, min/max time of only StoreAPI is being updated by all components.
	return er.metadata.Store.MinTime, er.metadata.Store.MaxTime
}

func (er *endpointRef) String() string {
	mint, maxt := er.TimeRange()
	return fmt.Sprintf("Addr: %s LabelSets: %v Mint: %d Maxt: %d", er.addr, labelpb.PromLabelSetsToString(er.LabelSets()), mint, maxt)
}

func (er *endpointRef) Addr() string {
	return er.addr
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
