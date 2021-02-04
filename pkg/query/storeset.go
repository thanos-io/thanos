// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/metadata/metadatapb"
	"github.com/thanos-io/thanos/pkg/rules/rulespb"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

const (
	unhealthyStoreMessage = "removing store because it's unhealthy or does not exist"
)

type StoreSpec interface {
	// Addr returns StoreAPI Address for the store spec. It is used as ID for store.
	Addr() string
	// Metadata returns current labels, store type and min, max ranges for store.
	// It can change for every call for this method.
	// If metadata call fails we assume that store is no longer accessible and we should not use it.
	// NOTE: It is implementation responsibility to retry until context timeout, but a caller responsibility to manage
	// given store connection.
	Metadata(ctx context.Context, client storepb.StoreClient) (labelSets []labels.Labels, mint int64, maxt int64, storeType component.StoreAPI, err error)

	// StrictStatic returns true if the StoreAPI has been statically defined and it is under a strict mode.
	StrictStatic() bool
}

type RuleSpec interface {
	// Addr returns RulesAPI Address for the rules spec. It is used as its ID.
	Addr() string
}

type MetadataSpec interface {
	// Addr returns MetadataAPI Address for the metadata spec. It is used as its ID.
	Addr() string
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

type StoreStatus struct {
	Name      string             `json:"name"`
	LastCheck time.Time          `json:"lastCheck"`
	LastError *stringError       `json:"lastError"`
	LabelSets []labels.Labels    `json:"labelSets"`
	StoreType component.StoreAPI `json:"-"`
	MinTime   int64              `json:"minTime"`
	MaxTime   int64              `json:"maxTime"`
}

type grpcStoreSpec struct {
	addr         string
	strictstatic bool
}

// NewGRPCStoreSpec creates store pure gRPC spec.
// It uses Info gRPC call to get Metadata.
func NewGRPCStoreSpec(addr string, strictstatic bool) StoreSpec {
	return &grpcStoreSpec{addr: addr, strictstatic: strictstatic}
}

// StrictStatic returns true if the StoreAPI has been statically defined and it is under a strict mode.
func (s *grpcStoreSpec) StrictStatic() bool {
	return s.strictstatic
}

func (s *grpcStoreSpec) Addr() string {
	// API addr should not change between state changes.
	return s.addr
}

// Metadata method for gRPC store API tries to reach host Info method until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (s *grpcStoreSpec) Metadata(ctx context.Context, client storepb.StoreClient) (labelSets []labels.Labels, mint int64, maxt int64, Type component.StoreAPI, err error) {
	resp, err := client.Info(ctx, &storepb.InfoRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, 0, 0, nil, errors.Wrapf(err, "fetching store info from %s", s.addr)
	}
	if len(resp.LabelSets) == 0 && len(resp.Labels) > 0 {
		resp.LabelSets = []labelpb.ZLabelSet{{Labels: resp.Labels}}
	}

	labelSets = make([]labels.Labels, 0, len(resp.LabelSets))
	for _, ls := range resp.LabelSets {
		labelSets = append(labelSets, ls.PromLabels())
	}
	return labelSets, resp.MinTime, resp.MaxTime, component.FromProto(resp.StoreType), nil
}

// storeSetNodeCollector is a metric collector reporting the number of available storeAPIs for Querier.
// A Collector is required as we want atomic updates for all 'thanos_store_nodes_grpc_connections' series.
type storeSetNodeCollector struct {
	mtx             sync.Mutex
	storeNodes      map[component.StoreAPI]map[string]int
	storePerExtLset map[string]int

	connectionsDesc *prometheus.Desc
}

func newStoreSetNodeCollector() *storeSetNodeCollector {
	return &storeSetNodeCollector{
		storeNodes: map[component.StoreAPI]map[string]int{},
		connectionsDesc: prometheus.NewDesc(
			"thanos_store_nodes_grpc_connections",
			"Number of gRPC connection to Store APIs. Opened connection means healthy store APIs available for Querier.",
			[]string{"external_labels", "store_type"}, nil,
		),
	}
}

func (c *storeSetNodeCollector) Update(nodes map[component.StoreAPI]map[string]int) {
	storeNodes := make(map[component.StoreAPI]map[string]int, len(nodes))
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

func (c *storeSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsDesc
}

func (c *storeSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
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

// StoreSet maintains a set of active stores. It is backed up by Store Specifications that are dynamically fetched on
// every Update() call.
type StoreSet struct {
	logger log.Logger

	// Store specifications can change dynamically. If some store is missing from the list, we assuming it is no longer
	// accessible and we close gRPC client for it.
	storeSpecs          func() []StoreSpec
	ruleSpecs           func() []RuleSpec
	metadataSpecs       func() []MetadataSpec
	dialOpts            []grpc.DialOption
	gRPCInfoCallTimeout time.Duration

	updateMtx         sync.Mutex
	storesMtx         sync.RWMutex
	storesStatusesMtx sync.RWMutex

	// Main map of stores currently used for fanout.
	stores       map[string]*storeRef
	storesMetric *storeSetNodeCollector

	// Map of statuses used only by UI.
	storeStatuses         map[string]*StoreStatus
	unhealthyStoreTimeout time.Duration
}

// NewStoreSet returns a new set of store APIs and potentially Rules APIs from given specs.
func NewStoreSet(
	logger log.Logger,
	reg *prometheus.Registry,
	storeSpecs func() []StoreSpec,
	ruleSpecs func() []RuleSpec,
	metadataSpecs func() []MetadataSpec,
	dialOpts []grpc.DialOption,
	unhealthyStoreTimeout time.Duration,
) *StoreSet {
	storesMetric := newStoreSetNodeCollector()
	if reg != nil {
		reg.MustRegister(storesMetric)
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}
	if storeSpecs == nil {
		storeSpecs = func() []StoreSpec { return nil }
	}
	if ruleSpecs == nil {
		ruleSpecs = func() []RuleSpec { return nil }
	}
	if metadataSpecs == nil {
		metadataSpecs = func() []MetadataSpec { return nil }
	}

	ss := &StoreSet{
		logger:                log.With(logger, "component", "storeset"),
		storeSpecs:            storeSpecs,
		ruleSpecs:             ruleSpecs,
		metadataSpecs:         metadataSpecs,
		dialOpts:              dialOpts,
		storesMetric:          storesMetric,
		gRPCInfoCallTimeout:   5 * time.Second,
		stores:                make(map[string]*storeRef),
		storeStatuses:         make(map[string]*StoreStatus),
		unhealthyStoreTimeout: unhealthyStoreTimeout,
	}
	return ss
}

// TODO(bwplotka): Consider moving storeRef out of this package and renaming it, as it also supports rules API.
type storeRef struct {
	storepb.StoreClient

	mtx  sync.RWMutex
	cc   *grpc.ClientConn
	addr string
	// If rule is not nil, then this store also supports rules API.
	rule     rulespb.RulesClient
	metadata metadatapb.MetadataClient

	// Meta (can change during runtime).
	labelSets []labels.Labels
	storeType component.StoreAPI
	minTime   int64
	maxTime   int64

	logger log.Logger
}

func (s *storeRef) Update(labelSets []labels.Labels, minTime int64, maxTime int64, storeType component.StoreAPI, rule rulespb.RulesClient, metadata metadatapb.MetadataClient) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.storeType = storeType
	s.labelSets = labelSets
	s.minTime = minTime
	s.maxTime = maxTime
	s.rule = rule
	s.metadata = metadata
}

func (s *storeRef) StoreType() component.StoreAPI {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.storeType
}

func (s *storeRef) HasRulesAPI() bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.rule != nil
}

func (s *storeRef) HasMetadataAPI() bool {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.metadata != nil
}

func (s *storeRef) LabelSets() []labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	labelSet := make([]labels.Labels, 0, len(s.labelSets))
	for _, ls := range s.labelSets {
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

func (s *storeRef) TimeRange() (mint int64, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.minTime, s.maxTime
}

func (s *storeRef) String() string {
	mint, maxt := s.TimeRange()
	return fmt.Sprintf("Addr: %s LabelSets: %v Mint: %d Maxt: %d", s.addr, labelpb.PromLabelSetsToString(s.LabelSets()), mint, maxt)
}

func (s *storeRef) Addr() string {
	return s.addr
}

func (s *storeRef) Close() {
	runutil.CloseWithLogOnErr(s.logger, s.cc, fmt.Sprintf("store %v connection close", s.addr))
}

func newStoreAPIStats() map[component.StoreAPI]map[string]int {
	nodes := make(map[component.StoreAPI]map[string]int, len(storepb.StoreType_name))
	for i := range storepb.StoreType_name {
		nodes[component.FromProto(storepb.StoreType(i))] = map[string]int{}
	}
	return nodes
}

// Update updates the store set. It fetches current list of store specs from function and updates the fresh metadata
// from all stores. Keeps around statically defined nodes that were defined with the strict mode.
func (s *StoreSet) Update(ctx context.Context) {
	s.updateMtx.Lock()
	defer s.updateMtx.Unlock()

	s.storesMtx.RLock()
	stores := make(map[string]*storeRef, len(s.stores))
	for addr, st := range s.stores {
		stores[addr] = st
	}
	s.storesMtx.RUnlock()

	level.Debug(s.logger).Log("msg", "starting updating storeAPIs", "cachedStores", len(stores))

	activeStores := s.getActiveStores(ctx, stores)
	level.Debug(s.logger).Log("msg", "checked requested storeAPIs", "activeStores", len(activeStores), "cachedStores", len(stores))

	stats := newStoreAPIStats()

	// Close stores that where not active this time (are not in active stores map).
	for addr, st := range stores {
		if _, ok := activeStores[addr]; ok {
			stats[st.StoreType()][labelpb.PromLabelSetsToString(st.LabelSets())]++
			continue
		}

		st.Close()
		delete(stores, addr)
		s.updateStoreStatus(st, errors.New(unhealthyStoreMessage))
		level.Info(s.logger).Log("msg", unhealthyStoreMessage, "address", addr, "extLset", labelpb.PromLabelSetsToString(st.LabelSets()))
	}

	// Add stores that are not yet in stores.
	for addr, st := range activeStores {
		if _, ok := stores[addr]; ok {
			continue
		}

		extLset := labelpb.PromLabelSetsToString(st.LabelSets())

		// All producers should have unique external labels. While this does not check only StoreAPIs connected to
		// this querier this allows to notify early user about misconfiguration. Warn only. This is also detectable from metric.
		if st.StoreType() != nil &&
			(st.StoreType() == component.Sidecar || st.StoreType() == component.Rule) &&
			stats[component.Sidecar][extLset]+stats[component.Rule][extLset] > 0 {

			level.Warn(s.logger).Log("msg", "found duplicate storeAPI producer (sidecar or ruler). This is not advices as it will malform data in in the same bucket",
				"address", addr, "extLset", extLset, "duplicates", fmt.Sprintf("%v", stats[component.Sidecar][extLset]+stats[component.Rule][extLset]+1))
		}
		stats[st.StoreType()][extLset]++

		stores[addr] = st
		s.updateStoreStatus(st, nil)

		if st.HasRulesAPI() {
			level.Info(s.logger).Log("msg", "adding new rulesAPI to query storeset", "address", addr)
		}

		level.Info(s.logger).Log("msg", "adding new storeAPI to query storeset", "address", addr, "extLset", extLset)
	}

	s.storesMetric.Update(stats)
	s.storesMtx.Lock()
	s.stores = stores
	s.storesMtx.Unlock()

	s.cleanUpStoreStatuses(stores)
}

func (s *StoreSet) getActiveStores(ctx context.Context, stores map[string]*storeRef) map[string]*storeRef {
	var (
		// UNIQUE?
		activeStores = make(map[string]*storeRef, len(stores))
		mtx          sync.Mutex
		wg           sync.WaitGroup

		storeAddrSet    = make(map[string]struct{})
		ruleAddrSet     = make(map[string]struct{})
		metadataAddrSet = make(map[string]struct{})
	)

	// Gather active stores map concurrently. Build new store if does not exist already.
	for _, ruleSpec := range s.ruleSpecs() {
		ruleAddrSet[ruleSpec.Addr()] = struct{}{}
	}

	// Gather active stores map concurrently. Build new store if does not exist already.
	for _, metadataSpec := range s.metadataSpecs() {
		metadataAddrSet[metadataSpec.Addr()] = struct{}{}
	}

	// Gather healthy stores map concurrently. Build new store if does not exist already.
	for _, storeSpec := range s.storeSpecs() {
		if _, ok := storeAddrSet[storeSpec.Addr()]; ok {
			level.Warn(s.logger).Log("msg", "duplicated address in store nodes", "address", storeSpec.Addr())
			continue
		}
		storeAddrSet[storeSpec.Addr()] = struct{}{}

		wg.Add(1)
		go func(spec StoreSpec) {
			defer wg.Done()

			addr := spec.Addr()

			ctx, cancel := context.WithTimeout(ctx, s.gRPCInfoCallTimeout)
			defer cancel()

			st, seenAlready := stores[addr]
			if !seenAlready {
				// New store or was unactive and was removed in the past - create new one.
				conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
				if err != nil {
					s.updateStoreStatus(&storeRef{addr: addr}, err)
					level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "dialing connection"), "address", addr)
					return
				}

				st = &storeRef{StoreClient: storepb.NewStoreClient(conn), storeType: component.UnknownStoreAPI, cc: conn, addr: addr, logger: s.logger}
			}

			var rule rulespb.RulesClient
			if _, ok := ruleAddrSet[addr]; ok {
				rule = rulespb.NewRulesClient(st.cc)
			}

			var metadata metadatapb.MetadataClient
			if _, ok := metadataAddrSet[addr]; ok {
				metadata = metadatapb.NewMetadataClient(st.cc)
			}

			// Check existing or new store. Is it healthy? What are current metadata?
			labelSets, minTime, maxTime, storeType, err := spec.Metadata(ctx, st.StoreClient)
			if err != nil {
				if !seenAlready && !spec.StrictStatic() {
					// Close only if new and not a strict static node.
					// Unactive `s.stores` will be closed later on.
					st.Close()
				}
				s.updateStoreStatus(st, err)
				level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "getting metadata"), "address", addr)

				if !spec.StrictStatic() {
					return
				}

				// Still keep it around if static & strict mode enabled.
				mtx.Lock()
				defer mtx.Unlock()

				activeStores[addr] = st
				return
			}

			s.updateStoreStatus(st, nil)
			st.Update(labelSets, minTime, maxTime, storeType, rule, metadata)

			mtx.Lock()
			defer mtx.Unlock()

			activeStores[addr] = st
		}(storeSpec)
	}
	wg.Wait()

	for ruleAddr := range ruleAddrSet {
		if _, ok := storeAddrSet[ruleAddr]; !ok {
			level.Warn(s.logger).Log("msg", "ignored rule store", "address", ruleAddr)
		}
	}
	return activeStores
}

func (s *StoreSet) updateStoreStatus(store *storeRef, err error) {
	s.storesStatusesMtx.Lock()
	defer s.storesStatusesMtx.Unlock()

	status := StoreStatus{Name: store.addr}
	prev, ok := s.storeStatuses[store.addr]
	if ok {
		status = *prev
	}

	if err == nil {
		status.LastCheck = time.Now()
		mint, maxt := store.TimeRange()
		status.LabelSets = store.LabelSets()
		status.StoreType = store.StoreType()
		status.MinTime = mint
		status.MaxTime = maxt
		status.LastError = nil
	} else {
		status.LastError = &stringError{originalErr: err}
	}

	s.storeStatuses[store.addr] = &status
}

func (s *StoreSet) GetStoreStatus() []StoreStatus {
	s.storesStatusesMtx.RLock()
	defer s.storesStatusesMtx.RUnlock()

	statuses := make([]StoreStatus, 0, len(s.storeStatuses))
	for _, v := range s.storeStatuses {
		statuses = append(statuses, *v)
	}

	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []store.Client {
	s.storesMtx.RLock()
	defer s.storesMtx.RUnlock()

	stores := make([]store.Client, 0, len(s.stores))
	for _, st := range s.stores {
		stores = append(stores, st)
	}
	return stores
}

// GetRulesClients returns a list of all active rules clients.
func (s *StoreSet) GetRulesClients() []rulespb.RulesClient {
	s.storesMtx.RLock()
	defer s.storesMtx.RUnlock()

	rules := make([]rulespb.RulesClient, 0, len(s.stores))
	for _, st := range s.stores {
		if st.HasRulesAPI() {
			rules = append(rules, st.rule)
		}
	}
	return rules
}

// GetMetadataClients returns a list of all active metadata clients.
func (s *StoreSet) GetMetadataClients() []metadatapb.MetadataClient {
	s.storesMtx.RLock()
	defer s.storesMtx.RUnlock()

	metadataClients := make([]metadatapb.MetadataClient, 0, len(s.stores))
	for _, st := range s.stores {
		if st.HasMetadataAPI() {
			metadataClients = append(metadataClients, st.metadata)
		}
	}
	return metadataClients
}

func (s *StoreSet) Close() {
	s.storesMtx.Lock()
	defer s.storesMtx.Unlock()

	for _, st := range s.stores {
		st.Close()
	}
	s.stores = map[string]*storeRef{}
}

func (s *StoreSet) cleanUpStoreStatuses(stores map[string]*storeRef) {
	s.storesStatusesMtx.Lock()
	defer s.storesStatusesMtx.Unlock()

	now := time.Now()
	for addr, status := range s.storeStatuses {
		if _, ok := stores[addr]; ok {
			continue
		}

		if now.Sub(status.LastCheck) >= s.unhealthyStoreTimeout {
			delete(s.storeStatuses, addr)
		}
	}
}
