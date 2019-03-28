package query

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
)

const (
	unhealthyStoreMessage = "removing store because it's unhealthy or does not exist"
	droppingStoreMessage  = "dropping store, external labels are not unique"
)

type StoreSpec interface {
	// Addr returns StoreAPI Address for the store spec. It is used as ID for store.
	Addr() string
	// Metadata returns current labels, store type and min, max ranges for store.
	// It can change for every call for this method.
	// If metadata call fails we assume that store is no longer accessible and we should not use it.
	// NOTE: It is implementation responsibility to retry until context timeout, but a caller responsibility to manage
	// given store connection.
	Metadata(ctx context.Context, client storepb.StoreClient) (labels []storepb.Label, mint int64, maxt int64, err error)
}

type StoreStatus struct {
	Name      string
	LastCheck time.Time
	LastError error
	Labels    []storepb.Label
	StoreType component.StoreAPI
	MinTime   int64
	MaxTime   int64
}

type grpcStoreSpec struct {
	addr string
}

// NewGRPCStoreSpec creates store pure gRPC spec.
// It uses Info gRPC call to get Metadata.
func NewGRPCStoreSpec(addr string) StoreSpec {
	return &grpcStoreSpec{addr: addr}
}

func (s *grpcStoreSpec) Addr() string {
	// API addr should not change between state changes.
	return s.addr
}

// Metadata method for gRPC store API tries to reach host Info method until context timeout. If we are unable to get metadata after
// that time, we assume that the host is unhealthy and return error.
func (s *grpcStoreSpec) Metadata(ctx context.Context, client storepb.StoreClient) (labels []storepb.Label, mint int64, maxt int64, err error) {
	resp, err := client.Info(ctx, &storepb.InfoRequest{}, grpc.FailFast(false))
	if err != nil {
		return nil, 0, 0, errors.Wrapf(err, "fetching store info from %s", s.addr)
	}
	return resp.Labels, resp.MinTime, resp.MaxTime, nil
}

// StoreSet maintains a set of active stores. It is backed up by Store Specifications that are dynamically fetched on
// every Update() call.
type StoreSet struct {
	logger log.Logger

	// Store specifications can change dynamically. If some store is missing from the list, we assuming it is no longer
	// accessible and we close gRPC client for it.
	storeSpecs          func() []StoreSpec
	dialOpts            []grpc.DialOption
	gRPCInfoCallTimeout time.Duration

	mtx                  sync.RWMutex
	storesStatusesMtx    sync.RWMutex
	stores               map[string]*storeRef
	storeNodeConnections prometheus.Gauge
	externalLabelStores  map[string]int
	storeStatuses        map[string]*StoreStatus
}

type storeSetNodeCollector struct {
	externalLabelOccurrences func() map[string]int
}

var (
	nodeInfoDesc = prometheus.NewDesc(
		"thanos_store_node_info",
		"Number of nodes with the same external labels identified by their hash. If any time-series is larger than 1, external label uniqueness is not true",
		[]string{"external_labels"}, nil,
	)
)

func (c *storeSetNodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nodeInfoDesc
}

func (c *storeSetNodeCollector) Collect(ch chan<- prometheus.Metric) {
	externalLabelOccurrences := c.externalLabelOccurrences()
	for externalLabels, occurrences := range externalLabelOccurrences {
		ch <- prometheus.MustNewConstMetric(nodeInfoDesc, prometheus.GaugeValue, float64(occurrences), externalLabels)
	}
}

// NewStoreSet returns a new set of stores from cluster peers and statically configured ones.
func NewStoreSet(
	logger log.Logger,
	reg *prometheus.Registry,
	storeSpecs func() []StoreSpec,
	dialOpts []grpc.DialOption,
) *StoreSet {
	storeNodeConnections := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "thanos_store_nodes_grpc_connections",
		Help: "Number indicating current number of gRPC connection to store nodes. This indicates also to how many stores query node have access to.",
	})

	if logger == nil {
		logger = log.NewNopLogger()
	}
	if reg != nil {
		reg.MustRegister(storeNodeConnections)
	}
	if storeSpecs == nil {
		storeSpecs = func() []StoreSpec { return nil }
	}

	ss := &StoreSet{
		logger:               log.With(logger, "component", "storeset"),
		storeSpecs:           storeSpecs,
		dialOpts:             dialOpts,
		storeNodeConnections: storeNodeConnections,
		gRPCInfoCallTimeout:  10 * time.Second,
		externalLabelStores:  map[string]int{},
		stores:               make(map[string]*storeRef),
		storeStatuses:        make(map[string]*StoreStatus),
	}

	storeNodeCollector := &storeSetNodeCollector{externalLabelOccurrences: ss.externalLabelOccurrences}
	if reg != nil {
		reg.MustRegister(storeNodeCollector)
	}

	return ss
}

type storeRef struct {
	storepb.StoreClient

	mtx  sync.RWMutex
	cc   *grpc.ClientConn
	addr string

	// Meta (can change during runtime).
	labels    []storepb.Label
	storeType component.StoreAPI
	minTime   int64
	maxTime   int64

	logger log.Logger
}

func (s *storeRef) Update(labels []storepb.Label, minTime int64, maxTime int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	s.labels = labels
	s.minTime = minTime
	s.maxTime = maxTime
}

func (s *storeRef) Labels() []storepb.Label {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels
}

func (s *storeRef) TimeRange() (int64, int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.minTime, s.maxTime
}

func (s *storeRef) String() string {
	mint, maxt := s.TimeRange()
	return fmt.Sprintf("Addr: %s Labels: %v Mint: %d Maxt: %d", s.addr, s.Labels(), mint, maxt)
}

func (s *storeRef) Addr() string {
	return s.addr
}

func (s *storeRef) close() {
	runutil.CloseWithLogOnErr(s.logger, s.cc, fmt.Sprintf("store %v connection close", s.addr))
}

// Update updates the store set. It fetches current list of store specs from function and updates the fresh metadata
// from all stores.
func (s *StoreSet) Update(ctx context.Context) {
	healthyStores := s.getHealthyStores(ctx)

	// Record the number of occurrences of external label combinations for current store slice.
	externalLabelStores := map[string]int{}
	for _, st := range healthyStores {
		externalLabelStores[externalLabelsFromStore(st)]++
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Close stores that where not healthy this time (are not in healthy stores map).
	for addr, store := range s.stores {
		if _, ok := healthyStores[addr]; ok {
			continue
		}

		// Peer does not exists anymore.
		store.close()
		delete(s.stores, addr)
		s.updateStoreStatus(store, errors.New(unhealthyStoreMessage))
		level.Info(s.logger).Log("msg", unhealthyStoreMessage, "address", addr)
	}

	// Add stores that are not yet in s.stores.
	for addr, store := range healthyStores {
		if _, ok := s.stores[addr]; ok {
			s.updateStoreStatus(store, nil)
			continue
		}

		// Check if it has some external labels specified.
		// No external labels means strictly store gateway or ruler and it is fine to have access to multiple instances of them.
		//
		// Sidecar will error out if it will be configured with empty external labels.
		if len(store.Labels()) > 0 && externalLabelStores[externalLabelsFromStore(store)] != 1 {
			store.close()
			s.updateStoreStatus(store, errors.New(droppingStoreMessage))
			level.Warn(s.logger).Log("msg", droppingStoreMessage, "address", addr)
			continue
		}

		s.stores[addr] = store
		s.updateStoreStatus(store, nil)
		level.Info(s.logger).Log("msg", "adding new store to query storeset", "address", addr)
	}
	s.externalLabelStores = externalLabelStores
	s.storeNodeConnections.Set(float64(len(s.stores)))
}

func (s *StoreSet) getHealthyStores(ctx context.Context) map[string]*storeRef {
	var (
		unique = make(map[string]struct{})

		healthyStores = make(map[string]*storeRef, len(s.stores))
		mtx           sync.Mutex
		wg            sync.WaitGroup
	)

	// Gather healthy stores map concurrently. Build new store if does not exist already.
	for _, storeSpec := range s.storeSpecs() {
		if _, ok := unique[storeSpec.Addr()]; ok {
			level.Warn(s.logger).Log("msg", "duplicated address in gossip or static store nodes", "address", storeSpec.Addr())
			continue
		}
		unique[storeSpec.Addr()] = struct{}{}

		wg.Add(1)
		go func(spec StoreSpec) {
			defer wg.Done()

			addr := spec.Addr()

			ctx, cancel := context.WithTimeout(ctx, s.gRPCInfoCallTimeout)
			defer cancel()

			store, ok := s.stores[addr]
			if ok {
				// Check existing store. Is it healthy? What are current metadata?
				labels, minTime, maxTime, err := spec.Metadata(ctx, store.StoreClient)
				if err != nil {
					// Peer unhealthy. Do not include in healthy stores.
					s.updateStoreStatus(store, err)
					level.Warn(s.logger).Log("msg", "update of store node failed", "err", err, "address", addr)
					return
				}
				store.Update(labels, minTime, maxTime)
			} else {
				// New store or was unhealthy and was removed in the past - create new one.
				conn, err := grpc.DialContext(ctx, addr, s.dialOpts...)
				if err != nil {
					s.updateStoreStatus(&storeRef{addr: addr}, err)
					level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "dialing connection"), "address", addr)
					return
				}
				store = &storeRef{StoreClient: storepb.NewStoreClient(conn), cc: conn, addr: addr, logger: s.logger}

				// Initial info call for all types of stores (gossip + static) to check gRPC StoreAPI.
				resp, err := store.StoreClient.Info(ctx, &storepb.InfoRequest{}, grpc.FailFast(false))
				if err != nil {
					store.close()
					s.updateStoreStatus(store, err)
					level.Warn(s.logger).Log("msg", "update of store node failed", "err", errors.Wrap(err, "initial store client info fetch"), "address", addr)
					return
				}
				store.storeType = component.FromProto(resp.StoreType)
				store.Update(resp.Labels, resp.MinTime, resp.MaxTime)
			}

			mtx.Lock()
			defer mtx.Unlock()

			healthyStores[addr] = store
		}(storeSpec)
	}

	wg.Wait()

	return healthyStores
}

func externalLabelsFromStore(store *storeRef) string {
	tsdbLabels := labels.Labels{}
	for _, l := range store.labels {
		tsdbLabels = append(tsdbLabels, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	sort.Sort(tsdbLabels)

	return tsdbLabels.String()
}

func (s *StoreSet) updateStoreStatus(store *storeRef, err error) {
	s.storesStatusesMtx.Lock()
	defer s.storesStatusesMtx.Unlock()

	now := time.Now()
	s.storeStatuses[store.addr] = &StoreStatus{
		Name:      store.addr,
		LastError: err,
		LastCheck: now,
		Labels:    store.labels,
		StoreType: store.storeType,
		MinTime:   store.minTime,
		MaxTime:   store.maxTime,
	}
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

func (s *StoreSet) externalLabelOccurrences() map[string]int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	r := make(map[string]int, len(s.externalLabelStores))
	for k, v := range s.externalLabelStores {
		r[k] = v
	}

	return r
}

// Get returns a list of all active stores.
func (s *StoreSet) Get() []store.Client {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	stores := make([]store.Client, 0, len(s.stores))
	for _, st := range s.stores {
		stores = append(stores, st)
	}
	return stores
}

func (s *StoreSet) Close() {
	for _, st := range s.stores {
		st.close()
	}
}
