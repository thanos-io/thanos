// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/thanos-io/objstore"

	"github.com/thanos-io/thanos/pkg/api/status"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type TSDBStats interface {
	// TenantStats returns TSDB head stats for the given tenants.
	// If no tenantIDs are provided, stats for all tenants are returned.
	TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []status.TenantStats
}

type MultiTSDB struct {
	dataDir         string
	logger          log.Logger
	reg             prometheus.Registerer
	tsdbOpts        *tsdb.Options
	tenantLabelName string
	labels          labels.Labels
	bucket          objstore.Bucket

	mtx                   *sync.RWMutex
	tenants               map[string]*tenant
	allowOutOfOrderUpload bool
	hashFunc              metadata.HashFunc
	hashringConfigs       []HashringConfig

	tsdbClients           []store.Client
	tsdbClientsNeedUpdate bool

	exemplarClients           map[string]*exemplars.TSDB
	exemplarClientsNeedUpdate bool

	metricNameFilterEnabled bool
}

// MultiTSDBOption is a functional option for MultiTSDB.
type MultiTSDBOption func(mt *MultiTSDB)

// WithMetricNameFilterEnabled enables metric name filtering on TSDB clients.
func WithMetricNameFilterEnabled() MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.metricNameFilterEnabled = true
	}
}

// NewMultiTSDB creates new MultiTSDB.
// NOTE: Passed labels must be sorted lexicographically (alphabetically).
func NewMultiTSDB(
	dataDir string,
	l log.Logger,
	reg prometheus.Registerer,
	tsdbOpts *tsdb.Options,
	labels labels.Labels,
	tenantLabelName string,
	bucket objstore.Bucket,
	allowOutOfOrderUpload bool,
	hashFunc metadata.HashFunc,
	options ...MultiTSDBOption,
) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	mt := &MultiTSDB{
		dataDir:                   dataDir,
		logger:                    log.With(l, "component", "multi-tsdb"),
		reg:                       reg,
		tsdbOpts:                  tsdbOpts,
		mtx:                       &sync.RWMutex{},
		tenants:                   map[string]*tenant{},
		labels:                    labels,
		tsdbClientsNeedUpdate:     true,
		exemplarClientsNeedUpdate: true,
		tenantLabelName:           tenantLabelName,
		bucket:                    bucket,
		allowOutOfOrderUpload:     allowOutOfOrderUpload,
		hashFunc:                  hashFunc,
	}

	for _, option := range options {
		option(mt)
	}

	return mt
}

type localClient struct {
	store *store.TSDBStore

	client storepb.StoreClient
}

func newLocalClient(store *store.TSDBStore) *localClient {
	return &localClient{
		store:  store,
		client: storepb.ServerAsClient(store),
	}
}

func (l *localClient) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storepb.Store_SeriesClient, error) {
	return l.client.Series(ctx, in, opts...)
}

func (l *localClient) LabelNames(ctx context.Context, in *storepb.LabelNamesRequest, opts ...grpc.CallOption) (*storepb.LabelNamesResponse, error) {
	return l.store.LabelNames(ctx, in)
}

func (l *localClient) LabelValues(ctx context.Context, in *storepb.LabelValuesRequest, opts ...grpc.CallOption) (*storepb.LabelValuesResponse, error) {
	return l.store.LabelValues(ctx, in)
}

func (l *localClient) Matches(matchers []*labels.Matcher) bool {
	return l.store.Matches(matchers)
}

func (l *localClient) LabelSets() []labels.Labels {
	return labelpb.ZLabelSetsToPromLabelSets(l.store.LabelSet()...)
}

func (l *localClient) TimeRange() (mint int64, maxt int64) {
	return l.store.TimeRange()
}

func (l *localClient) TSDBInfos() []infopb.TSDBInfo {
	labelsets := l.store.LabelSet()
	if len(labelsets) == 0 {
		return []infopb.TSDBInfo{}
	}

	mint, maxt := l.store.TimeRange()
	return []infopb.TSDBInfo{
		{
			Labels:  labelsets[0],
			MinTime: mint,
			MaxTime: maxt,
		},
	}
}

func (l *localClient) String() string {
	mint, maxt := l.store.TimeRange()
	return fmt.Sprintf(
		"LabelSets: %v MinTime: %d MaxTime: %d",
		labelpb.PromLabelSetsToString(l.LabelSets()), mint, maxt,
	)
}

func (l *localClient) Addr() (string, bool) {
	return "", true
}

func (l *localClient) SupportsSharding() bool {
	return true
}

func (l *localClient) SupportsWithoutReplicaLabels() bool {
	return true
}

type tenant struct {
	readyS        *ReadyStorage
	storeTSDB     *store.TSDBStore
	exemplarsTSDB *exemplars.TSDB
	ship          *shipper.Shipper

	mtx  *sync.RWMutex
	tsdb *tsdb.DB

	// For tests.
	blocksToDeleteFn func(db *tsdb.DB) tsdb.BlocksToDeleteFunc
}

func (t *tenant) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.tsdb == nil {
		return nil
	}

	deletable := t.blocksToDeleteFn(t.tsdb)(blocks)
	if t.ship == nil {
		return deletable
	}

	uploaded := t.ship.UploadedBlocks()
	for deletableID := range deletable {
		if _, ok := uploaded[deletableID]; !ok {
			delete(deletable, deletableID)
		}
	}

	return deletable
}

func newTenant() *tenant {
	return &tenant{
		readyS: &ReadyStorage{},
		mtx:    &sync.RWMutex{},
	}
}

func (t *tenant) readyStorage() *ReadyStorage {
	return t.readyS
}

func (t *tenant) client() store.Client {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	tsdbStore := t.storeTSDB
	if tsdbStore == nil {
		return nil
	}

	return newLocalClient(tsdbStore)
}

func (t *tenant) exemplars() *exemplars.TSDB {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.exemplarsTSDB
}

func (t *tenant) shipper() *shipper.Shipper {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.ship
}

func (t *tenant) set(storeTSDB *store.TSDBStore, tenantTSDB *tsdb.DB, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB) {
	t.readyS.Set(tenantTSDB)
	t.mtx.Lock()
	t.setComponents(storeTSDB, ship, exemplarsTSDB, tenantTSDB)
	t.mtx.Unlock()
}

func (t *tenant) setComponents(storeTSDB *store.TSDBStore, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB, tenantTSDB *tsdb.DB) {
	if storeTSDB == nil && t.storeTSDB != nil {
		t.storeTSDB.Close()
	}
	t.storeTSDB = storeTSDB
	t.ship = ship
	t.exemplarsTSDB = exemplarsTSDB
	t.tsdb = tenantTSDB
}

func (t *MultiTSDB) Open() error {
	if err := os.MkdirAll(t.dataDir, 0750); err != nil {
		return err
	}

	files, err := os.ReadDir(t.dataDir)
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, f := range files {
		f := f
		if !f.IsDir() {
			continue
		}

		g.Go(func() error {
			_, err := t.getOrLoadTenant(f.Name(), true)
			return err
		})
	}

	return g.Wait()
}

func (t *MultiTSDB) Flush() error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	errmtx := &sync.Mutex{}
	merr := errutil.MultiError{}
	wg := &sync.WaitGroup{}
	for id, tenant := range t.tenants {
		db := tenant.readyStorage().Get()
		if db == nil {
			level.Error(t.logger).Log("msg", "flushing TSDB failed; not ready", "tenant", id)
			continue
		}
		level.Info(t.logger).Log("msg", "flushing TSDB", "tenant", id)
		wg.Add(1)
		go func() {
			if err := t.flushHead(db); err != nil {
				errmtx.Lock()
				merr.Add(err)
				errmtx.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return merr.Err()
}

func (t *MultiTSDB) flushHead(db *tsdb.DB) error {
	head := db.Head()
	if head.MinTime() == head.MaxTime() {
		return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()))
	}
	blockAlignedMaxt := head.MaxTime() - (head.MaxTime() % t.tsdbOpts.MaxBlockDuration)
	// Flush a well aligned TSDB block.
	if err := db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), blockAlignedMaxt-1)); err != nil {
		return err
	}
	// Flush the remainder of the head.
	return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()-1))
}

func (t *MultiTSDB) Close() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	merr := errutil.MultiError{}
	for id, tenant := range t.tenants {
		db := tenant.readyStorage().Get()
		if db == nil {
			level.Error(t.logger).Log("msg", "closing TSDB failed; not ready", "tenant", id)
			continue
		}
		level.Info(t.logger).Log("msg", "closing TSDB", "tenant", id)
		merr.Add(db.Close())
	}
	return merr.Err()
}

// Prune flushes and closes the TSDB for tenants that haven't received
// any new samples for longer than the TSDB retention period.
func (t *MultiTSDB) Prune(ctx context.Context) error {
	// Retention of 0 means infinite retention.
	if t.tsdbOpts.RetentionDuration == 0 {
		return nil
	}
	level.Info(t.logger).Log("msg", "Running pruning job")

	var (
		wg   sync.WaitGroup
		merr errutil.SyncMultiError

		prunedTenants []string
		pmtx          sync.Mutex
	)
	t.mtx.RLock()
	for tenantID, tenantInstance := range t.tenants {
		wg.Add(1)
		go func(tenantID string, tenantInstance *tenant) {
			defer wg.Done()
			tlog := log.With(t.logger, "tenant", tenantID)
			pruned, err := t.pruneTSDB(ctx, tlog, tenantInstance)
			if err != nil {
				merr.Add(err)
				return
			}

			if pruned {
				pmtx.Lock()
				defer pmtx.Unlock()
				prunedTenants = append(prunedTenants, tenantID)
			}
		}(tenantID, tenantInstance)
	}
	wg.Wait()
	t.mtx.RUnlock()

	t.mtx.Lock()
	defer t.mtx.Unlock()
	for _, tenantID := range prunedTenants {
		// Check that the tenant hasn't been reinitialized in-between locks.
		if t.tenants[tenantID].readyStorage().get() != nil {
			continue
		}

		level.Info(t.logger).Log("msg", "Pruned tenant", "tenant", tenantID)
		delete(t.tenants, tenantID)
		t.tsdbClientsNeedUpdate = true
		t.exemplarClientsNeedUpdate = true
	}

	return merr.Err()
}

// pruneTSDB removes a TSDB if its past the retention period.
// It compacts the TSDB head, sends all remaining blocks to S3 and removes the TSDB from disk.
func (t *MultiTSDB) pruneTSDB(ctx context.Context, logger log.Logger, tenantInstance *tenant) (pruned bool, rerr error) {
	tenantTSDB := tenantInstance.readyStorage()
	if tenantTSDB == nil {
		return false, nil
	}
	tenantTSDB.mtx.RLock()
	if tenantTSDB.a == nil || tenantTSDB.a.db == nil {
		tenantTSDB.mtx.RUnlock()
		return false, nil
	}

	tdb := tenantTSDB.a.db
	head := tdb.Head()
	if head.MaxTime() < 0 {
		tenantTSDB.mtx.RUnlock()
		return false, nil
	}

	sinceLastAppendMillis := time.Since(time.UnixMilli(head.MaxTime())).Milliseconds()
	compactThreshold := int64(1.5 * float64(t.tsdbOpts.MaxBlockDuration))
	if sinceLastAppendMillis <= compactThreshold {
		tenantTSDB.mtx.RUnlock()
		return false, nil
	}
	tenantTSDB.mtx.RUnlock()

	// Acquire a write lock and check that no writes have occurred in-between locks.
	tenantTSDB.mtx.Lock()
	defer tenantTSDB.mtx.Unlock()

	// Make sure the shipper is not running in parallel.
	tenantInstance.mtx.Lock()
	shipper := tenantInstance.ship
	tenantInstance.ship = nil
	tenantInstance.mtx.Unlock()

	defer func() {
		if pruned {
			return
		}
		// If the tenant was not pruned, re-enable the shipper.
		tenantInstance.mtx.Lock()
		tenantInstance.ship = shipper
		tenantInstance.mtx.Unlock()
	}()

	sinceLastAppendMillis = time.Since(time.UnixMilli(head.MaxTime())).Milliseconds()
	if sinceLastAppendMillis <= compactThreshold {
		return false, nil
	}

	level.Info(logger).Log("msg", "Compacting tenant")
	if err := t.flushHead(tdb); err != nil {
		return false, err
	}

	if sinceLastAppendMillis <= t.tsdbOpts.RetentionDuration {
		return false, nil
	}

	level.Info(logger).Log("msg", "Pruning tenant")
	if shipper != nil {
		// No other code can reach this shipper anymore so enable it again to be able to sync manually.
		uploaded, err := shipper.Sync(ctx)
		if err != nil {
			return false, err
		}

		if uploaded > 0 {
			level.Info(logger).Log("msg", "Uploaded head block")
		}
	}

	if err := tdb.Close(); err != nil {
		return false, err
	}

	if err := os.RemoveAll(tdb.Dir()); err != nil {
		return false, err
	}

	tenantInstance.mtx.Lock()
	tenantInstance.readyS.set(nil)
	tenantInstance.setComponents(nil, nil, nil, nil)
	tenantInstance.mtx.Unlock()

	return true, nil
}

func (t *MultiTSDB) Sync(ctx context.Context) (int, error) {
	if t.bucket == nil {
		return 0, errors.New("bucket is not specified, Sync should not be invoked")
	}

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	var (
		errmtx   = &sync.Mutex{}
		merr     = errutil.MultiError{}
		wg       = &sync.WaitGroup{}
		uploaded atomic.Int64
	)

	for tenantID, tenant := range t.tenants {
		level.Debug(t.logger).Log("msg", "uploading block for tenant", "tenant", tenantID)
		s := tenant.shipper()
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			up, err := s.Sync(ctx)
			if err != nil {
				errmtx.Lock()
				merr.Add(errors.Wrap(err, "upload"))
				errmtx.Unlock()
			}
			uploaded.Add(int64(up))
			wg.Done()
		}()
	}
	wg.Wait()
	return int(uploaded.Load()), merr.Err()
}

func (t *MultiTSDB) RemoveLockFilesIfAny() error {
	fis, err := os.ReadDir(t.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	merr := errutil.MultiError{}
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(t.defaultTenantDataDir(fi.Name()), "lock")); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			merr.Add(err)
			continue
		}
		level.Info(t.logger).Log("msg", "a leftover lockfile found and removed", "tenant", fi.Name())
	}
	return merr.Err()
}

func (t *MultiTSDB) TSDBLocalClients() []store.Client {
	t.mtx.RLock()
	if !t.tsdbClientsNeedUpdate {
		t.mtx.RUnlock()
		return t.tsdbClients
	}

	t.mtx.RUnlock()
	t.mtx.Lock()
	if !t.tsdbClientsNeedUpdate {
		return t.tsdbClients
	}
	defer t.mtx.Unlock()

	res := make([]store.Client, 0, len(t.tenants))
	for _, tenant := range t.tenants {
		client := tenant.client()
		if client != nil {
			res = append(res, client)
		}
	}

	t.tsdbClientsNeedUpdate = false
	t.tsdbClients = res

	return t.tsdbClients
}

func (t *MultiTSDB) TSDBExemplars() map[string]*exemplars.TSDB {
	t.mtx.RLock()
	if !t.exemplarClientsNeedUpdate {
		t.mtx.RUnlock()
		return t.exemplarClients
	}
	t.mtx.RUnlock()
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if !t.exemplarClientsNeedUpdate {
		return t.exemplarClients
	}

	res := make(map[string]*exemplars.TSDB, len(t.tenants))
	for k, tenant := range t.tenants {
		e := tenant.exemplars()
		if e != nil {
			res[k] = e
		}
	}

	t.exemplarClientsNeedUpdate = false
	t.exemplarClients = res
	return t.exemplarClients
}

func (t *MultiTSDB) TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []status.TenantStats {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	if len(tenantIDs) == 0 {
		for tenantID := range t.tenants {
			tenantIDs = append(tenantIDs, tenantID)
		}
	}

	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		result = make([]status.TenantStats, 0, len(t.tenants))
	)
	for _, tenantID := range tenantIDs {
		tenantInstance, ok := t.tenants[tenantID]
		if !ok {
			continue
		}

		wg.Add(1)
		go func(tenantID string, tenantInstance *tenant) {
			defer wg.Done()
			db := tenantInstance.readyS.Get()
			if db == nil {
				return
			}
			stats := db.Head().Stats(statsByLabelName, limit)

			mu.Lock()
			defer mu.Unlock()
			result = append(result, status.TenantStats{
				Tenant: tenantID,
				Stats:  stats,
			})
		}(tenantID, tenantInstance)
	}
	wg.Wait()

	sort.Slice(result, func(i, j int) bool {
		return result[i].Tenant < result[j].Tenant
	})
	return result
}

func (t *MultiTSDB) startTSDB(logger log.Logger, tenantID string, tenant *tenant) error {
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"tenant": tenantID}, t.reg)
	reg = NewUnRegisterer(reg)

	initialLset := labelpb.ExtendSortedLabels(t.labels, labels.FromStrings(t.tenantLabelName, tenantID))
	lset := t.extractTenantsLabels(tenantID, initialLset)
	dataDir := t.defaultTenantDataDir(tenantID)

	level.Info(logger).Log("msg", "opening TSDB")
	opts := *t.tsdbOpts
	opts.BlocksToDelete = tenant.blocksToDelete
	tenant.blocksToDeleteFn = tsdb.DefaultBlocksToDelete

	// NOTE(GiedriusS): always set to false to properly handle OOO samples - OOO samples are written into the WBL
	// which gets later converted into a block. Without setting this flag to false, the block would get compacted
	// into other ones. This presents a race between compaction and the shipper (if it is configured to upload compacted blocks).
	// Hence, avoid this situation by disabling overlapping compaction. Vertical compaction must be enabled on the compactor.
	opts.EnableOverlappingCompaction = false
	s, err := tsdb.Open(
		dataDir,
		logger,
		reg,
		&opts,
		nil,
	)
	if err != nil {
		t.mtx.Lock()
		delete(t.tenants, tenantID)
		t.tsdbClientsNeedUpdate = true
		t.exemplarClientsNeedUpdate = true
		t.mtx.Unlock()
		return err
	}
	var ship *shipper.Shipper
	if t.bucket != nil {
		ship = shipper.New(
			logger,
			reg,
			dataDir,
			t.bucket,
			func() labels.Labels { return lset },
			metadata.ReceiveSource,
			nil,
			t.allowOutOfOrderUpload,
			t.hashFunc,
			shipper.DefaultMetaFilename,
		)
	}
	options := []store.TSDBStoreOption{}
	if t.metricNameFilterEnabled {
		options = append(options, store.WithCuckooMetricNameStoreFilter())
	}
	tenant.set(store.NewTSDBStore(logger, s, component.Receive, lset, options...), s, ship, exemplars.NewTSDB(s, lset))
	level.Info(logger).Log("msg", "TSDB is now ready")
	return nil
}

func (t *MultiTSDB) defaultTenantDataDir(tenantID string) string {
	return path.Join(t.dataDir, tenantID)
}

func (t *MultiTSDB) getOrLoadTenant(tenantID string, blockingStart bool) (*tenant, error) {
	// Fast path, as creating tenants is a very rare operation.
	t.mtx.RLock()
	tenant, exist := t.tenants[tenantID]
	t.mtx.RUnlock()
	if exist {
		return tenant, nil
	}

	// Slow path needs to lock fully and attempt to read again to prevent race
	// conditions, where since the fast path was tried, there may have actually
	// been the same tenant inserted in the map.
	t.mtx.Lock()
	tenant, exist = t.tenants[tenantID]
	if exist {
		t.mtx.Unlock()
		return tenant, nil
	}

	tenant = newTenant()
	t.tenants[tenantID] = tenant
	t.tsdbClientsNeedUpdate = true
	t.exemplarClientsNeedUpdate = true
	t.mtx.Unlock()

	logger := log.With(t.logger, "tenant", tenantID)
	if !blockingStart {
		go func() {
			if err := t.startTSDB(logger, tenantID, tenant); err != nil {
				level.Error(logger).Log("msg", "failed to start tsdb asynchronously", "err", err)
			}
		}()
		return tenant, nil
	}
	return tenant, t.startTSDB(logger, tenantID, tenant)
}

func (t *MultiTSDB) TenantAppendable(tenantID string) (Appendable, error) {
	tenant, err := t.getOrLoadTenant(tenantID, false)
	if err != nil {
		return nil, err
	}
	return tenant.readyStorage(), nil
}

func (t *MultiTSDB) SetHashringConfig(cfg []HashringConfig) error {
	t.hashringConfigs = cfg

	// If a tenant's already existed in MultiTSDB, update its label set
	// from the latest []HashringConfig.
	// In case one tenant appears in multiple hashring configs,
	// only the label set from the first hashring config is applied.
	// This is the same logic as startTSDB.
	updatedTenants := make([]string, 0)
	for _, hc := range t.hashringConfigs {
		for _, tenantID := range hc.Tenants {
			if slices.Contains(updatedTenants, tenantID) {
				continue
			}
			if t.tenants[tenantID] != nil {
				updatedTenants = append(updatedTenants, tenantID)

				lset := labelpb.ExtendSortedLabels(t.labels, labels.FromStrings(t.tenantLabelName, tenantID))
				lset = labelpb.ExtendSortedLabels(hc.ExternalLabels, lset)

				if t.tenants[tenantID].ship != nil {
					t.tenants[tenantID].ship.SetLabels(lset)
				}
				t.tenants[tenantID].storeTSDB.SetExtLset(lset)
				t.tenants[tenantID].exemplarsTSDB.SetExtLabels(lset)
			}
		}
	}

	return nil
}

// ErrNotReady is returned if the underlying storage is not ready yet.
var ErrNotReady = errors.New("TSDB not ready")

// ReadyStorage implements the Storage interface while allowing to set the actual
// storage at a later point in time.
// TODO: Replace this with upstream Prometheus implementation when it is exposed.
type ReadyStorage struct {
	mtx sync.RWMutex
	a   *adapter
}

// Set the storage.
func (s *ReadyStorage) Set(db *tsdb.DB) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.set(&adapter{db: db})
}

func (s *ReadyStorage) set(a *adapter) {
	s.a = a
}

// Get the storage.
func (s *ReadyStorage) Get() *tsdb.DB {
	if x := s.get(); x != nil {
		return x.db
	}
	return nil
}

func (s *ReadyStorage) get() *adapter {
	s.mtx.RLock()
	x := s.a
	s.mtx.RUnlock()
	return x
}

// StartTime implements the Storage interface.
func (s *ReadyStorage) StartTime() (int64, error) {
	return 0, errors.New("not implemented")
}

// Querier implements the Storage interface.
func (s *ReadyStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(mint, maxt)
	}
	return nil, ErrNotReady
}

// ExemplarQuerier implements the Storage interface.
func (s *ReadyStorage) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	if x := s.get(); x != nil {
		return x.ExemplarQuerier(ctx)
	}
	return nil, ErrNotReady
}

// Appender implements the Storage interface.
func (s *ReadyStorage) Appender(ctx context.Context) (storage.Appender, error) {
	if x := s.get(); x != nil {
		return x.Appender(ctx)
	}
	return nil, ErrNotReady
}

// Close implements the Storage interface.
func (s *ReadyStorage) Close() error {
	if x := s.Get(); x != nil {
		return x.Close()
	}
	return nil
}

// adapter implements a storage.Storage around TSDB.
type adapter struct {
	db *tsdb.DB
}

// StartTime implements the Storage interface.
func (a adapter) StartTime() (int64, error) {
	return 0, errors.New("not implemented")
}

func (a adapter) Querier(mint, maxt int64) (storage.Querier, error) {
	return a.db.Querier(mint, maxt)
}

func (a adapter) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return a.db.ExemplarQuerier(ctx)
}

// Appender returns a new appender against the storage.
func (a adapter) Appender(ctx context.Context) (storage.Appender, error) {
	return a.db.Appender(ctx), nil
}

// Close closes the storage and all its underlying resources.
func (a adapter) Close() error {
	return a.db.Close()
}

// UnRegisterer is a Prometheus registerer that
// ensures that collectors can be registered
// by unregistering already-registered collectors.
// FlushableStorage uses this registerer in order
// to not lose metric values between DB flushes.
//
// This type cannot embed the inner registerer, because Prometheus since
// v2.39.0 is wrapping the Registry with prometheus.WrapRegistererWithPrefix.
// This wrapper will call the Register function of the wrapped registerer.
// If UnRegisterer is the wrapped registerer, this would end up calling the
// inner registerer's Register, which doesn't implement the "unregister" logic
// that this type intends to use.
type UnRegisterer struct {
	innerReg prometheus.Registerer
}

func NewUnRegisterer(inner prometheus.Registerer) *UnRegisterer {
	return &UnRegisterer{innerReg: inner}
}

// Register registers the given collector. If it's already registered, it will
// be unregistered and registered.
func (u *UnRegisterer) Register(c prometheus.Collector) error {
	if err := u.innerReg.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if ok = u.innerReg.Unregister(c); !ok {
				panic("unable to unregister existing collector")
			}
			u.innerReg.MustRegister(c)
			return nil
		}
		return err
	}
	return nil
}

// Unregister unregisters the given collector.
func (u *UnRegisterer) Unregister(c prometheus.Collector) bool {
	return u.innerReg.Unregister(c)
}

// MustRegister registers the given collectors. It panics if an error happens.
// Note that if a collector is already registered it will be re-registered
// without panicking.
func (u *UnRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := u.Register(c); err != nil {
			panic(err)
		}
	}
}

// extractTenantsLabels extracts tenant's external labels from hashring configs.
// If one tenant appears in multiple hashring configs,
// only the external label set from the first hashring config is applied.
func (t *MultiTSDB) extractTenantsLabels(tenantID string, initialLset labels.Labels) labels.Labels {
	for _, hc := range t.hashringConfigs {
		for _, tenant := range hc.Tenants {
			if tenant != tenantID {
				continue
			}
			return labelpb.ExtendSortedLabels(hc.ExternalLabels, initialLset)
		}
	}
	return initialLset
}
