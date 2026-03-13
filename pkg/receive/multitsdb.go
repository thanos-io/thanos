// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"

	"github.com/thanos-io/thanos/pkg/api"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/logutil"
	"github.com/thanos-io/thanos/pkg/receive/expandedpostingscache"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type TSDBStats interface {
	// TenantStats returns TSDB head stats for the given tenants.
	// If no tenantIDs are provided, stats for all tenants are returned.
	TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []api.TenantStats
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
	skipCorruptedBlocks   bool
	hashFunc              metadata.HashFunc
	uploadConcurrency     int
	hashringConfigs       []HashringConfig

	matcherCache storecache.MatchersCache

	tsdbClients     []store.Client
	exemplarClients map[string]*exemplars.TSDB

	metricNameFilterEnabled bool

	headExpandedPostingsCacheSize  uint64
	blockExpandedPostingsCacheSize uint64

	initSingleFlight singleflight.Group

	gcImmediately bool
}

// MultiTSDBOption is a functional option for MultiTSDB.
type MultiTSDBOption func(mt *MultiTSDB)

// WithMetricNameFilterEnabled enables metric name filtering on TSDB clients.
func WithMetricNameFilterEnabled() MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.metricNameFilterEnabled = true
	}
}

func WithGCImmediately() MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.gcImmediately = true
	}
}

func WithHeadExpandedPostingsCacheSize(size uint64) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.headExpandedPostingsCacheSize = size
	}
}

func WithBlockExpandedPostingsCacheSize(size uint64) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.blockExpandedPostingsCacheSize = size
	}
}

func WithMatchersCache(cache storecache.MatchersCache) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.matcherCache = cache
	}
}

func WithUploadConcurrency(concurrency int) MultiTSDBOption {
	return func(s *MultiTSDB) {
		s.uploadConcurrency = concurrency
	}
}

/*
NewMultiTSDB creates a new MultiTSDB instance.

State machine for tenants:
- Modifying the state of MultiTSDB so we need to protect this with a lock.
  - First request comes in for tenant T. It is created automatically.
  - The TSDB of T is synchronously initialized if not already initialized.
  - The MultiTSDB clients list is updated.

Tenant T can be: active, GC ready / read-only, deleted.

Periodically, in a randomized manner compaction happens. This needs to be a tenant level operation so no need to lock MultiTSDB.
If the tenant is a read-only mode then we need to skip it.

Periodically, check if compaction needs to happen for each tenant. This is a tenant-level operation. Upload data if needed.

Periodically, prune tenants that have not received data for longer than retention period. First, mark that tenant as GC ready / read-only.
Only do this for tenants that have all blocks uploaded.

If a write comes in for a tenant that is GC ready / read-only, we need to switch it back to active mode.

After a tenant is marked as GC ready / read-only, check after 1 minute if no writes have happened. If no writes happened,
take a MultiTSDB lock and remove the tenant from the list. Update clients.

Invariants:
- Any object storage operations must not block reading or writing new samples.
*/
func NewMultiTSDB(
	dataDir string,
	l log.Logger,
	reg prometheus.Registerer,
	tsdbOpts *tsdb.Options,
	labels labels.Labels,
	tenantLabelName string,
	bucket objstore.Bucket,
	allowOutOfOrderUpload bool,
	skipCorruptedBlocks bool,
	hashFunc metadata.HashFunc,
	options ...MultiTSDBOption,
) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	mt := &MultiTSDB{
		dataDir:               dataDir,
		logger:                log.With(l, "component", "multi-tsdb"),
		reg:                   reg,
		tsdbOpts:              tsdbOpts,
		mtx:                   &sync.RWMutex{},
		tenants:               map[string]*tenant{},
		labels:                labels,
		tsdbClients:           make([]store.Client, 0),
		exemplarClients:       map[string]*exemplars.TSDB{},
		tenantLabelName:       tenantLabelName,
		bucket:                bucket,
		allowOutOfOrderUpload: allowOutOfOrderUpload,
		skipCorruptedBlocks:   skipCorruptedBlocks,
		hashFunc:              hashFunc,
		uploadConcurrency:     0,
		matcherCache:          storecache.NoopMatchersCache,
	}

	for _, option := range options {
		option(mt)
	}

	return mt
}

// testGetTenant returns the tenant with the given tenantID for testing purposes.
func (t *MultiTSDB) testGetTenant(tenantID string) *tenant {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.tenants[tenantID]
}

func (t *MultiTSDB) updateTSDBClients() {
	t.tsdbClients = t.tsdbClients[:0]
	for _, tenant := range t.tenants {
		client := tenant.client()
		if client != nil {
			t.tsdbClients = append(t.tsdbClients, client)
		}
	}
}

func (t *MultiTSDB) addTenantUnlocked(tenantID string, newTenant *tenant) {
	t.tenants[tenantID] = newTenant
	t.updateTSDBClients()
	if newTenant.exemplars() != nil {
		t.exemplarClients[tenantID] = newTenant.exemplars()
	}
}

func (t *MultiTSDB) addTenantLocked(tenantID string, newTenant *tenant) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.addTenantUnlocked(tenantID, newTenant)
}

func (t *MultiTSDB) removeTenantUnlocked(tenantID string) {
	delete(t.tenants, tenantID)
	delete(t.exemplarClients, tenantID)
	t.updateTSDBClients()
}

func (t *MultiTSDB) removeTenantLocked(tenantID string) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.removeTenantUnlocked(tenantID)
}

type localClient struct {
	store *store.TSDBStore

	client storepb.StoreClient
}

func newLocalClient(store *store.TSDBStore, readOnly atomic.Bool) *localClient {
	return &localClient{
		store:  store,
		client: storepb.ServerAsClient(store, readOnly),
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
		"MinTime: %d MaxTime: %d",
		mint, maxt,
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
	reg           *UnRegisterer
	doneC         chan struct{}

	closeOnce sync.Once

	readOnly atomic.Bool

	mtx  *sync.RWMutex
	tsdb *tsdb.DB

	// For tests.
	blocksToDeleteFn func(db *tsdb.DB) tsdb.BlocksToDeleteFunc

	retentionDuration int64
	logger            log.Logger

	tenantName string

	maxBlockDuration int64

	lastSuccessfulHeadCompaction atomic.Int64
}

// shouldBeMarkedInactive checks if the tenant should be marked as inactive / read-only.
func (t *tenant) shouldBeMarkedInactive() bool {
	// NOTE(GiedriusS): it could also happen that compaction is failing and it is not producing new blocks.
	// But if compaction is failing then that probably also means that the storage layer is hosed
	// and if that is the case then we cannot do anything about it anyway.
	head := t.tsdb.Head()
	if head.MaxTime() < 0 {
		return false
	}

	sinceLastAppendMillis := time.Since(time.UnixMilli(head.MaxTime())).Milliseconds()
	if sinceLastAppendMillis <= t.retentionDuration {
		return false
	}

	s := t.shipper()
	if s == nil {
		return true
	}

	// NOTE(GiedriusS): compaction needs to be successful so that we would ensure that all new blocks are uploaded.
	lastCompactionNs := t.lastSuccessfulHeadCompaction.Load()
	if lastCompactionNs == 0 {
		return false
	}

	if time.Since(time.Unix(0, lastCompactionNs)) > 6*time.Hour {
		level.Error(t.logger).Log("msg", "compaction is stuck so NOT marking tenant as inactive", "now", time.Now(), "last_compaction", time.Unix(0, lastCompactionNs), "diff", time.Since(time.Unix(0, lastCompactionNs)))
		return false
	}

	allBlocks, err := s.AreAllBlocksUploaded()
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to check if all blocks are uploaded", "err", err)
		return false
	}

	return allBlocks
}

func (m *MultiTSDB) initTSDBIfNeeded(tenantID string, t *tenant) error {
	_, err, _ := m.initSingleFlight.Do(tenantID, func() (any, error) {
		if t.readyS.Get() != nil {
			return nil, nil
		}
		logger := log.With(m.logger, "tenant", tenantID)

		err := m.startTSDB(logger, tenantID, t)
		if err != nil {
			return nil, err
		}

		t.startPeriodicHeadCompaction()
		if t.shipper() != nil {
			t.startPeriodicUploader()
		}

		return nil, nil
	})

	return err
}

const compactionDelayPercentBlockLength = 10

// generateCompactionDelay() generates a time.Duration of up to compactionDelayPercentBlockLength% of the block range. Used to stagger compactions & uploads.
func (t *tenant) generateCompactionDelay() time.Duration {
	return time.Duration(rand.Int63n((t.maxBlockDuration*compactionDelayPercentBlockLength)/100)) * time.Millisecond
}

func (t *tenant) startPeriodicHeadCompaction() {
	// NOTE(GiedriusS): from the old cmd/thanos/receive.go.
	var interval = 2 * time.Duration(t.maxBlockDuration) * time.Millisecond

	doIter := func() error {
		db := t.readyS.Get()
		if db == nil {
			return fmt.Errorf("no DB found")
		}
		head := db.Head()
		if head.MinTime() < 0 {
			return nil
		}
		sinceOldestDataMillis := time.Since(time.UnixMilli(head.MinTime())).Milliseconds()

		// NOTE(GiedriusS): this is what Prometheus does. 0.5 is an extra appending window.
		compactionThreshold := int64(1.5 * float64(t.maxBlockDuration))
		if sinceOldestDataMillis > compactionThreshold {
			if err := t.compactHead(db); err != nil {
				return fmt.Errorf("compact head: %w", err)
			}
			t.lastSuccessfulHeadCompaction.Store(time.Now().UnixNano())
		}

		return nil
	}

	compactionDelay := t.generateCompactionDelay()
	go func() {
		level.Info(t.logger).Log("msg", "starting periodic head compaction", "initial_delay", compactionDelay.String(), "interval", interval.String())
		select {
		case <-time.After(compactionDelay):
		case <-t.doneC:
			return
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				level.Info(t.logger).Log("msg", "running periodic head compaction")
				if err := doIter(); err != nil {
					level.Error(t.logger).Log("msg", "periodic head compaction failed", "err", err)
				}

			case <-t.doneC:
				return
			}
		}
	}()
}

func (t *tenant) startPeriodicUploader() {
	s := t.shipper()
	if s == nil {
		panic("BUG: periodic uploader started but shipper is nil")
	}

	var interval = 30 * time.Second

	doIter := func() error {
		syncCtx, cancel := context.WithTimeout(context.Background(), interval)
		defer cancel()
		if _, err := s.Sync(syncCtx); err != nil {
			return fmt.Errorf("sync: %w", err)
		}

		return nil
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		if err := doIter(); err != nil {
			level.Error(t.logger).Log("msg", "initial upload failed", "err", err)
		}

		for {
			select {
			case <-ticker.C:
				if err := doIter(); err != nil {
					level.Error(t.logger).Log("msg", "periodic upload failed", "err", err)
				}
			case <-t.doneC:
				return
			}
		}
	}()
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

func newTenant(l log.Logger, retentionDuration, maxBlockDuration int64, name string) *tenant {
	return &tenant{
		readyS:            &ReadyStorage{},
		mtx:               &sync.RWMutex{},
		retentionDuration: retentionDuration,
		logger:            log.With(l, "tenant", name),
		doneC:             make(chan struct{}),
		tenantName:        name,
		maxBlockDuration:  maxBlockDuration,
	}
}

type closeDelete int

const DELETE_DATA closeDelete = 0
const KEEP_DATA closeDelete = 1

func (t *tenant) close(cd closeDelete) {
	t.closeOnce.Do(func() {
		close(t.doneC)

		// NOTE(GiedriusS): on paper, we could Close() again but the TSDB's Close() function is not idempotent.
		// If storage starts erroring out then we are hosed anyway, so just log an error.
		if err := t.tsdb.Close(); err != nil {
			level.Error(t.logger).Log("msg", "failed closing tenant's TSDB", "tenant", t.tenantName, "err", err)
		}

		if cd == DELETE_DATA {
			if err := os.RemoveAll(t.tsdb.Dir()); err != nil {
				level.Error(t.logger).Log("msg", "failed removing tenant's TSDB directory", "tenant", t.tenantName, "err", err)
			}
		}

		t.readyS.set(nil)
		t.setComponents(nil, nil, nil, nil, nil)
	})
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

	return newLocalClient(tsdbStore, t.readOnly)
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

func (t *tenant) set(storeTSDB *store.TSDBStore, tenantTSDB *tsdb.DB, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB, reg *UnRegisterer) {
	t.readyS.Set(tenantTSDB)
	t.mtx.Lock()
	t.setComponents(storeTSDB, ship, exemplarsTSDB, tenantTSDB, reg)
	t.mtx.Unlock()
}

func (t *tenant) setComponents(storeTSDB *store.TSDBStore, ship *shipper.Shipper, exemplarsTSDB *exemplars.TSDB, tenantTSDB *tsdb.DB, reg *UnRegisterer) {
	if storeTSDB == nil && t.storeTSDB != nil {
		t.storeTSDB.Close()
	}
	if reg == nil && t.reg != nil {
		t.reg.UnregisterAll()
	}
	t.storeTSDB = storeTSDB
	t.reg = reg
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
		if !f.IsDir() {
			continue
		}

		g.Go(func() error {
			_, err := t.getOrLoadTenant(f.Name())
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
		wg.Go(func() {
			if err := tenant.flushHead(db); err != nil {
				errmtx.Lock()
				merr.Add(err)
				errmtx.Unlock()
			}
		})
	}

	wg.Wait()
	return merr.Err()
}

func (t *tenant) flushHead(db *tsdb.DB) error {
	head := db.Head()
	if head.MinTime() == head.MaxTime() {
		return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()))
	}
	blockAlignedMaxt := head.MaxTime() - (head.MaxTime() % t.maxBlockDuration)
	// Flush a well aligned TSDB block.
	if err := db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), blockAlignedMaxt-1)); err != nil {
		return err
	}
	// Flush the remainder of the head.
	return db.CompactHead(tsdb.NewRangeHead(head, head.MinTime(), head.MaxTime()-1))
}

func (t *tenant) compactHead(db *tsdb.DB) error {
	head := db.Head()
	mint := head.MinTime()
	blockAlignedMint := mint - (mint % t.maxBlockDuration)
	maxt := blockAlignedMint + t.maxBlockDuration - 1
	return db.CompactHead(tsdb.NewRangeHead(head, mint, maxt))
}

func (t *MultiTSDB) Close() {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	for _, tenant := range t.tenants {
		tenant.close(KEEP_DATA)
	}
}

func (t *MultiTSDB) maybeDeleteTenant(tenant *tenant) {
	// It is not read-only anymore, so skip deletion.
	if !tenant.readOnly.Load() {
		level.Info(t.logger).Log("msg", "tenant is no longer read-only, skipping deletion", "tenant", tenant.tenantName)
		return
	}

	// It could have uploaded something in the meantime, so check again.
	if !tenant.shouldBeMarkedInactive() {
		level.Info(t.logger).Log("msg", "tenant should no longer be marked as inactive, skipping deletion", "tenant", tenant.tenantName)
		return
	}

	level.Info(t.logger).Log("msg", "tenant is being deleted due to inactivity", "tenant", tenant.tenantName)

	tenant.close(DELETE_DATA)
	t.removeTenantUnlocked(tenant.tenantName)

}

const tenantGCDelay = 1 * time.Minute

// Prune flushes and closes the TSDB for tenants that haven't received
// any new samples for longer than the TSDB retention period.
func (t *MultiTSDB) Prune(ctx context.Context) error {
	if t.tsdbOpts.RetentionDuration == 0 {
		return nil
	}
	level.Info(t.logger).Log("msg", "Running pruning job")

	var (
		markedInactive int
	)

	t.mtx.RLock()
	defer t.mtx.RUnlock()

	begin := time.Now()
	for _, tenant := range t.tenants {
		if shouldBeMarked := tenant.shouldBeMarkedInactive(); !shouldBeMarked {
			continue
		}
		markedInactive++

		oldReadOnly := tenant.readOnly.Swap(true)
		if !oldReadOnly {
			level.Info(t.logger).Log("msg", "marking tenant as read-only due to inactivity", "tenant", tenant.tenantName)

			if t.gcImmediately {
				t.maybeDeleteTenant(tenant)
			} else {
				time.AfterFunc(tenantGCDelay, func() {
					t.mtx.Lock()
					defer t.mtx.Unlock()
					t.maybeDeleteTenant(tenant)
				})
			}
		}
	}

	level.Info(t.logger).Log("msg", "Pruning job completed", "marked_inactive_tenants_count", markedInactive, "took_seconds", time.Since(begin).Seconds())

	return nil
}

func (t *MultiTSDB) SyncAllTenants(ctx context.Context) (int, error) {
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
		s := tenant.shipper()
		if s == nil {
			continue
		}
		wg.Go(func() {
			up, err := s.Sync(ctx)
			if err != nil {
				errmtx.Lock()
				merr.Add(errors.Wrap(err, "upload"))
				errmtx.Unlock()
			}
			uploaded.Add(int64(up))
			level.Debug(t.logger).Log("msg", "uploaded blocks for tenant", "tenant", tenantID, "blocks_count", up)

		})
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

// TSDBLocalClients should be used as read-only.
func (t *MultiTSDB) TSDBLocalClients() []store.Client {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.tsdbClients
}

// TSDBExemplars should be used as read-only.
func (t *MultiTSDB) TSDBExemplars() map[string]*exemplars.TSDB {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.exemplarClients
}

func (t *MultiTSDB) TenantStats(limit int, statsByLabelName string, tenantIDs ...string) []api.TenantStats {
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
		result = make([]api.TenantStats, 0, len(t.tenants))
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
			result = append(result, api.TenantStats{
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

	var expandedPostingsCache expandedpostingscache.ExpandedPostingsCache
	if t.headExpandedPostingsCacheSize > 0 || t.blockExpandedPostingsCacheSize > 0 {
		var expandedPostingsCacheMetrics = expandedpostingscache.NewPostingCacheMetrics(extprom.WrapRegistererWithPrefix("thanos_", reg))

		expandedPostingsCache = expandedpostingscache.NewBlocksPostingsForMatchersCache(expandedPostingsCacheMetrics, t.headExpandedPostingsCacheSize, t.blockExpandedPostingsCacheSize, 0)
	}

	opts := *t.tsdbOpts
	opts.BlocksToDelete = tenant.blocksToDelete
	opts.EnableDelayedCompaction = true
	opts.CompactionDelayMaxPercent = tsdb.DefaultCompactionDelayMaxPercent
	opts.IsolationDisabled = true

	opts.BlockChunkQuerierFunc = func(b tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
		if expandedPostingsCache != nil {
			return expandedpostingscache.NewCachedBlockChunkQuerier(expandedPostingsCache, b, mint, maxt)
		}
		return tsdb.NewBlockChunkQuerier(b, mint, maxt)
	}
	if expandedPostingsCache != nil {
		opts.SeriesLifecycleCallback = expandedPostingsCache
	}
	tenant.blocksToDeleteFn = tsdb.DefaultBlocksToDelete

	// NOTE(GiedriusS): always set to false to properly handle OOO samples - OOO samples are written into the WBL
	// which gets later converted into a block. Without setting this flag to false, the block would get compacted
	// into other ones. This presents a race between compaction and the shipper (if it is configured to upload compacted blocks).
	// Hence, avoid this situation by disabling overlapping compaction. Vertical compaction must be enabled on the compactor.
	opts.EnableOverlappingCompaction = false

	// Exclude blocks from compaction that have not yet been uploaded by the shipper.
	// This allows running with compaction enabled (tsdb.min-block-duration != tsdb.max-block-duration)
	// without risking data loss due to blocks being compacted before upload.
	opts.BlockCompactionExcludeFunc = func(meta *tsdb.BlockMeta) bool {
		// Blocks with level > 1 are not uploaded by shipper. We dont want to exclude them from compaction.
		if meta.Compaction.Level > 1 {
			return false
		}

		s := tenant.shipper()
		if s == nil {
			return false
		}
		uploaded := s.UploadedBlocks()
		_, ok := uploaded[meta.ULID]
		return !ok
	}

	// We don't do scrapes ourselves so this only gives us a performance penalty.
	opts.IsolationDisabled = true

	s, err := tsdb.Open(
		dataDir,
		logutil.GoKitLogToSlog(logger),
		reg,
		&opts,
		nil,
	)
	if err != nil {
		t.removeTenantLocked(tenantID)
		return err
	}
	// NOTE(GiedriusS): disable compactions because we need to always create a block periodically even though it could remain in the WAL
	// forever. Reason being is that object storage serves as a backup. If ONLY auto compactions were enabled, it could be that
	// some tenant might never produce a block and that data will remain on disk forever. So, for clarity let's just disable auto
	// compactions and do it ourselves so that the logic wouldn't clash together.
	s.DisableCompactions()

	var ship *shipper.Shipper
	if t.bucket != nil {
		ship = shipper.New(
			t.bucket,
			dataDir,
			shipper.WithLogger(logger),
			shipper.WithRegisterer(reg),
			shipper.WithSource(metadata.ReceiveSource),
			shipper.WithHashFunc(t.hashFunc),
			shipper.WithMetaFileName(shipper.DefaultMetaFilename),
			shipper.WithLabels(func() labels.Labels { return lset }),
			shipper.WithAllowOutOfOrderUploads(t.allowOutOfOrderUpload),
			shipper.WithSkipCorruptedBlocks(t.skipCorruptedBlocks),
			shipper.WithUploadConcurrency(t.uploadConcurrency),
		)
	}
	var options []store.TSDBStoreOption
	if t.metricNameFilterEnabled {
		options = append(options, store.WithCuckooMetricNameStoreFilter())
	}
	if t.matcherCache != nil {
		options = append(options, store.WithMatcherCacheInstance(t.matcherCache))
	}
	tenant.set(store.NewTSDBStore(logger, s, component.Receive, lset, options...), s, ship, exemplars.NewTSDB(s, lset), reg.(*UnRegisterer))
	t.addTenantLocked(tenantID, tenant) // need to update the client list once store is ready & client != nil
	level.Info(logger).Log("msg", "TSDB is now ready")
	return nil
}

func (t *MultiTSDB) defaultTenantDataDir(tenantID string) string {
	return path.Join(t.dataDir, tenantID)
}

func (t *MultiTSDB) getOrLoadTenant(tenantID string) (*tenant, error) {
	t.mtx.Lock()
	tenant, exist := t.tenants[tenantID]
	if exist {
		tenant.readOnly.CompareAndSwap(true, false)
		t.mtx.Unlock()
		return tenant, t.initTSDBIfNeeded(tenantID, tenant)
	}

	tenant = newTenant(t.logger, t.tsdbOpts.RetentionDuration, t.tsdbOpts.MaxBlockDuration, tenantID)
	t.addTenantUnlocked(tenantID, tenant)
	t.mtx.Unlock()

	return tenant, t.initTSDBIfNeeded(tenantID, tenant)
}

func (t *MultiTSDB) TenantAppendable(tenantID string) (Appendable, error) {
	tenant, err := t.getOrLoadTenant(tenantID)
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

	collectors []prometheus.Collector
}

func NewUnRegisterer(inner prometheus.Registerer) *UnRegisterer {
	return &UnRegisterer{innerReg: inner}
}

// UnregisterAll unregisters all collectors in a best-effort manner.
func (u *UnRegisterer) UnregisterAll() {
	for _, c := range u.collectors {
		u.innerReg.Unregister(c)
	}
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

			u.collectors = append(u.collectors, c)
			return nil
		}
		return err
	}

	u.collectors = append(u.collectors, c)
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
	u.collectors = append(u.collectors, cs...)
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
