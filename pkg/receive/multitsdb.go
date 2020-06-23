// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/thanos-io/thanos/pkg/store"
	"golang.org/x/sync/errgroup"
)

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
}

func NewMultiTSDB(
	dataDir string,
	l log.Logger,
	reg prometheus.Registerer,
	tsdbOpts *tsdb.Options,
	labels labels.Labels,
	tenantLabelName string,
	bucket objstore.Bucket,
	allowOutOfOrderUpload bool,
) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &MultiTSDB{
		dataDir:               dataDir,
		logger:                l,
		reg:                   reg,
		tsdbOpts:              tsdbOpts,
		mtx:                   &sync.RWMutex{},
		tenants:               map[string]*tenant{},
		labels:                labels,
		tenantLabelName:       tenantLabelName,
		bucket:                bucket,
		allowOutOfOrderUpload: allowOutOfOrderUpload,
	}
}

type tenant struct {
	tsdbOpts *tsdb.Options

	readyS    *ReadyStorage
	tsdb      *tsdb.DB
	storeTSDB *store.TSDBStore
	ship      *shipper.Shipper

	mtx *sync.RWMutex
}

func newTenant(tsdbOpts *tsdb.Options) *tenant {
	return &tenant{
		tsdbOpts: tsdbOpts,
		readyS:   &ReadyStorage{},
		mtx:      &sync.RWMutex{},
	}
}

func (t *tenant) readyStorage() *ReadyStorage {
	return t.readyS
}

func (t *tenant) store() *store.TSDBStore {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.storeTSDB
}

func (t *tenant) shipper() *shipper.Shipper {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.ship
}

func (t *tenant) db() *tsdb.DB {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.tsdb
}

func (t *tenant) set(storeTSDB *store.TSDBStore, tenantTSDB *tsdb.DB, ship *shipper.Shipper) {
	t.readyS.Set(tenantTSDB, int64(2*time.Duration(t.tsdbOpts.MinBlockDuration).Seconds()*1000))
	t.mtx.Lock()
	t.tsdb = tenantTSDB
	t.storeTSDB = storeTSDB
	t.ship = ship
	t.mtx.Unlock()
}

func (t *MultiTSDB) Open() error {
	if err := os.MkdirAll(t.dataDir, 0777); err != nil {
		return err
	}

	files, err := ioutil.ReadDir(t.dataDir)
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
	merr := terrors.MultiError{}
	wg := &sync.WaitGroup{}
	for _, tenant := range t.tenants {
		db := tenant.db()
		if db == nil {
			continue
		}

		wg.Add(1)
		go func() {
			head := db.Head()
			mint, maxt := head.MinTime(), head.MaxTime()
			if err := db.CompactHead(tsdb.NewRangeHead(head, mint, maxt-1)); err != nil {
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

func (t *MultiTSDB) Sync(ctx context.Context) error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	errmtx := &sync.Mutex{}
	merr := terrors.MultiError{}
	wg := &sync.WaitGroup{}
	for tenantID, tenant := range t.tenants {
		level.Debug(t.logger).Log("msg", "uploading block for tenant", "tenant", tenantID)
		s := tenant.shipper()
		if s == nil {
			continue
		}

		wg.Add(1)
		go func() {
			if uploaded, err := s.Sync(ctx); err != nil {
				errmtx.Lock()
				merr.Add(errors.Wrapf(err, "upload %d", uploaded))
				errmtx.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return merr.Err()
}

func (t *MultiTSDB) TSDBStores() map[string]*store.TSDBStore {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	res := make(map[string]*store.TSDBStore, len(t.tenants))
	for k, tenant := range t.tenants {
		s := tenant.store()
		if s != nil {
			res[k] = s
		}
	}
	return res
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

	tenant = newTenant(t.tsdbOpts)
	t.tenants[tenantID] = tenant
	t.mtx.Unlock()

	var err error
	startTSDB := func() {
		reg := prometheus.WrapRegistererWith(prometheus.Labels{
			"tenant": tenantID,
		}, t.reg)
		logger := log.With(t.logger, "tenant", tenantID)
		lbls := append(t.labels, labels.Label{Name: t.tenantLabelName, Value: tenantID})
		dataDir := path.Join(t.dataDir, tenantID)

		var ship *shipper.Shipper
		if t.bucket != nil {
			ship = shipper.New(
				logger,
				reg,
				dataDir,
				t.bucket,
				func() labels.Labels { return lbls },
				metadata.ReceiveSource,
				t.allowOutOfOrderUpload,
			)
		}

		s, err := tsdb.Open(
			dataDir,
			logger,
			&UnRegisterer{Registerer: reg},
			t.tsdbOpts,
		)

		// Assign to outer error to report in blocking case.
		if err != nil {
			level.Error(logger).Log("msg", "failed to open tsdb", "err", err)
			t.mtx.Lock()
			delete(t.tenants, tenantID)
			t.mtx.Unlock()
			runutil.CloseWithLogOnErr(logger, s, "failed to close tsdb")
			return
		}

		tenant.set(
			store.NewTSDBStore(
				logger,
				reg,
				s,
				component.Receive,
				lbls,
			),
			s,
			ship,
		)
	}
	if !blockingStart {
		go startTSDB()
		return tenant, nil
	}

	startTSDB()
	return tenant, err
}

func (t *MultiTSDB) TenantAppendable(tenantID string) (Appendable, error) {
	tenant, err := t.getOrLoadTenant(tenantID, false)
	if err != nil {
		return nil, err
	}
	return tenant.readyStorage(), nil
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
func (s *ReadyStorage) Set(db *tsdb.DB, startTimeMargin int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.a = &adapter{db: db, startTimeMargin: startTimeMargin}
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
	if x := s.get(); x != nil {
		return x.StartTime()
	}
	return int64(model.Latest), ErrNotReady
}

// Querier implements the Storage interface.
func (s *ReadyStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(ctx, mint, maxt)
	}
	return nil, ErrNotReady
}

// Appender implements the Storage interface.
func (s *ReadyStorage) Appender() (storage.Appender, error) {
	if x := s.get(); x != nil {
		return x.Appender()
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
	db              *tsdb.DB
	startTimeMargin int64
}

// StartTime implements the Storage interface.
func (a adapter) StartTime() (int64, error) {
	var startTime int64

	if len(a.db.Blocks()) > 0 {
		startTime = a.db.Blocks()[0].Meta().MinTime
	} else {
		startTime = time.Now().Unix() * 1000
	}

	// Add a safety margin as it may take a few minutes for everything to spin up.
	return startTime + a.startTimeMargin, nil
}

func (a adapter) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := a.db.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	return q, nil
}

// Appender returns a new appender against the storage.
func (a adapter) Appender() (storage.Appender, error) {
	return a.db.Appender(), nil
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
type UnRegisterer struct {
	prometheus.Registerer
}

func (u *UnRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := u.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				if ok = u.Unregister(c); !ok {
					panic("unable to unregister existing collector")
				}
				u.Registerer.MustRegister(c)
				continue
			}
			panic(err)
		}
	}
}
