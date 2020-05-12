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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/tsdb"
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
	tsdbCfg         *tsdb.Options
	tenantLabelName string
	labels          labels.Labels
	bucket          objstore.Bucket

	mtx     *sync.RWMutex
	tenants map[string]*tenant
}

type tenant struct {
	tsdbCfg *tsdb.Options

	readyS *tsdb.ReadyStorage
	fs     *FlushableStorage
	s      *store.TSDBStore
	ship   *shipper.Shipper

	mtx *sync.RWMutex
}

func newTenant(tsdbCfg *tsdb.Options) *tenant {
	return &tenant{
		tsdbCfg: tsdbCfg,
		readyS:  &tsdb.ReadyStorage{},
		mtx:     &sync.RWMutex{},
	}
}

func (t *tenant) readyStorage() *tsdb.ReadyStorage {
	return t.readyS
}

func (t *tenant) store() *store.TSDBStore {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.s
}

func (t *tenant) shipper() *shipper.Shipper {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.ship
}

func (t *tenant) flushableStorage() *FlushableStorage {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return t.fs
}

func (t *tenant) set(tstore *store.TSDBStore, fs *FlushableStorage, ship *shipper.Shipper) {
	t.readyS.Set(fs.Get(), int64(2*time.Duration(t.tsdbCfg.MinBlockDuration).Seconds()*1000))
	t.mtx.Lock()
	t.fs = fs
	t.s = tstore
	t.ship = ship
	t.mtx.Unlock()
}

func NewMultiTSDB(
	dataDir string,
	l log.Logger,
	reg prometheus.Registerer,
	tsdbCfg *tsdb.Options,
	labels labels.Labels,
	tenantLabelName string,
	bucket objstore.Bucket,
) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &MultiTSDB{
		dataDir:         dataDir,
		logger:          l,
		reg:             reg,
		tsdbCfg:         tsdbCfg,
		mtx:             &sync.RWMutex{},
		tenants:         map[string]*tenant{},
		labels:          labels,
		tenantLabelName: tenantLabelName,
		bucket:          bucket,
	}
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
		s := tenant.flushableStorage()
		if s == nil {
			continue
		}

		wg.Add(1)
		go func() {
			if err := s.Flush(); err != nil {
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

	tenant = newTenant(t.tsdbCfg)
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
			)
		}

		s := NewFlushableStorage(
			dataDir,
			logger,
			reg,
			t.tsdbCfg,
		)

		// Assign to outer error to report in blocking case.
		if err = s.Open(); err != nil {
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
				s.Get(),
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
