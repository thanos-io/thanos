// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/tsdb"
	terrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/objstore"
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
	upload          bool

	mtx         *sync.RWMutex
	dbs         map[string]*FlushableStorage
	appendables map[string]*tsdb.ReadyStorage
	stores      map[string]*store.TSDBStore
	shippers    map[string]*shipper.Shipper
}

func NewMultiTSDB(dataDir string, l log.Logger, reg prometheus.Registerer, tsdbCfg *tsdb.Options, labels labels.Labels, tenantLabelName string, bucket objstore.Bucket) *MultiTSDB {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &MultiTSDB{
		dataDir:         dataDir,
		logger:          l,
		reg:             reg,
		tsdbCfg:         tsdbCfg,
		mtx:             &sync.RWMutex{},
		dbs:             map[string]*FlushableStorage{},
		stores:          map[string]*store.TSDBStore{},
		appendables:     map[string]*tsdb.ReadyStorage{},
		shippers:        map[string]*shipper.Shipper{},
		labels:          labels,
		tenantLabelName: tenantLabelName,
		bucket:          bucket,
		upload:          bucket != nil,
	}
}

func (t *MultiTSDB) Open() error {
	if err := os.MkdirAll(t.dataDir, 0777); err != nil {
		return err
	}

	return t.openTSDBs()
}

func (t *MultiTSDB) Close() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	errmtx := &sync.Mutex{}
	merr := terrors.MultiError{}
	wg := &sync.WaitGroup{}
	for _, tsdb := range t.dbs {
		tsdb := tsdb
		wg.Add(1)
		go func() {
			if err := tsdb.Close(); err != nil {
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

func (t *MultiTSDB) Flush() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	errmtx := &sync.Mutex{}
	merr := terrors.MultiError{}
	wg := &sync.WaitGroup{}
	for _, tsdb := range t.dbs {
		tsdb := tsdb
		wg.Add(1)
		go func() {
			if err := tsdb.Flush(); err != nil {
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

func (t *MultiTSDB) Upload(ctx context.Context) error {
	if !t.upload {
		return nil
	}

	t.mtx.Lock()
	defer t.mtx.Unlock()

	errmtx := &sync.Mutex{}
	merr := terrors.MultiError{}
	wg := &sync.WaitGroup{}
	for tenant, s := range t.shippers {
		level.Debug(t.logger).Log("msg", "uploading block for tenant", "tenant", tenant)
		s := s
		wg.Add(1)
		go func() {
			if uploaded, err := s.Sync(ctx); err != nil {
				errmtx.Lock()
				merr.Add(fmt.Errorf("failed to upload %d: %w", uploaded, err))
				errmtx.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return merr.Err()
}

func (t *MultiTSDB) openTSDBs() error {
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
			tenantId := f.Name()
			_, err := t.getOrLoadTenant(tenantId)
			return err
		})
	}

	return g.Wait()
}

func (t *MultiTSDB) TSDBStores() map[string]*store.TSDBStore {
	t.mtx.RLock()
	res := make(map[string]*store.TSDBStore, len(t.stores))
	for k, v := range t.stores {
		res[k] = v
	}
	defer t.mtx.RUnlock()
	return res
}

func (t *MultiTSDB) getOrLoadTenant(tenantID string) (*tsdb.ReadyStorage, error) {
	// Fast path, as creating tenants is a very rare operation.
	t.mtx.RLock()
	db, exist := t.appendables[tenantID]
	t.mtx.RUnlock()
	if exist {
		return db, nil
	}

	// Slow path needs to lock fully and attempt to read again to prevent race
	// conditions, where since the fast path was tried, there may have actually
	// been the same tenant inserted in the map.
	t.mtx.Lock()
	db, exist = t.appendables[tenantID]
	if exist {
		t.mtx.Unlock()
		return db, nil
	}

	rs := &tsdb.ReadyStorage{}
	t.appendables[tenantID] = rs
	t.mtx.Unlock()

	go func() {
		reg := prometheus.WrapRegistererWith(prometheus.Labels{
			"tenant": tenantID,
		}, t.reg)
		logger := log.With(t.logger, "tenant", tenantID)
		lbls := append(t.labels, labels.Label{Name: t.tenantLabelName, Value: tenantID})
		dataDir := path.Join(t.dataDir, tenantID)

		var ship *shipper.Shipper
		if t.upload {
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

		if err := s.Open(); err != nil {
			level.Error(logger).Log("msg", "failed to open tsdb", "err", err)
			t.mtx.Lock()
			delete(t.appendables, tenantID)
			delete(t.stores, tenantID)
			t.mtx.Unlock()
			if err := s.Close(); err != nil {
				level.Error(logger).Log("msg", "failed to close tsdb", "err", err)
			}
			return
		}

		tstore := store.NewTSDBStore(
			logger,
			reg,
			s.Get(),
			component.Receive,
			lbls,
		)

		t.mtx.Lock()
		rs.Set(s.Get(), int64(2*time.Duration(t.tsdbCfg.MinBlockDuration).Seconds()*1000))
		t.stores[tenantID] = tstore
		t.dbs[tenantID] = s
		t.shippers[tenantID] = ship
		t.mtx.Unlock()
	}()

	return rs, nil
}

func (t *MultiTSDB) TenantAppendable(tenantID string) (Appendable, error) {
	return t.getOrLoadTenant(tenantID)
}
