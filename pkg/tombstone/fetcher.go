// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package tombstone

import (
	"context"
	"encoding/json"
	"github.com/golang/groupcache/singleflight"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	tombstoneSubSys = "tombstone"
)

var (
	ErrorSyncTombstoneNotFound  = errors.New("tombstone not found")
	ErrorSyncTombstoneCorrupted = errors.New("tombstone corrupted")
)

type TombstoneFetcher interface {
	Fetch(ctx context.Context) (tombstones map[ulid.ULID]*Tombstone, partial map[ulid.ULID]error, err error)
}

// TombstoneFilter allows filtering or modifying metas from the provided map or returns error.
type TombstoneFilter interface {
	Filter(ctx context.Context, tombstones map[ulid.ULID]*Tombstone, synced *extprom.TxGaugeVec) error
}

func newFetcherMetrics(reg prometheus.Registerer, syncedExtraLabels, modifiedExtraLabels [][]string) *block.FetcherMetrics {
	var m block.FetcherMetrics

	m.Syncs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: tombstoneSubSys,
		Name:      "syncs_total",
		Help:      "Total tombstone synchronization attempts",
	})
	m.SyncFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: tombstoneSubSys,
		Name:      "sync_failures_total",
		Help:      "Total tombstone synchronization failures",
	})
	m.SyncDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Subsystem: tombstoneSubSys,
		Name:      "sync_duration_seconds",
		Help:      "Duration of the tombstone synchronization in seconds",
		Buckets:   []float64{0.01, 1, 10, 100, 300, 600, 1000},
	})
	//m.Synced = extprom.NewTxGaugeVec(
	//	reg,
	//	prometheus.GaugeOpts{
	//		Subsystem: tombstoneSubSys,
	//		Name:      "synced",
	//		Help:      "Number of tombstone synced",
	//	},
	//	[]string{"state"},
	//	append([][]string{
	//		{CorruptedMeta},
	//		{NoMeta},
	//		{LoadedMeta},
	//		{tooFreshMeta},
	//		{FailedMeta},
	//		{labelExcludedMeta},
	//		{timeExcludedMeta},
	//		{duplicateMeta},
	//		{MarkedForDeletionMeta},
	//		{MarkedForNoCompactionMeta},
	//	}, syncedExtraLabels...)...,
	//)
	return &m
}

type Fetcher struct {
	bkt    objstore.InstrumentedBucketReader
	logger log.Logger

	metrics *block.FetcherMetrics
	wrapped *BaseFetcher
	filters []TombstoneFilter
}

// NewTombstoneFetcher returns Fetcher.
func NewTombstoneFetcher(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, dir string, reg prometheus.Registerer, filters []TombstoneFilter) (*Fetcher, error) {
	b, err := NewBaseFetcher(logger, concurrency, bkt, dir, reg)
	if err != nil {
		return nil, err
	}
	return &Fetcher{metrics: newFetcherMetrics(reg, nil, nil), wrapped: b, filters: filters, logger: b.logger}, nil
}

//func (f *Fetcher) Fetch(ctx context.Context) (tombstones map[ulid.ULID]*Tombstone, partial map[ulid.ULID]error, err error) {
//	var (
//		ts map[ulid.ULID]*Tombstone
//	)
//
//	if err := f.bkt.Iter(ctx, TombstoneDir, func(name string) error {
//		tombstoneFilename := path.Join("", name)
//		tombstoneFile, err := f.bkt.Get(ctx, tombstoneFilename)
//		if err != nil {
//			return nil
//		}
//		defer runutil.CloseWithLogOnErr(f.logger, tombstoneFile, "close bkt tombstone reader")
//
//		var t Tombstone
//		tombstone, err := ioutil.ReadAll(tombstoneFile)
//		if err != nil {
//			return nil
//		}
//		if err := json.Unmarshal(tombstone, &t); err != nil {
//			level.Error(f.logger).Log("msg", "failed to unmarshal tombstone", "file", tombstoneFilename, "err", err)
//			return nil
//		}
//		ts = append(ts, &t)
//		return nil
//	}); err != nil {
//		return nil, err
//	}
//	return ts, nil
//}

// Fetch returns all block metas as well as partial blocks (blocks without or with corrupted meta file) from the bucket.
// It's caller responsibility to not change the returned metadata files. Maps can be modified.
//
// Returned error indicates a failure in fetching metadata. Returned meta can be assumed as correct, with some blocks missing.
func (f *Fetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*Tombstone, partial map[ulid.ULID]error, err error) {
	return f.wrapped.fetch(ctx, f.metrics, f.filters)
}

// BaseFetcher is a struct that synchronizes filtered metadata of all block in the object storage with the local state.
// Go-routine safe.
type BaseFetcher struct {
	logger      log.Logger
	concurrency int
	bkt         objstore.InstrumentedBucketReader

	// Optional local directory to cache meta.json files.
	cacheDir string
	syncs    prometheus.Counter
	g        singleflight.Group

	mtx    sync.Mutex
	cached map[ulid.ULID]*Tombstone
}

// NewBaseFetcher constructs BaseFetcher.
func NewBaseFetcher(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, dir string, reg prometheus.Registerer) (*BaseFetcher, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	cacheDir := ""
	if dir != "" {
		cacheDir = filepath.Join(dir, "meta-syncer")
		if err := os.MkdirAll(cacheDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	return &BaseFetcher{
		logger:      log.With(logger, "component", "tombstone.BaseFetcher"),
		concurrency: concurrency,
		bkt:         bkt,
		cacheDir:    cacheDir,
		cached:      map[ulid.ULID]*Tombstone{},
		//syncs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
		//	Subsystem: fetcherSubSys,
		//	Name:      "base_syncs_total",
		//	Help:      "Total tombstone synchronization attempts by base Fetcher",
		//}),
	}, nil
}

type response struct {
	tombstones map[ulid.ULID]*Tombstone
	partial    map[ulid.ULID]error
	// If tombstoneErrs > 0 it means incomplete view, so some metas, failed to be loaded.
	tombstoneErrs errutil.MultiError

	corruptedTombstones float64
}

// loadMeta returns metadata from object storage or error.
// It returns `ErrorSyncMetaNotFound` and `ErrorSyncMetaCorrupted` sentinel errors in those cases.
func (f *BaseFetcher) loadTombstone(ctx context.Context, id ulid.ULID) (*Tombstone, error) {
	tombstoneFile := path.Join(TombstoneDir, id.String()+".json")
	ok, err := f.bkt.Exists(ctx, tombstoneFile)
	if err != nil {
		return nil, errors.Wrapf(err, "%s file doesn't exist", tombstoneFile)
	}
	if !ok {
		return nil, ErrorSyncTombstoneNotFound
	}

	if m, seen := f.cached[id]; seen {
		return m, nil
	}

	//// Best effort load from local dir.
	//if f.cacheDir != "" {
	//	m, err := metadata.ReadFromDir(cachedBlockDir)
	//	if err == nil {
	//		return m, nil
	//	}
	//
	//	if !errors.Is(err, os.ErrNotExist) {
	//		level.Warn(f.logger).Log("msg", "best effort read of the local meta.json failed; removing cached block dir", "dir", cachedBlockDir, "err", err)
	//		if err := os.RemoveAll(cachedBlockDir); err != nil {
	//			level.Warn(f.logger).Log("msg", "best effort remove of cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
	//		}
	//	}
	//}

	r, err := f.bkt.ReaderWithExpectedErrs(f.bkt.IsObjNotFoundErr).Get(ctx, tombstoneFile)
	if f.bkt.IsObjNotFoundErr(err) {
		// Meta.json was deleted between bkt.Exists and here.
		return nil, errors.Wrapf(ErrorSyncTombstoneNotFound, "%v", err)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "get tombstone file: %v", tombstoneFile)
	}

	defer runutil.CloseWithLogOnErr(f.logger, r, "close bkt meta get")

	content, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta file: %v", tombstoneFile)
	}

	var t Tombstone
	if err := json.Unmarshal(content, &t); err != nil {
		return nil, errors.Wrapf(ErrorSyncTombstoneCorrupted, "tombstone %v unmarshal: %v", tombstoneFile, err)
	}

	//// Best effort cache in local dir.
	//if f.cacheDir != "" {
	//	if err := os.MkdirAll(cachedBlockDir, os.ModePerm); err != nil {
	//		level.Warn(f.logger).Log("msg", "best effort mkdir of the meta.json block dir failed; ignoring", "dir", cachedBlockDir, "err", err)
	//	}
	//
	//	if err := m.WriteToDir(f.logger, cachedBlockDir); err != nil {
	//		level.Warn(f.logger).Log("msg", "best effort save of the meta.json to local dir failed; ignoring", "dir", cachedBlockDir, "err", err)
	//	}
	//}
	return &t, nil
}

func (f *BaseFetcher) fetchTombstone(ctx context.Context) (interface{}, error) {
	f.syncs.Inc()

	var (
		resp = response{
			tombstones: make(map[ulid.ULID]*Tombstone),
			partial:    make(map[ulid.ULID]error),
		}
		eg  errgroup.Group
		ch  = make(chan ulid.ULID, f.concurrency)
		mtx sync.Mutex
	)
	level.Debug(f.logger).Log("msg", "fetching tombstone", "concurrency", f.concurrency)
	for i := 0; i < f.concurrency; i++ {
		eg.Go(func() error {
			for id := range ch {
				t, err := f.loadTombstone(ctx, id)
				if err == nil {
					mtx.Lock()
					resp.tombstones[id] = t
					mtx.Unlock()
					continue
				}

				switch errors.Cause(err) {
				default:
					mtx.Lock()
					resp.tombstoneErrs.Add(err)
					mtx.Unlock()
					continue
				case ErrorSyncTombstoneCorrupted:
					mtx.Lock()
					resp.corruptedTombstones++
					mtx.Unlock()
				}

				mtx.Lock()
				resp.partial[id] = err
				mtx.Unlock()
			}
			return nil
		})
	}

	// Workers scheduled, distribute blocks.
	eg.Go(func() error {
		defer close(ch)
		return f.bkt.Iter(ctx, TombstoneDir, func(name string) error {
			id, err := ulid.Parse(strings.TrimSuffix(filepath.Base(name), ".json"))
			if err != nil {
				return nil
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- id:
			}

			return nil
		})
	})

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "BaseFetcher: iter bucket")
	}

	if len(resp.tombstoneErrs) > 0 {
		return resp, nil
	}

	//// Only for complete view of blocks update the cache.
	//cached := make(map[ulid.ULID]*metadata.Meta, len(resp.metas))
	//for id, m := range resp.metas {
	//	cached[id] = m
	//}

	//f.mtx.Lock()
	//f.cached = cached
	//f.mtx.Unlock()

	//// Best effort cleanup of disk-cached metas.
	//if f.cacheDir != "" {
	//	fis, err := ioutil.ReadDir(f.cacheDir)
	//	names := make([]string, 0, len(fis))
	//	for _, fi := range fis {
	//		names = append(names, fi.Name())
	//	}
	//	if err != nil {
	//		level.Warn(f.logger).Log("msg", "best effort remove of not needed cached dirs failed; ignoring", "err", err)
	//	} else {
	//		for _, n := range names {
	//			id, ok := IsBlockDir(n)
	//			if !ok {
	//				continue
	//			}
	//
	//			if _, ok := resp.metas[id]; ok {
	//				continue
	//			}
	//
	//			cachedBlockDir := filepath.Join(f.cacheDir, id.String())
	//
	//			// No such block loaded, remove the local dir.
	//			if err := os.RemoveAll(cachedBlockDir); err != nil {
	//				level.Warn(f.logger).Log("msg", "best effort remove of not needed cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
	//			}
	//		}
	//	}
	//}
	return resp, nil
}

func (f *BaseFetcher) fetch(ctx context.Context, metrics *block.FetcherMetrics, filters []TombstoneFilter) (_ map[ulid.ULID]*Tombstone, _ map[ulid.ULID]error, err error) {
	start := time.Now()
	defer func() {
		metrics.SyncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.SyncFailures.Inc()
		}
	}()
	metrics.Syncs.Inc()
	metrics.ResetTx()

	// Run this in thread safe run group.
	v, err := f.g.Do("", func() (i interface{}, err error) {
		// NOTE: First go routine context will go through.
		return f.fetchTombstone(ctx)
	})
	if err != nil {
		return nil, nil, err
	}
	resp := v.(response)

	// Copy as same response might be reused by different goroutines.
	tombstones := make(map[ulid.ULID]*Tombstone, len(resp.tombstoneErrs))
	for id, m := range resp.tombstones {
		tombstones[id] = m
	}

	//metrics.Synced.WithLabelValues(FailedMeta).Set(float64(len(resp.metaErrs)))
	//metrics.Synced.WithLabelValues(NoMeta).Set(resp.noMetas)
	//metrics.Synced.WithLabelValues(CorruptedMeta).Set(resp.corruptedMetas)

	for _, filter := range filters {
		// NOTE: filter can update synced metric accordingly to the reason of the exclude.
		if err := filter.Filter(ctx, tombstones, metrics.Synced); err != nil {
			return nil, nil, errors.Wrap(err, "filter tombstones")
		}
	}

	//metrics.Synced.WithLabelValues(LoadedMeta).Set(float64(len(metas)))
	metrics.Submit()

	if len(resp.tombstoneErrs) > 0 {
		return tombstones, resp.partial, errors.Wrap(resp.tombstoneErrs.Err(), "incomplete view")
	}

	level.Info(f.logger).Log("msg", "successfully synchronized tombstones", "duration", time.Since(start).String(), "duration_ms", time.Since(start).Milliseconds(), "returned", len(tombstones), "partial", len(resp.partial))
	return tombstones, resp.partial, nil
}

// TombstoneDelayFilter considers the tombstone file only after it exists longer than the delay period.
type TombstoneDelayFilter struct {
	delay  time.Duration
	logger log.Logger
}

func (f *TombstoneDelayFilter) Filter(_ context.Context, tombstones map[ulid.ULID]*Tombstone, synced *extprom.TxGaugeVec) error {
	for id := range tombstones {
		if ulid.Now()-id.Time() < uint64(f.delay/time.Millisecond) {
			level.Debug(f.logger).Log("msg", "tombstone is too fresh for now", "tombstone", id)
			delete(tombstones, id)
		}
	}

	return nil
}
