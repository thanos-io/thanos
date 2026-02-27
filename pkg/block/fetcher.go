// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/easyproto"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/groupcache/singleflight"
	"github.com/oklog/ulid/v2"
	objstoretracing "github.com/thanos-io/objstore/tracing/opentracing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/client"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/exthttp"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const FetcherConcurrency = 32

// BaseFetcherMetrics holds metrics tracked by the base fetcher. This struct and its fields are exported
// to allow depending projects (eg. Cortex) to implement their own custom metadata fetcher while tracking
// compatible metrics.
type BaseFetcherMetrics struct {
	Syncs      prometheus.Counter
	CacheBusts prometheus.Counter
}

// FetcherMetrics holds metrics tracked by the metadata fetcher. This struct and its fields are exported
// to allow depending projects (eg. Cortex) to implement their own custom metadata fetcher while tracking
// compatible metrics.
type FetcherMetrics struct {
	Syncs        prometheus.Counter
	SyncFailures prometheus.Counter
	SyncDuration prometheus.Observer

	Synced   *extprom.TxGaugeVec
	Modified *extprom.TxGaugeVec
}

// Submit applies new values for metrics tracked by transaction GaugeVec.
func (s *FetcherMetrics) Submit() {
	s.Synced.Submit()
	s.Modified.Submit()
}

// ResetTx starts new transaction for metrics tracked by transaction GaugeVec.
func (s *FetcherMetrics) ResetTx() {
	s.Synced.ResetTx()
	s.Modified.ResetTx()
}

const (
	FetcherSubSys = "blocks_meta"

	CorruptedMeta = "corrupted-meta-json"
	NoMeta        = "no-meta-json"
	LoadedMeta    = "loaded"
	FailedMeta    = "failed"

	// Synced label values.
	labelExcludedMeta = "label-excluded"
	timeExcludedMeta  = "time-excluded"
	tooFreshMeta      = "too-fresh"
	duplicateMeta     = "duplicate"
	// Blocks that are marked for deletion can be loaded as well. This is done to make sure that we load blocks that are meant to be deleted,
	// but don't have a replacement block yet.
	MarkedForDeletionMeta = "marked-for-deletion"

	// MarkedForNoCompactionMeta is label for blocks which are loaded but also marked for no compaction. This label is also counted in `loaded` label metric.
	MarkedForNoCompactionMeta = "marked-for-no-compact"

	// MarkedForNoDownsampleMeta is label for blocks which are loaded but also marked for no downsample. This label is also counted in `loaded` label metric.
	MarkedForNoDownsampleMeta = "marked-for-no-downsample"

	// ParquetMigratedMeta is label for blocks which are marked as migrated to parquet format.
	ParquetMigratedMeta = "parquet-migrated"

	// Modified label values.
	replicaRemovedMeta = "replica-label-removed"
)

func NewBaseFetcherMetrics(reg prometheus.Registerer) *BaseFetcherMetrics {
	var m BaseFetcherMetrics

	m.Syncs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: FetcherSubSys,
		Name:      "base_syncs_total",
		Help:      "Total blocks metadata synchronization attempts by base Fetcher",
	})
	m.CacheBusts = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: FetcherSubSys,
		Name:      "base_cache_busts_total",
		Help:      "Total blocks metadata cache busts by base Fetcher",
	})

	return &m
}

func NewFetcherMetrics(reg prometheus.Registerer, syncedExtraLabels, modifiedExtraLabels [][]string) *FetcherMetrics {
	var m FetcherMetrics

	m.Syncs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: FetcherSubSys,
		Name:      "syncs_total",
		Help:      "Total blocks metadata synchronization attempts",
	})
	m.SyncFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Subsystem: FetcherSubSys,
		Name:      "sync_failures_total",
		Help:      "Total blocks metadata synchronization failures",
	})
	m.SyncDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Subsystem: FetcherSubSys,
		Name:      "sync_duration_seconds",
		Help:      "Duration of the blocks metadata synchronization in seconds",
		Buckets:   []float64{0.01, 1, 10, 100, 300, 600, 1000},
	})
	m.Synced = extprom.NewTxGaugeVec(
		reg,
		prometheus.GaugeOpts{
			Subsystem: FetcherSubSys,
			Name:      "synced",
			Help:      "Number of block metadata synced",
		},
		[]string{"state"},
		append(DefaultSyncedStateLabelValues(), syncedExtraLabels...)...,
	)
	m.Modified = extprom.NewTxGaugeVec(
		reg,
		prometheus.GaugeOpts{
			Subsystem: FetcherSubSys,
			Name:      "modified",
			Help:      "Number of blocks whose metadata changed",
		},
		[]string{"modified"},
		append(DefaultModifiedLabelValues(), modifiedExtraLabels...)...,
	)
	return &m
}

func DefaultSyncedStateLabelValues() [][]string {
	return [][]string{
		{CorruptedMeta},
		{NoMeta},
		{LoadedMeta},
		{tooFreshMeta},
		{FailedMeta},
		{labelExcludedMeta},
		{timeExcludedMeta},
		{duplicateMeta},
		{MarkedForDeletionMeta},
		{MarkedForNoCompactionMeta},
		{ParquetMigratedMeta},
	}
}

func DefaultModifiedLabelValues() [][]string {
	return [][]string{
		{replicaRemovedMeta},
	}
}

// Lister lists block IDs from a bucket.
type Lister interface {
	// GetActiveAndPartialBlockIDs GetActiveBlocksIDs returning it via channel (streaming) and response.
	// Active blocks are blocks which contain meta.json, while partial blocks are blocks without meta.json
	GetActiveAndPartialBlockIDs(ctx context.Context, activeBlocks chan<- ActiveBlockFetchData) (partialBlocks map[ulid.ULID]bool, err error)
}

// RecursiveLister lists block IDs by recursively iterating through a bucket.
type RecursiveLister struct {
	logger log.Logger
	bkt    objstore.InstrumentedBucketReader
}

func NewRecursiveLister(logger log.Logger, bkt objstore.InstrumentedBucketReader) *RecursiveLister {
	return &RecursiveLister{
		logger: logger,
		bkt:    bkt,
	}
}

type ActiveBlockFetchData struct {
	lastModified time.Time
	ulid.ULID
}

func (f *RecursiveLister) GetActiveAndPartialBlockIDs(ctx context.Context, activeBlocks chan<- ActiveBlockFetchData) (partialBlocks map[ulid.ULID]bool, err error) {
	partialBlocks = make(map[ulid.ULID]bool)

	err = f.bkt.IterWithAttributes(ctx, "", func(attrs objstore.IterObjectAttributes) error {
		name := attrs.Name

		parts := strings.Split(name, "/")
		dir, file := parts[0], parts[len(parts)-1]
		id, ok := IsBlockDir(dir)
		if !ok {
			return nil
		}
		if _, ok := partialBlocks[id]; !ok {
			partialBlocks[id] = true
		}
		if !IsBlockMetaFile(file) {
			return nil
		}

		lastModified, _ := attrs.LastModified()
		partialBlocks[id] = false

		select {
		case <-ctx.Done():
			return ctx.Err()
		case activeBlocks <- ActiveBlockFetchData{
			ULID:         id,
			lastModified: lastModified,
		}:
		}
		return nil
	}, objstore.WithUpdatedAt(), objstore.WithRecursiveIter())
	return partialBlocks, err
}

// ConcurrentLister lists block IDs by doing a top level iteration of the bucket
// followed by one Exists call for each discovered block to detect partial blocks.
type ConcurrentLister struct {
	logger log.Logger
	bkt    objstore.InstrumentedBucketReader
}

func NewConcurrentLister(logger log.Logger, bkt objstore.InstrumentedBucketReader) *ConcurrentLister {
	return &ConcurrentLister{
		logger: logger,
		bkt:    bkt,
	}
}

func (f *ConcurrentLister) GetActiveAndPartialBlockIDs(ctx context.Context, activeBlocks chan<- ActiveBlockFetchData) (partialBlocks map[ulid.ULID]bool, err error) {
	const concurrency = 64

	partialBlocks = make(map[ulid.ULID]bool)

	var (
		metaChan = make(chan ulid.ULID, concurrency)
		eg, gCtx = errgroup.WithContext(ctx)
		mu       sync.Mutex
	)
	for range concurrency {
		eg.Go(func() error {
			for uid := range metaChan {
				// TODO(bwplotka): If that causes problems (obj store rate limits), add longer ttl to cached items.
				// For 1y and 100 block sources this generates ~1.5-3k HEAD RPM. AWS handles 330k RPM per prefix.
				// TODO(bwplotka): Consider filtering by consistency delay here (can't do until compactor healthyOverride work).
				metaFile := path.Join(uid.String(), MetaFilename)
				ok, err := f.bkt.Exists(gCtx, metaFile)
				if err != nil {
					return errors.Wrapf(err, "meta.json file exists: %v", uid)
				}
				if !ok {
					mu.Lock()
					partialBlocks[uid] = true
					mu.Unlock()
					continue
				}

				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case activeBlocks <- ActiveBlockFetchData{
					ULID:         uid,
					lastModified: time.Time{}, // Not used, cache busting is only implemented by the recursive lister because otherwise we would have to call Attributes() (one extra call).
				}:
				}
			}
			return nil
		})
	}

	if err = f.bkt.Iter(ctx, "", func(name string) error {
		id, ok := IsBlockDir(name)
		if !ok {
			return nil
		}
		select {
		case <-gCtx.Done():
			return gCtx.Err()
		case metaChan <- id:
		}
		return nil
	}); err != nil {
		return nil, err
	}
	close(metaChan)

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return partialBlocks, nil
}

type MetadataFetcher interface {
	Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error)
	UpdateOnChange(func([]metadata.Meta, error))
}

// GaugeVec hides something like a Prometheus GaugeVec or an extprom.TxGaugeVec.
type GaugeVec interface {
	WithLabelValues(lvs ...string) prometheus.Gauge
}

// Filter allows filtering or modifying metas from the provided map or returns error.
type MetadataFilter interface {
	Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error
}

// BaseFetcher is a struct that synchronizes filtered metadata of all block in the object storage with the local state.
// Go-routine safe.
type BaseFetcher struct {
	logger         log.Logger
	concurrency    int
	bkt            objstore.InstrumentedBucketReader
	blockIDsLister Lister

	// Optional local directory to cache meta.json files.
	cacheDir   string
	syncs      prometheus.Counter
	cacheBusts prometheus.Counter
	g          singleflight.Group

	mtx sync.Mutex

	cached *sync.Map

	modifiedTimestamps map[ulid.ULID]time.Time
}

// NewBaseFetcher constructs BaseFetcher.
func NewBaseFetcher(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, blockIDsFetcher Lister, dir string, reg prometheus.Registerer) (*BaseFetcher, error) {
	return NewBaseFetcherWithMetrics(logger, concurrency, bkt, blockIDsFetcher, dir, NewBaseFetcherMetrics(reg))
}

// NewBaseFetcherWithMetrics constructs BaseFetcher.
func NewBaseFetcherWithMetrics(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, blockIDsLister Lister, dir string, metrics *BaseFetcherMetrics) (*BaseFetcher, error) {
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
		logger:         log.With(logger, "component", "block.BaseFetcher"),
		concurrency:    concurrency,
		bkt:            bkt,
		blockIDsLister: blockIDsLister,
		cacheDir:       cacheDir,
		cached:         &sync.Map{},
		syncs:          metrics.Syncs,
		cacheBusts:     metrics.CacheBusts,
	}, nil
}

// NewRawMetaFetcher returns basic meta fetcher without proper handling for eventual consistent backends or partial uploads.
// NOTE: Not suitable to use in production.
func NewRawMetaFetcher(logger log.Logger, bkt objstore.InstrumentedBucketReader, blockIDsFetcher Lister) (*MetaFetcher, error) {
	return NewMetaFetcher(logger, 1, bkt, blockIDsFetcher, "", nil, nil)
}

// NewMetaFetcher returns meta fetcher.
func NewMetaFetcher(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, blockIDsFetcher Lister, dir string, reg prometheus.Registerer, filters []MetadataFilter) (*MetaFetcher, error) {
	b, err := NewBaseFetcher(logger, concurrency, bkt, blockIDsFetcher, dir, reg)
	if err != nil {
		return nil, err
	}
	return b.NewMetaFetcher(reg, filters), nil
}

// NewMetaFetcherWithMetrics returns meta fetcher.
func NewMetaFetcherWithMetrics(logger log.Logger, concurrency int, bkt objstore.InstrumentedBucketReader, blockIDsFetcher Lister, dir string, baseFetcherMetrics *BaseFetcherMetrics, fetcherMetrics *FetcherMetrics, filters []MetadataFilter) (*MetaFetcher, error) {
	b, err := NewBaseFetcherWithMetrics(logger, concurrency, bkt, blockIDsFetcher, dir, baseFetcherMetrics)
	if err != nil {
		return nil, err
	}
	return b.NewMetaFetcherWithMetrics(fetcherMetrics, filters), nil
}

// NewMetaFetcher transforms BaseFetcher into actually usable *MetaFetcher.
func (f *BaseFetcher) NewMetaFetcher(reg prometheus.Registerer, filters []MetadataFilter, logTags ...any) *MetaFetcher {
	return f.NewMetaFetcherWithMetrics(NewFetcherMetrics(reg, nil, nil), filters, logTags...)
}

// NewMetaFetcherWithMetrics transforms BaseFetcher into actually usable *MetaFetcher.
func (f *BaseFetcher) NewMetaFetcherWithMetrics(fetcherMetrics *FetcherMetrics, filters []MetadataFilter, logTags ...any) *MetaFetcher {
	return &MetaFetcher{metrics: fetcherMetrics, wrapped: f, filters: filters, logger: log.With(f.logger, logTags...)}
}

var (
	ErrorSyncMetaNotFound  = errors.New("meta.json not found")
	ErrorSyncMetaCorrupted = errors.New("meta.json corrupted")
)

func (f *BaseFetcher) metaUpdated(id ulid.ULID, modified time.Time) bool {
	if f.modifiedTimestamps[id].IsZero() {
		return false
	}
	return !f.modifiedTimestamps[id].Equal(modified)
}

func (f *BaseFetcher) bustCacheForID(id ulid.ULID) {
	f.cacheBusts.Inc()

	f.cached.Delete(id)
	if err := os.RemoveAll(filepath.Join(f.cacheDir, id.String())); err != nil {
		level.Warn(f.logger).Log("msg", "failed to remove cached meta.json dir", "dir", filepath.Join(f.cacheDir, id.String()), "err", err)
	}
}

// loadMeta returns metadata from object storage or error.
// It returns `ErrorSyncMetaNotFound` and `ErrorSyncMetaCorrupted` sentinel errors in those cases.
func (f *BaseFetcher) loadMeta(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	var (
		metaFile       = path.Join(id.String(), MetaFilename)
		cachedBlockDir = filepath.Join(f.cacheDir, id.String())
	)

	if m, seen := f.cached.Load(id); seen {
		return m.(*metadata.Meta), nil
	}

	// Best effort load from local dir.
	if f.cacheDir != "" {
		m, err := metadata.ReadFromDir(cachedBlockDir)
		if err == nil {
			return m, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			level.Warn(f.logger).Log("msg", "best effort read of the local meta.json failed; removing cached block dir", "dir", cachedBlockDir, "err", err)
			if err := os.RemoveAll(cachedBlockDir); err != nil {
				level.Warn(f.logger).Log("msg", "best effort remove of cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
			}
		}
	}

	r, err := f.bkt.ReaderWithExpectedErrs(f.bkt.IsObjNotFoundErr).Get(ctx, metaFile)
	if f.bkt.IsObjNotFoundErr(err) {
		// Meta.json was deleted between bkt.Exists and here.
		return nil, errors.Wrapf(ErrorSyncMetaNotFound, "%v", err)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "get meta file: %v", metaFile)
	}

	defer runutil.CloseWithLogOnErr(f.logger, r, "close bkt meta get")

	metaContent, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta file: %v", metaFile)
	}

	m := &metadata.Meta{}
	if err := json.Unmarshal(metaContent, m); err != nil {
		return nil, errors.Wrapf(ErrorSyncMetaCorrupted, "meta.json %v unmarshal: %v", metaFile, err)
	}

	if m.Version != metadata.TSDBVersion1 {
		return nil, errors.Errorf("unexpected meta file: %s version: %d", metaFile, m.Version)
	}

	// Best effort cache in local dir.
	if f.cacheDir != "" {
		if err := os.MkdirAll(cachedBlockDir, os.ModePerm); err != nil {
			level.Warn(f.logger).Log("msg", "best effort mkdir of the meta.json block dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}

		if err := m.WriteToDir(f.logger, cachedBlockDir); err != nil {
			level.Warn(f.logger).Log("msg", "best effort save of the meta.json to local dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}
	}
	return m, nil
}

type response struct {
	metas              map[ulid.ULID]*metadata.Meta
	partial            map[ulid.ULID]error
	modifiedTimestamps map[ulid.ULID]time.Time
	// If metaErr > 0 it means incomplete view, so some metas, failed to be loaded.
	metaErrs errutil.MultiError

	noMetas        float64
	corruptedMetas float64
}

func (f *BaseFetcher) fetchMetadata(ctx context.Context) (any, error) {
	f.syncs.Inc()

	var (
		resp = response{
			metas:              make(map[ulid.ULID]*metadata.Meta),
			partial:            make(map[ulid.ULID]error),
			modifiedTimestamps: make(map[ulid.ULID]time.Time),
		}
		eg             errgroup.Group
		activeBlocksCh = make(chan ActiveBlockFetchData, f.concurrency)
		mtx            sync.Mutex
	)
	level.Debug(f.logger).Log("msg", "fetching meta data", "concurrency", f.concurrency)
	for i := 0; i < f.concurrency; i++ {
		eg.Go(func() error {
			for activeBlockFetchMD := range activeBlocksCh {
				id := activeBlockFetchMD.ULID

				if f.metaUpdated(id, activeBlockFetchMD.lastModified) {
					f.bustCacheForID(id)
				}

				meta, err := f.loadMeta(ctx, id)
				if err == nil {
					mtx.Lock()
					resp.metas[id] = meta
					resp.modifiedTimestamps[id] = activeBlockFetchMD.lastModified
					mtx.Unlock()
					continue
				}

				switch errors.Cause(err) {
				default:
					mtx.Lock()
					resp.metaErrs.Add(err)
					mtx.Unlock()
					continue
				case ErrorSyncMetaNotFound:
					mtx.Lock()
					resp.noMetas++
					mtx.Unlock()
				case ErrorSyncMetaCorrupted:
					mtx.Lock()
					resp.corruptedMetas++
					mtx.Unlock()
				}

				mtx.Lock()
				resp.partial[id] = err
				mtx.Unlock()
			}
			return nil
		})
	}

	var partialBlocks map[ulid.ULID]bool
	var err error
	// Workers scheduled, distribute blocks.
	eg.Go(func() error {
		defer close(activeBlocksCh)
		partialBlocks, err = f.blockIDsLister.GetActiveAndPartialBlockIDs(ctx, activeBlocksCh)
		return err
	})

	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "BaseFetcher: iter bucket")
	}

	mtx.Lock()
	for blockULID, isPartial := range partialBlocks {
		if isPartial {
			resp.partial[blockULID] = errors.Errorf("block %s has no meta file", blockULID)
			resp.noMetas++
		}
	}
	mtx.Unlock()

	if len(resp.metaErrs) > 0 {
		return resp, nil
	}

	// Only for complete view of blocks update the cache.

	cached := &sync.Map{}
	for id, m := range resp.metas {
		cached.Store(id, m)
	}

	modifiedTimestamps := make(map[ulid.ULID]time.Time, len(resp.modifiedTimestamps))
	maps.Copy(modifiedTimestamps, resp.modifiedTimestamps)

	f.mtx.Lock()
	f.cached = cached
	f.modifiedTimestamps = modifiedTimestamps
	f.mtx.Unlock()

	// Best effort cleanup of disk-cached metas.
	if f.cacheDir != "" {
		fis, err := os.ReadDir(f.cacheDir)
		names := make([]string, 0, len(fis))
		for _, fi := range fis {
			names = append(names, fi.Name())
		}
		if err != nil {
			level.Warn(f.logger).Log("msg", "best effort remove of not needed cached dirs failed; ignoring", "err", err)
		} else {
			for _, n := range names {
				id, ok := IsBlockDir(n)
				if !ok {
					continue
				}

				if _, ok := resp.metas[id]; ok {
					continue
				}

				cachedBlockDir := filepath.Join(f.cacheDir, id.String())

				// No such block loaded, remove the local dir.
				if err := os.RemoveAll(cachedBlockDir); err != nil {
					level.Warn(f.logger).Log("msg", "best effort remove of not needed cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
				}
			}
		}
	}
	return resp, nil
}

func (f *BaseFetcher) fetch(ctx context.Context, metrics *FetcherMetrics, filters []MetadataFilter) (_ map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]error, err error) {
	start := time.Now()
	defer func() {
		metrics.SyncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.SyncFailures.Inc()
		}
	}()

	// Run this in thread safe run group.
	// TODO(bwplotka): Consider custom singleflight with ttl.
	v, err := f.g.Do("", func() (i any, err error) {
		// NOTE: First go routine context will go through.
		return f.fetchMetadata(ctx)
	})
	if err != nil {
		return nil, nil, err
	}
	resp := v.(response)

	// Copy as same response might be reused by different goroutines.
	metas := make(map[ulid.ULID]*metadata.Meta, len(resp.metas))
	maps.Copy(metas, resp.metas)

	metrics.Synced.WithLabelValues(FailedMeta).Set(float64(len(resp.metaErrs)))
	metrics.Synced.WithLabelValues(NoMeta).Set(resp.noMetas)
	metrics.Synced.WithLabelValues(CorruptedMeta).Set(resp.corruptedMetas)

	for _, filter := range filters {
		// NOTE: filter can update synced metric accordingly to the reason of the exclude.
		if err := filter.Filter(ctx, metas, metrics.Synced, metrics.Modified); err != nil {
			return nil, nil, errors.Wrap(err, "filter metas")
		}
	}

	metrics.Synced.WithLabelValues(LoadedMeta).Set(float64(len(metas)))

	if len(resp.metaErrs) > 0 {
		return metas, resp.partial, errors.Wrap(resp.metaErrs.Err(), "incomplete view")
	}

	level.Info(f.logger).Log("msg", "successfully synchronized block metadata", "duration", time.Since(start).String(), "duration_ms", time.Since(start).Milliseconds(), "cached", f.countCached(), "returned", len(metas), "partial", len(resp.partial))
	return metas, resp.partial, nil
}

func (f *BaseFetcher) countCached() int {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	var i int
	f.cached.Range(func(_, _ any) bool {
		i++
		return true
	})
	return i
}

type MetaFetcher struct {
	wrapped *BaseFetcher
	metrics *FetcherMetrics

	filters []MetadataFilter

	listener func([]metadata.Meta, error)

	logger log.Logger
}

// Fetch returns all block metas as well as partial blocks (blocks without or with corrupted meta file) from the bucket.
// It's caller responsibility to not change the returned metadata files. Maps can be modified.
//
// Returned error indicates a failure in fetching metadata. Returned meta can be assumed as correct, with some blocks missing.
func (f *MetaFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error) {
	f.metrics.Syncs.Inc()
	f.metrics.ResetTx()

	metas, partial, err = f.wrapped.fetch(ctx, f.metrics, f.filters)
	if f.listener != nil {
		blocks := make([]metadata.Meta, 0, len(metas))
		for _, meta := range metas {
			blocks = append(blocks, *meta)
		}
		f.listener(blocks, err)
	}

	f.metrics.Submit()
	return metas, partial, err
}

// UpdateOnChange allows to add listener that will be update on every change.
func (f *MetaFetcher) UpdateOnChange(listener func([]metadata.Meta, error)) {
	f.listener = listener
}

var _ MetadataFilter = &TimePartitionMetaFilter{}

// TimePartitionMetaFilter is a BaseFetcher filter that filters out blocks that are outside of specified time range.
// Not go-routine safe.
type TimePartitionMetaFilter struct {
	minTime, maxTime model.TimeOrDurationValue
}

// NewTimePartitionMetaFilter creates TimePartitionMetaFilter.
func NewTimePartitionMetaFilter(MinTime, MaxTime model.TimeOrDurationValue) *TimePartitionMetaFilter {
	return &TimePartitionMetaFilter{minTime: MinTime, maxTime: MaxTime}
}

// Filter filters out blocks that are outside of specified time range.
func (f *TimePartitionMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	for id, m := range metas {
		if m.MaxTime >= f.minTime.PrometheusTimestamp() && m.MinTime <= f.maxTime.PrometheusTimestamp() {
			continue
		}
		synced.WithLabelValues(timeExcludedMeta).Inc()
		delete(metas, id)
	}
	return nil
}

var _ MetadataFilter = &LabelShardedMetaFilter{}

// LabelShardedMetaFilter represents struct that allows sharding.
// Not go-routine safe.
type LabelShardedMetaFilter struct {
	relabelConfig []*relabel.Config
}

// NewLabelShardedMetaFilter creates LabelShardedMetaFilter.
func NewLabelShardedMetaFilter(relabelConfig []*relabel.Config) *LabelShardedMetaFilter {
	return &LabelShardedMetaFilter{relabelConfig: relabelConfig}
}

// Special label that will have an ULID of the meta.json being referenced to.
const BlockIDLabel = "__block_id"

// Filter filters out blocks that have no labels after relabelling of each block external (Thanos) labels.
func (f *LabelShardedMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	var b labels.Builder
	for id, m := range metas {
		b.Reset(labels.EmptyLabels())
		b.Set(BlockIDLabel, id.String())

		for k, v := range m.Thanos.Labels {
			b.Set(k, v)
		}

		if processedLabels, _ := relabel.Process(b.Labels(), f.relabelConfig...); processedLabels.IsEmpty() {
			synced.WithLabelValues(labelExcludedMeta).Inc()
			delete(metas, id)
		}
	}
	return nil
}

var _ MetadataFilter = &DefaultDeduplicateFilter{}

type DeduplicateFilter interface {
	DuplicateIDs() []ulid.ULID
}

// DefaultDeduplicateFilter is a BaseFetcher filter that filters out older blocks that have exactly the same data.
// Not go-routine safe.
type DefaultDeduplicateFilter struct {
	duplicateIDs []ulid.ULID
	concurrency  int
	mu           sync.Mutex
}

// NewDeduplicateFilter creates DefaultDeduplicateFilter.
func NewDeduplicateFilter(concurrency int) *DefaultDeduplicateFilter {
	return &DefaultDeduplicateFilter{concurrency: concurrency}
}

// Filter filters out duplicate blocks that can be formed
// from two or more overlapping blocks that fully submatches the source blocks of the older blocks.
func (f *DefaultDeduplicateFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	var filterWg, dupWg sync.WaitGroup
	var groupChan = make(chan []*metadata.Meta)

	var dupsChan = make(chan ulid.ULID)

	dupWg.Go(func() {
		dups := make([]ulid.ULID, 0)
		for dup := range dupsChan {
			if metas[dup] != nil {
				dups = append(dups, dup)
			}
			synced.WithLabelValues(duplicateMeta).Inc()
			delete(metas, dup)
		}
		f.mu.Lock()
		f.duplicateIDs = dups
		f.mu.Unlock()
	})

	// Start up workers to deduplicate workgroups when they're ready.
	for i := 0; i < f.concurrency; i++ {
		filterWg.Go(func() {
			for group := range groupChan {
				f.filterGroup(group, dupsChan)
			}
		})
	}

	// We need only look within a compaction group for duplicates, so splitting by group key gives us parallelizable streams.
	metasByCompactionGroup := make(map[string][]*metadata.Meta)
	for _, meta := range metas {
		groupKey := meta.Thanos.GroupKey()
		metasByCompactionGroup[groupKey] = append(metasByCompactionGroup[groupKey], meta)
	}
	for _, group := range metasByCompactionGroup {
		groupChan <- group
	}
	close(groupChan)
	filterWg.Wait()

	close(dupsChan)
	dupWg.Wait()

	return nil
}

func (f *DefaultDeduplicateFilter) filterGroup(metaSlice []*metadata.Meta, dupsChan chan ulid.ULID) {
	sort.Slice(metaSlice, func(i, j int) bool {
		ilen := len(metaSlice[i].Compaction.Sources)
		jlen := len(metaSlice[j].Compaction.Sources)

		if ilen == jlen {
			return metaSlice[i].ULID.Compare(metaSlice[j].ULID) < 0
		}

		return ilen-jlen > 0
	})

	var coveringSet []*metadata.Meta
	var duplicates []ulid.ULID
childLoop:
	for _, child := range metaSlice {
		childSources := child.Compaction.Sources
		for _, parent := range coveringSet {
			parentSources := parent.Compaction.Sources

			// child's sources are present in parent's sources, filter it out.
			if contains(parentSources, childSources) {
				duplicates = append(duplicates, child.ULID)
				continue childLoop
			}
		}

		// Child's sources not covered by any member of coveringSet, add it to coveringSet.
		coveringSet = append(coveringSet, child)
	}

	for _, duplicate := range duplicates {
		dupsChan <- duplicate
	}
}

// DuplicateIDs returns slice of block ids that are filtered out by DefaultDeduplicateFilter.
func (f *DefaultDeduplicateFilter) DuplicateIDs() []ulid.ULID {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.duplicateIDs
}

func contains(s1, s2 []ulid.ULID) bool {
	for _, a := range s2 {
		found := false
		for _, e := range s1 {
			if a.Compare(e) == 0 {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

var _ MetadataFilter = &ReplicaLabelRemover{}

// ReplicaLabelRemover is a BaseFetcher filter that modifies external labels of existing blocks, it removes given replica labels from the metadata of blocks that have it.
type ReplicaLabelRemover struct {
	logger log.Logger

	replicaLabels []string
}

// NewReplicaLabelRemover creates a ReplicaLabelRemover.
func NewReplicaLabelRemover(logger log.Logger, replicaLabels []string) *ReplicaLabelRemover {
	return &ReplicaLabelRemover{logger: logger, replicaLabels: replicaLabels}
}

// Filter modifies external labels of existing blocks, it removes given replica labels from the metadata of blocks that have it.
func (r *ReplicaLabelRemover) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	if len(r.replicaLabels) == 0 {
		return nil
	}

	countReplicaLabelRemoved := make(map[string]int, len(metas))
	for u, meta := range metas {
		l := make(map[string]string)
		maps.Copy(l, meta.Thanos.Labels)

		for _, replicaLabel := range r.replicaLabels {
			if _, exists := l[replicaLabel]; exists {
				delete(l, replicaLabel)
				countReplicaLabelRemoved[replicaLabel] += 1
				modified.WithLabelValues(replicaRemovedMeta).Inc()
			}
		}
		if len(l) == 0 {
			level.Warn(r.logger).Log("msg", "block has no labels left, creating one", r.replicaLabels[0], "deduped")
			l[r.replicaLabels[0]] = "deduped"
		}

		nm := *meta
		nm.Thanos.Labels = l
		metas[u] = &nm
	}
	for replicaLabelRemoved, count := range countReplicaLabelRemoved {
		level.Debug(r.logger).Log("msg", "removed replica label", "label", replicaLabelRemoved, "count", count)
	}
	return nil
}

// ConsistencyDelayMetaFilter is a BaseFetcher filter that filters out blocks that are created before a specified consistency delay.
// Not go-routine safe.
type ConsistencyDelayMetaFilter struct {
	logger           log.Logger
	consistencyDelay time.Duration
}

// NewConsistencyDelayMetaFilter creates ConsistencyDelayMetaFilter.
func NewConsistencyDelayMetaFilter(logger log.Logger, consistencyDelay time.Duration, reg prometheus.Registerer) *ConsistencyDelayMetaFilter {
	_ = promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "consistency_delay_seconds",
		Help: "Configured consistency delay in seconds.",
	}, func() float64 {
		return consistencyDelay.Seconds()
	})

	return NewConsistencyDelayMetaFilterWithoutMetrics(logger, consistencyDelay)
}

// NewConsistencyDelayMetaFilterWithoutMetrics creates ConsistencyDelayMetaFilter.
func NewConsistencyDelayMetaFilterWithoutMetrics(logger log.Logger, consistencyDelay time.Duration) *ConsistencyDelayMetaFilter {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &ConsistencyDelayMetaFilter{
		logger:           logger,
		consistencyDelay: consistencyDelay,
	}
}

// Filter filters out blocks that filters blocks that have are created before a specified consistency delay.
func (f *ConsistencyDelayMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	for id, meta := range metas {
		var metaUploadTime = meta.Thanos.UploadTime

		var tooFresh bool
		if !metaUploadTime.IsZero() {
			tooFresh = time.Since(metaUploadTime) < f.consistencyDelay
		} else {
			tooFresh = ulid.Now()-id.Time() < uint64(f.consistencyDelay/time.Millisecond)
		}

		// TODO(khyatisoneji): Remove the checks about Thanos Source
		//  by implementing delete delay to fetch metas.
		if tooFresh &&
			meta.Thanos.Source != metadata.BucketRepairSource &&
			meta.Thanos.Source != metadata.CompactorSource &&
			meta.Thanos.Source != metadata.CompactorRepairSource {

			level.Debug(f.logger).Log("msg", "block is too fresh for now", "block", id)
			synced.WithLabelValues(tooFreshMeta).Inc()
			delete(metas, id)
		}
	}

	return nil
}

// IgnoreParquetConvertedBlocksFilter is a filter that filters out blocks that have been converted into Parquet blocks.
// It takes a look at the metadata files inside of a given bucket and then filters them out during syncing.
type IgnoreParquetConvertedBlocksFilter struct {
	bkt         objstore.InstrumentedBucketReader
	concurrency int
	logger      log.Logger
}

func NewIgnoreParquetConvertedBlocksFilter(logger log.Logger, config []byte, concurrency int, reg prometheus.Registerer) (*IgnoreParquetConvertedBlocksFilter, error) {
	if len(config) == 0 {
		return &IgnoreParquetConvertedBlocksFilter{}, nil
	}
	customBktConfig := exthttp.DefaultCustomBucketConfig()
	if err := yaml.Unmarshal(config, &customBktConfig); err != nil {
		return nil, errors.Wrap(err, "parsing config YAML file")
	}
	bkt, err := client.NewBucket(logger, config, component.Store.String(), exthttp.CreateHedgedTransportWithConfig(customBktConfig))
	if err != nil {
		return nil, err
	}
	insBkt := objstoretracing.WrapWithTraces(objstore.WrapWithMetrics(bkt, reg, bkt.Name()))

	return &IgnoreParquetConvertedBlocksFilter{bkt: insBkt, concurrency: concurrency, logger: logger}, nil
}

const parquetMetaFileName = "meta.pb"

func (f *IgnoreParquetConvertedBlocksFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	if f.bkt == nil {
		return nil
	}

	var (
		eg                errgroup.Group
		ch                = make(chan string, f.concurrency)
		mtx               sync.Mutex
		allMigratedBlocks = make(map[ulid.ULID]struct{})
		merrs             errutil.MultiError
	)

	for range f.concurrency {
		eg.Go(func() error {
			for path := range ch {
				migratedBlocks, err := f.readMigratedBlocksFromParquetMetadata(ctx, path)
				if err != nil {
					mtx.Lock()
					merrs.Add(fmt.Errorf("failed to read parquet metadata from %s: %w", path, err))
					mtx.Unlock()

					continue
				}

				mtx.Lock()
				for id := range migratedBlocks {
					allMigratedBlocks[id] = struct{}{}
				}
				mtx.Unlock()
			}
			return nil
		})
	}

	eg.Go(func() error {
		defer close(ch)

		return f.bkt.Iter(ctx, "", func(name string) error {
			if path.Base(name) == parquetMetaFileName {
				select {
				case ch <- name:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}, objstore.WithRecursiveIter())
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "iterate bucket for parquet metadata")
	}

	if len(merrs) > 0 {
		return errors.Wrap(merrs.Err(), "read parquet metadata files")
	}

	for id := range allMigratedBlocks {
		if _, exists := metas[id]; exists {
			delete(metas, id)
			synced.WithLabelValues("parquet-converted").Inc()
		}
	}

	return nil
}

func (f *IgnoreParquetConvertedBlocksFilter) readMigratedBlocksFromParquetMetadata(ctx context.Context, path string) (map[ulid.ULID]struct{}, error) {
	migratedBlocks := make(map[ulid.ULID]struct{})
	r, err := f.bkt.Get(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "get parquet metadata file: %v", path)
	}
	defer runutil.CloseWithLogOnErr(f.logger, r, "close bkt get")

	content, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read parquet metadata file: %v", path)
	}

	var fc easyproto.FieldContext
	for len(content) > 0 {
		content, err = fc.NextField(content)
		if err != nil {
			return nil, errors.Wrapf(err, "read next field from parquet metadata file: %v", path)
		}
		switch fc.FieldNum {
		case 6:
			u, ok := fc.String()
			if !ok {
				return nil, errors.Wrapf(err, "read convertedFromBLIDs field from parquet metadata file: %v", path)
			}
			id, err := ulid.Parse(u)
			if err != nil {
				return nil, errors.Wrapf(err, "parse block ID %q from parquet metadata file: %v", u, path)
			}
			migratedBlocks[id] = struct{}{}
		}
	}

	return migratedBlocks, nil
}

// IgnoreDeletionMarkFilter is a filter that filters out the blocks that are marked for deletion after a given delay.
// The delay duration is to make sure that the replacement block can be fetched before we filter out the old block.
// Delay is not considered when computing DeletionMarkBlocks map.
// Not go-routine safe.
type IgnoreDeletionMarkFilter struct {
	logger      log.Logger
	delay       time.Duration
	concurrency int
	bkt         objstore.InstrumentedBucketReader

	mtx             sync.Mutex
	deletionMarkMap map[ulid.ULID]*metadata.DeletionMark
}

// NewIgnoreDeletionMarkFilter creates IgnoreDeletionMarkFilter.
func NewIgnoreDeletionMarkFilter(logger log.Logger, bkt objstore.InstrumentedBucketReader, delay time.Duration, concurrency int) *IgnoreDeletionMarkFilter {
	return &IgnoreDeletionMarkFilter{
		logger:      logger,
		bkt:         bkt,
		delay:       delay,
		concurrency: concurrency,
	}
}

// DeletionMarkBlocks returns block ids that were marked for deletion.
func (f *IgnoreDeletionMarkFilter) DeletionMarkBlocks() map[ulid.ULID]*metadata.DeletionMark {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	deletionMarkMap := make(map[ulid.ULID]*metadata.DeletionMark, len(f.deletionMarkMap))
	maps.Copy(deletionMarkMap, f.deletionMarkMap)

	return deletionMarkMap
}

// Filter filters out blocks that are marked for deletion after a given delay.
// It also returns the blocks that can be deleted since they were uploaded delay duration before current time.
func (f *IgnoreDeletionMarkFilter) Filter(ctx context.Context, metas map[ulid.ULID]*metadata.Meta, synced GaugeVec, modified GaugeVec) error {
	deletionMarkMap := make(map[ulid.ULID]*metadata.DeletionMark)

	// Make a copy of block IDs to check, in order to avoid concurrency issues
	// between the scheduler and workers.
	blockIDs := make([]ulid.ULID, 0, len(metas))
	for id := range metas {
		blockIDs = append(blockIDs, id)
	}

	var (
		eg             errgroup.Group
		ch             = make(chan ulid.ULID, f.concurrency)
		mtx            sync.Mutex
		preFilterMetas = make(map[ulid.ULID]struct{}, len(metas))
	)

	for k := range metas {
		preFilterMetas[k] = struct{}{}
	}

	for i := 0; i < f.concurrency; i++ {
		eg.Go(func() error {
			var lastErr error
			for id := range ch {
				m := &metadata.DeletionMark{}
				if err := metadata.ReadMarker(ctx, f.logger, f.bkt, id.String(), m); err != nil {
					if errors.Cause(err) == metadata.ErrorMarkerNotFound {
						continue
					}
					if errors.Cause(err) == metadata.ErrorUnmarshalMarker {
						level.Warn(f.logger).Log("msg", "found partial deletion-mark.json; if we will see it happening often for the same block, consider manually deleting deletion-mark.json from the object storage", "block", id, "err", err)
						continue
					}
					// Remember the last error and continue to drain the channel.
					lastErr = err
					continue
				}

				// Keep track of the blocks marked for deletion and filter them out if their
				// deletion time is greater than the configured delay.
				mtx.Lock()
				deletionMarkMap[id] = m
				if time.Since(time.Unix(m.DeletionTime, 0)).Seconds() > f.delay.Seconds() {
					synced.WithLabelValues(MarkedForDeletionMeta).Inc()
					delete(metas, id)
				}
				mtx.Unlock()
			}

			return lastErr
		})
	}

	// Workers scheduled, distribute blocks.
	eg.Go(func() error {
		defer close(ch)

		for _, id := range blockIDs {
			select {
			case ch <- id:
				// Nothing to do.
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	})

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "filter blocks marked for deletion")
	}

	f.mtx.Lock()
	if f.deletionMarkMap == nil {
		f.deletionMarkMap = make(map[ulid.ULID]*metadata.DeletionMark)
	}
	maps.Copy(f.deletionMarkMap, deletionMarkMap)

	for u := range f.deletionMarkMap {
		if _, exists := preFilterMetas[u]; exists {
			continue
		}

		delete(f.deletionMarkMap, u)
	}

	f.mtx.Unlock()

	return nil
}

var (
	SelectorSupportedRelabelActions = map[relabel.Action]struct{}{relabel.Keep: {}, relabel.Drop: {}, relabel.HashMod: {}}
)

// ParseRelabelConfig parses relabel configuration.
// If supportedActions not specified, all relabel actions are valid.
func ParseRelabelConfig(contentYaml []byte, supportedActions map[relabel.Action]struct{}) ([]*relabel.Config, error) {
	var relabelConfig []*relabel.Config
	if err := yaml.Unmarshal(contentYaml, &relabelConfig); err != nil {
		return nil, errors.Wrap(err, "parsing relabel configuration")
	}
	for _, cfg := range relabelConfig {
		if err := cfg.Validate(prommodel.UTF8Validation); err != nil {
			return nil, errors.Wrap(err, "validate relabel config")
		}
	}

	if supportedActions != nil {
		for _, cfg := range relabelConfig {
			if _, ok := supportedActions[cfg.Action]; !ok {
				return nil, errors.Errorf("unsupported relabel action: %v", cfg.Action)
			}
		}
	}

	return relabelConfig, nil
}
