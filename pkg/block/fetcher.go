// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package block

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb"
	tsdberrors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/model"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type syncMetrics struct {
	syncs        prometheus.Counter
	syncFailures prometheus.Counter
	syncDuration prometheus.Histogram

	synced *extprom.TxGaugeVec
}

const (
	syncMetricSubSys = "blocks_meta"

	corruptedMeta = "corrupted-meta-json"
	noMeta        = "no-meta-json"
	loadedMeta    = "loaded"
	failedMeta    = "failed"

	// Filter's label values.
	labelExcludedMeta = "label-excluded"
	timeExcludedMeta  = "time-excluded"
	tooFreshMeta      = "too-fresh"
	duplicateMeta     = "duplicate"
)

func newSyncMetrics(r prometheus.Registerer) *syncMetrics {
	var m syncMetrics

	m.syncs = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: syncMetricSubSys,
		Name:      "syncs_total",
		Help:      "Total blocks metadata synchronization attempts",
	})
	m.syncFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Subsystem: syncMetricSubSys,
		Name:      "sync_failures_total",
		Help:      "Total blocks metadata synchronization failures",
	})
	m.syncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Subsystem: syncMetricSubSys,
		Name:      "sync_duration_seconds",
		Help:      "Duration of the blocks metadata synchronization in seconds",
		Buckets:   []float64{0.01, 1, 10, 100, 1000},
	})
	m.synced = extprom.NewTxGaugeVec(prometheus.GaugeOpts{
		Subsystem: syncMetricSubSys,
		Name:      "synced",
		Help:      "Number of block metadata synced",
	},
		[]string{"state"},
		[]string{corruptedMeta},
		[]string{noMeta},
		[]string{loadedMeta},
		[]string{tooFreshMeta},
		[]string{failedMeta},
		[]string{labelExcludedMeta},
		[]string{timeExcludedMeta},
		[]string{duplicateMeta},
	)
	if r != nil {
		r.MustRegister(
			m.syncs,
			m.syncFailures,
			m.syncDuration,
			m.synced,
		)
	}
	return &m
}

type MetadataFetcher interface {
	Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error)
}

type GaugeLabeled interface {
	WithLabelValues(lvs ...string) prometheus.Gauge
}

type MetaFetcherFilter func(metas map[ulid.ULID]*metadata.Meta, synced GaugeLabeled, incompleteView bool)

// MetaFetcher is a struct that synchronizes filtered metadata of all block in the object storage with the local state.
type MetaFetcher struct {
	logger      log.Logger
	concurrency int
	bkt         objstore.BucketReader

	// Optional local directory to cache meta.json files.
	cacheDir string
	metrics  *syncMetrics

	filters []MetaFetcherFilter

	cached map[ulid.ULID]*metadata.Meta
}

// NewMetaFetcher constructs MetaFetcher.
func NewMetaFetcher(logger log.Logger, concurrency int, bkt objstore.BucketReader, dir string, r prometheus.Registerer, filters ...MetaFetcherFilter) (*MetaFetcher, error) {
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

	return &MetaFetcher{
		logger:      log.With(logger, "component", "block.MetaFetcher"),
		concurrency: concurrency,
		bkt:         bkt,
		cacheDir:    cacheDir,
		metrics:     newSyncMetrics(r),
		filters:     filters,
		cached:      map[ulid.ULID]*metadata.Meta{},
	}, nil
}

var (
	ErrorSyncMetaNotFound  = errors.New("meta.json not found")
	ErrorSyncMetaCorrupted = errors.New("meta.json corrupted")
)

// loadMeta returns metadata from object storage or error.
// It returns `ErrorSyncMetaNotFound` and `ErrorSyncMetaCorrupted` sentinel errors in those cases.
func (s *MetaFetcher) loadMeta(ctx context.Context, id ulid.ULID) (*metadata.Meta, error) {
	var (
		metaFile       = path.Join(id.String(), MetaFilename)
		cachedBlockDir = filepath.Join(s.cacheDir, id.String())
	)

	// TODO(bwplotka): If that causes problems (obj store rate limits), add longer ttl to cached items.
	// For 1y and 100 block sources this generates ~1.5-3k HEAD RPM. AWS handles 330k RPM per prefix.
	// TODO(bwplotka): Consider filtering by consistency delay here (can't do until compactor healthyOverride work).
	ok, err := s.bkt.Exists(ctx, metaFile)
	if err != nil {
		return nil, errors.Wrapf(err, "meta.json file exists: %v", metaFile)
	}
	if !ok {
		return nil, ErrorSyncMetaNotFound
	}

	if m, seen := s.cached[id]; seen {
		return m, nil
	}

	// Best effort load from local dir.
	if s.cacheDir != "" {
		m, err := metadata.Read(cachedBlockDir)
		if err == nil {
			return m, nil
		}

		if !stderrors.Is(err, os.ErrNotExist) {
			level.Warn(s.logger).Log("msg", "best effort read of the local meta.json failed; removing cached block dir", "dir", cachedBlockDir, "err", err)
			if err := os.RemoveAll(cachedBlockDir); err != nil {
				level.Warn(s.logger).Log("msg", "best effort remove of cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
			}
		}
	}

	level.Debug(s.logger).Log("msg", "download meta", "name", metaFile)

	r, err := s.bkt.Get(ctx, metaFile)
	if s.bkt.IsObjNotFoundErr(err) {
		// Meta.json was deleted between bkt.Exists and here.
		return nil, errors.Wrapf(ErrorSyncMetaNotFound, "%v", err)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "get meta file: %v", metaFile)
	}

	defer runutil.CloseWithLogOnErr(s.logger, r, "close bkt meta get")

	metaContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta file: %v", metaFile)
	}

	m := &metadata.Meta{}
	if err := json.Unmarshal(metaContent, m); err != nil {
		return nil, errors.Wrapf(ErrorSyncMetaCorrupted, "meta.json %v unmarshal: %v", metaFile, err)
	}

	if m.Version != metadata.MetaVersion1 {
		return nil, errors.Errorf("unexpected meta file: %s version: %d", metaFile, m.Version)
	}

	// Best effort cache in local dir.
	if s.cacheDir != "" {
		if err := os.MkdirAll(cachedBlockDir, os.ModePerm); err != nil {
			level.Warn(s.logger).Log("msg", "best effort mkdir of the meta.json block dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}

		if err := metadata.Write(s.logger, cachedBlockDir, m); err != nil {
			level.Warn(s.logger).Log("msg", "best effort save of the meta.json to local dir failed; ignoring", "dir", cachedBlockDir, "err", err)
		}
	}
	return m, nil
}

// Fetch returns all block metas as well as partial blocks (blocks without or with corrupted meta file) from the bucket.
// It's caller responsibility to not change the returned metadata files. Maps can be modified.
//
// Returned error indicates a failure in fetching metadata. Returned meta can be assumed as correct, with some blocks missing.
func (s *MetaFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error) {
	start := time.Now()
	defer func() {
		s.metrics.syncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			s.metrics.syncFailures.Inc()
		}
	}()
	s.metrics.syncs.Inc()

	metas = make(map[ulid.ULID]*metadata.Meta)
	partial = make(map[ulid.ULID]error)

	var (
		wg  sync.WaitGroup
		ch  = make(chan ulid.ULID, s.concurrency)
		mtx sync.Mutex

		metaErrs tsdberrors.MultiError
	)

	s.metrics.synced.ResetTx()

	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range ch {
				meta, err := s.loadMeta(ctx, id)
				if err == nil {
					mtx.Lock()
					metas[id] = meta
					mtx.Unlock()
					continue
				}

				switch errors.Cause(err) {
				default:
					s.metrics.synced.WithLabelValues(failedMeta).Inc()
					mtx.Lock()
					metaErrs.Add(err)
					mtx.Unlock()
					continue
				case ErrorSyncMetaNotFound:
					s.metrics.synced.WithLabelValues(noMeta).Inc()
				case ErrorSyncMetaCorrupted:
					s.metrics.synced.WithLabelValues(corruptedMeta).Inc()
				}

				mtx.Lock()
				partial[id] = err
				mtx.Unlock()
			}
		}()
	}

	// Workers scheduled, distribute blocks.
	err = s.bkt.Iter(ctx, "", func(name string) error {
		id, ok := IsBlockDir(name)
		if !ok {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- id:
		}

		return nil
	})
	close(ch)

	wg.Wait()
	if err != nil {
		return nil, nil, errors.Wrap(err, "MetaFetcher: iter bucket")
	}

	incompleteView := len(metaErrs) > 0

	// Only for complete view of blocks update the cache.
	if !incompleteView {
		cached := make(map[ulid.ULID]*metadata.Meta, len(metas))
		for id, m := range metas {
			cached[id] = m
		}
		s.cached = cached

		// Best effort cleanup of disk-cached metas.
		if s.cacheDir != "" {
			names, err := fileutil.ReadDir(s.cacheDir)
			if err != nil {
				level.Warn(s.logger).Log("msg", "best effort remove of not needed cached dirs failed; ignoring", "err", err)
			} else {
				for _, n := range names {
					id, ok := IsBlockDir(n)
					if !ok {
						continue
					}

					if _, ok := metas[id]; ok {
						continue
					}

					cachedBlockDir := filepath.Join(s.cacheDir, id.String())

					// No such block loaded, remove the local dir.
					if err := os.RemoveAll(cachedBlockDir); err != nil {
						level.Warn(s.logger).Log("msg", "best effort remove of not needed cached dir failed; ignoring", "dir", cachedBlockDir, "err", err)
					}
				}
			}
		}
	}

	for _, f := range s.filters {
		// NOTE: filter can update synced metric accordingly to the reason of the exclude.
		f(metas, s.metrics.synced, incompleteView)
	}

	s.metrics.synced.WithLabelValues(loadedMeta).Set(float64(len(metas)))
	s.metrics.synced.Submit()

	if incompleteView {
		return metas, partial, errors.Wrap(metaErrs, "incomplete view")
	}

	level.Info(s.logger).Log("msg", "successfully fetched block metadata", "duration", time.Since(start).String(), "cached", len(s.cached), "returned", len(metas), "partial", len(partial))
	return metas, partial, nil
}

var _ MetaFetcherFilter = (&TimePartitionMetaFilter{}).Filter

// TimePartitionMetaFilter is a MetaFetcher filter that filters out blocks that are outside of specified time range.
type TimePartitionMetaFilter struct {
	minTime, maxTime model.TimeOrDurationValue
}

// NewTimePartitionMetaFilter creates TimePartitionMetaFilter.
func NewTimePartitionMetaFilter(MinTime, MaxTime model.TimeOrDurationValue) *TimePartitionMetaFilter {
	return &TimePartitionMetaFilter{minTime: MinTime, maxTime: MaxTime}
}

// Filter filters out blocks that are outside of specified time range.
func (f *TimePartitionMetaFilter) Filter(metas map[ulid.ULID]*metadata.Meta, synced GaugeLabeled, _ bool) {
	for id, m := range metas {
		if m.MaxTime >= f.minTime.PrometheusTimestamp() && m.MinTime <= f.maxTime.PrometheusTimestamp() {
			continue
		}
		synced.WithLabelValues(timeExcludedMeta).Inc()
		delete(metas, id)
	}
}

var _ MetaFetcherFilter = (&LabelShardedMetaFilter{}).Filter

// LabelShardedMetaFilter is a MetaFetcher filter that filters out blocks that have no labels after relabelling.
type LabelShardedMetaFilter struct {
	relabelConfig []*relabel.Config
}

// NewLabelShardedMetaFilter creates LabelShardedMetaFilter.
func NewLabelShardedMetaFilter(relabelConfig []*relabel.Config) *LabelShardedMetaFilter {
	return &LabelShardedMetaFilter{relabelConfig: relabelConfig}
}

// Filter filters out blocks that filters blocks that have no labels after relabelling.
func (f *LabelShardedMetaFilter) Filter(metas map[ulid.ULID]*metadata.Meta, synced GaugeLabeled, _ bool) {
	for id, m := range metas {
		if processedLabels := relabel.Process(labels.FromMap(m.Thanos.Labels), f.relabelConfig...); processedLabels != nil {
			continue
		}
		synced.WithLabelValues(labelExcludedMeta).Inc()
		delete(metas, id)
	}
}

// DeduplicateFilter is a MetaFetcher filter that filters out older blocks that have exactly the same data.
type DeduplicateFilter struct {
	duplicateIDs []ulid.ULID
}

// NewDeduplicateFilter creates DeduplicateFilter.
func NewDeduplicateFilter() *DeduplicateFilter {
	return &DeduplicateFilter{}
}

// Filter filters out duplicate blocks that can be formed
// from two or more overlapping blocks that fully submatches the source blocks of the older blocks.
func (f *DeduplicateFilter) Filter(metas map[ulid.ULID]*metadata.Meta, synced GaugeLabeled, _ bool) {
	var wg sync.WaitGroup

	metasByResolution := make(map[int64][]*metadata.Meta)
	for _, meta := range metas {
		res := meta.Thanos.Downsample.Resolution
		metasByResolution[res] = append(metasByResolution[res], meta)
	}

	for res := range metasByResolution {
		wg.Add(1)
		go func(res int64) {
			defer wg.Done()
			f.filterForResolution(NewNode(&metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID: ulid.MustNew(uint64(0), nil),
				},
			}), metasByResolution[res], metas, res, synced)
		}(res)
	}

	wg.Wait()
}

func (f *DeduplicateFilter) filterForResolution(root *Node, metaSlice []*metadata.Meta, metas map[ulid.ULID]*metadata.Meta, res int64, synced GaugeLabeled) {
	sort.Slice(metaSlice, func(i, j int) bool {
		ilen := len(metaSlice[i].Compaction.Sources)
		jlen := len(metaSlice[j].Compaction.Sources)

		if ilen == jlen {
			return metaSlice[i].ULID.Compare(metaSlice[j].ULID) < 0
		}

		return ilen-jlen > 0
	})

	for _, meta := range metaSlice {
		addNodeBySources(root, NewNode(meta))
	}

	duplicateULIDs := getNonRootIDs(root)
	for _, id := range duplicateULIDs {
		if metas[id] != nil {
			f.duplicateIDs = append(f.duplicateIDs, id)
		}
		synced.WithLabelValues(duplicateMeta).Inc()
		delete(metas, id)
	}
}

// DuplicateIDs returns slice of block ids
// that are filtered out by DeduplicateFilter.
func (f *DeduplicateFilter) DuplicateIDs() []ulid.ULID {
	return f.duplicateIDs
}

func addNodeBySources(root *Node, add *Node) bool {
	var rootNode *Node
	for _, node := range root.Children {
		parentSources := node.Compaction.Sources
		childSources := add.Compaction.Sources

		// Block exists with same sources, add as child.
		if contains(parentSources, childSources) && contains(childSources, parentSources) {
			node.Children = append(node.Children, add)
			return true
		}

		// Block's sources are present in other block's sources, add as child.
		if contains(parentSources, childSources) {
			rootNode = node
			break
		}
	}

	// Block cannot be attached to any child nodes, add it as child of root.
	if rootNode == nil {
		root.Children = append(root.Children, add)
		return true
	}

	return addNodeBySources(rootNode, add)
}

func contains(s1 []ulid.ULID, s2 []ulid.ULID) bool {
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

// ConsistencyDelayMetaFilter is a MetaFetcher filter that filters out blocks that are created before a specified consistency delay.
type ConsistencyDelayMetaFilter struct {
	logger           log.Logger
	consistencyDelay time.Duration
}

// NewConsistencyDelayMetaFilter creates ConsistencyDelayMetaFilter.
func NewConsistencyDelayMetaFilter(logger log.Logger, consistencyDelay time.Duration, reg prometheus.Registerer) *ConsistencyDelayMetaFilter {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	consistencyDelayMetric := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "consistency_delay_seconds",
		Help: "Configured consistency delay in seconds.",
	}, func() float64 {
		return consistencyDelay.Seconds()
	})
	reg.MustRegister(consistencyDelayMetric)

	return &ConsistencyDelayMetaFilter{
		logger:           logger,
		consistencyDelay: consistencyDelay,
	}
}

// Filter filters out blocks that filters blocks that have are created before a specified consistency delay.
func (f *ConsistencyDelayMetaFilter) Filter(metas map[ulid.ULID]*metadata.Meta, synced GaugeLabeled, _ bool) {
	for id, meta := range metas {
		// TODO(khyatisoneji): Remove the checks about Thanos Source
		//  by implementing delete delay to fetch metas.
		// TODO(bwplotka): Check consistency delay based on file upload / modification time instead of ULID.
		if ulid.Now()-id.Time() < uint64(f.consistencyDelay/time.Millisecond) &&
			meta.Thanos.Source != metadata.BucketRepairSource &&
			meta.Thanos.Source != metadata.CompactorSource &&
			meta.Thanos.Source != metadata.CompactorRepairSource {

			level.Debug(f.logger).Log("msg", "block is too fresh for now", "block", id)
			synced.WithLabelValues(tooFreshMeta).Inc()
			delete(metas, id)
		}
	}
}
