package compact

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/compact/downsample"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/labels"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
)

// Syncer syncronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger    log.Logger
	reg       prometheus.Registerer
	bkt       objstore.Bucket
	syncDelay time.Duration
	mtx       sync.Mutex
	blocks    map[ulid.ULID]*block.Meta
	metrics   *syncerMetrics
}

type syncerMetrics struct {
	syncMetas                 prometheus.Counter
	syncMetaFailures          prometheus.Counter
	syncMetaDuration          prometheus.Histogram
	garbageCollectedBlocks    prometheus.Counter
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration prometheus.Histogram
	compactions               prometheus.Counter
	compactionFailures        prometheus.Counter
}

func newSyncerMetrics(reg prometheus.Registerer) *syncerMetrics {
	var m syncerMetrics

	m.syncMetas = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_sync_meta_total",
		Help: "Total number of sync meta operations.",
	})
	m.syncMetaFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_sync_meta_failures_total",
		Help: "Total number of failed sync meta operations.",
	})
	m.syncMetaDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_compact_sync_meta_duration_seconds",
		Help: "Time it took to sync meta files.",
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	})

	m.garbageCollectedBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collected_blocks_total",
		Help: "Total number of deleted blocks by compactor.",
	})

	m.garbageCollections = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "thanos_compact_garbage_collection_duration_seconds",
		Help: "Time it took to perform garbage collection iteration.",
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	})

	m.compactions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_total",
		Help: "Total number of group compactions attempts.",
	})
	m.compactionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "thanos_compact_group_compactions_failures_total",
		Help: "Total number of failed group compactions.",
	})

	if reg != nil {
		reg.MustRegister(
			m.syncMetas,
			m.syncMetaFailures,
			m.syncMetaDuration,
			m.garbageCollectedBlocks,
			m.garbageCollections,
			m.garbageCollectionFailures,
			m.garbageCollectionDuration,
			m.compactions,
			m.compactionFailures,
		)
	}
	return &m
}

// NewSyncer returns a new Syncer for the given Bucket and directory.
// Blocks must be at least as old as the sync delay for being considered.
func NewSyncer(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, syncDelay time.Duration) (*Syncer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Syncer{
		logger:    logger,
		reg:       reg,
		syncDelay: syncDelay,
		blocks:    map[ulid.ULID]*block.Meta{},
		bkt:       bkt,
		metrics:   newSyncerMetrics(reg),
	}, nil
}

// SyncMetas synchronizes all meta files from blocks in the bucket into
// the memory.
func (c *Syncer) SyncMetas(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	begin := time.Now()

	err := c.syncMetas(ctx)
	if err != nil {
		c.metrics.syncMetaFailures.Inc()
	}
	c.metrics.syncMetas.Inc()
	c.metrics.syncMetaDuration.Observe(time.Since(begin).Seconds())

	return err
}

func (c *Syncer) syncMetas(ctx context.Context) error {
	// Read back all block metas so we can detect deleted blocks.
	remote := map[ulid.ULID]struct{}{}

	err := c.bkt.Iter(ctx, "", func(name string) error {
		if !strings.HasSuffix(name, "/") {
			return nil
		}
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}
		remote[id] = struct{}{}

		// Check if we already have this block cached locally.
		if _, ok := c.blocks[id]; ok {
			return nil
		}

		// ULIDs contain a millisecond timestamp. We do not consider blocks
		// that have been created too recently to avoid races when a block
		// is only partially uploaded.
		if ulid.Now()-id.Time() < uint64(c.syncDelay/time.Millisecond) {
			return nil
		}
		level.Debug(c.logger).Log("msg", "download meta", "block", id)

		rc, err := c.bkt.Get(ctx, path.Join(id.String(), "meta.json"))
		if err != nil {
			level.Warn(c.logger).Log("msg", "downloading meta.json failed", "block", id, "err", err)
			return nil
		}
		defer rc.Close()

		var meta block.Meta
		if err := json.NewDecoder(rc).Decode(&meta); err != nil {
			level.Warn(c.logger).Log("msg", "decoding meta.json failed", "block", id, "err", err)
			return nil
		}
		remote[id] = struct{}{}
		c.blocks[id] = &meta

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "retrieve bucket block metas")
	}

	// Delete all local block dirs that no longer exist in the bucket.
	for id := range c.blocks {
		if _, ok := remote[id]; !ok {
			delete(c.blocks, id)
		}
	}
	return nil
}

// groupKey returns a unique identifier for the group the block belongs to. It considers
// the downsampling resolution and the block's labels.
func groupKey(meta *block.Meta) string {
	return fmt.Sprintf("%d@%s", meta.Thanos.Downsample.Resolution, labels.FromMap(meta.Thanos.Labels))
}

// Groups returns the compaction groups for all blocks currently known to the syncer.
// It creates all groups from the scratch on every call.
func (c *Syncer) Groups() (res []*Group, err error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	groups := map[string]*Group{}
	for _, m := range c.blocks {
		g, ok := groups[groupKey(m)]
		if !ok {
			g, err = newGroup(
				log.With(c.logger, "compactionGroup", groupKey(m)),
				c.bkt,
				labels.FromMap(m.Thanos.Labels),
				m.Thanos.Downsample.Resolution,
				c.metrics.compactions,
				c.metrics.compactionFailures,
			)
			if err != nil {
				return nil, errors.Wrap(err, "create compaction group")
			}
			groups[groupKey(m)] = g
			res = append(res, g)
		}
		g.Add(m)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key() < res[j].Key()
	})
	return res, nil
}

// GarbageCollect deletes blocks from the bucket if their data is available as part of a
// block with a higher compaction level.
func (c *Syncer) GarbageCollect(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	begin := time.Now()

	// Run a separate round of garbage collections for each valid resolution.
	for _, res := range []int64{
		downsample.ResLevel0, downsample.ResLevel1, downsample.ResLevel2,
	} {
		err := c.garbageCollect(ctx, res)
		if err != nil {
			c.metrics.garbageCollectionFailures.Inc()
		}
		c.metrics.garbageCollections.Inc()
		c.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())

		if err != nil {
			return errors.Wrapf(err, "garbage collect resolution %d", res)
		}
	}
	return nil
}

func (c *Syncer) garbageCollect(ctx context.Context, resolution int64) error {
	// Map each block to its highest priority parent. Initial blocks have themselves
	// in their source section, i.e. are their own parent.
	parents := map[ulid.ULID]ulid.ULID{}

	for id, meta := range c.blocks {
		// Skip any block that has a different resolution.
		if meta.Thanos.Downsample.Resolution != resolution {
			continue
		}
		// For each source block we contain, check whether we are the highest priority parent block.
		for _, sid := range meta.Compaction.Sources {
			pid, ok := parents[sid]
			// No parents for the source block so far.
			if !ok {
				parents[sid] = id
				continue
			}
			pmeta, ok := c.blocks[pid]
			if !ok {
				return errors.Errorf("previous parent block %s not found", pid)
			}
			// The current block is the higher priority parent for the source if its
			// compaction level is higher than that of the previously set parent.
			// If compaction levels are equal, the more recent ULID wins.
			//
			// The ULID recency alone is not sufficient since races, e.g. induced
			// by downtime of garbage collection, may re-compact blocks that are
			// were already compacted into higher-level blocks multiple times.
			level, plevel := meta.Compaction.Level, pmeta.Compaction.Level

			if level > plevel || (level == plevel && id.Compare(pid) > 0) {
				parents[sid] = id
			}
		}
	}

	// A block can safely be deleted if they are not the highest priority parent for
	// any source block.
	topParents := map[ulid.ULID]struct{}{}
	for _, pid := range parents {
		topParents[pid] = struct{}{}
	}

	for id, meta := range c.blocks {
		// Skip any block that has a different resolution.
		if meta.Thanos.Downsample.Resolution != resolution {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, ok := topParents[id]; ok {
			continue
		}
		// Spawn a new context so we always delete a block in full on shutdown.
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

		level.Info(c.logger).Log("msg", "deleting outdated block", "block", id)

		err := objstore.DeleteDir(delCtx, c.bkt, id.String())
		cancel()
		if err != nil {
			return errors.Wrapf(err, "delete block %s from bucket", id)
		}
		// Immediately update our in-memory state so no further call to SyncMetas is needed
		/// after running garbage collection.
		delete(c.blocks, id)
		c.metrics.garbageCollectedBlocks.Inc()
	}
	return nil
}

// Group captures a set of blocks that have the same origin labels and downsampling resolution.
// Those blocks generally contain the same series and can thus efficiently be compacted.
type Group struct {
	logger             log.Logger
	bkt                objstore.Bucket
	labels             labels.Labels
	resolution         int64
	mtx                sync.Mutex
	blocks             map[ulid.ULID]*block.Meta
	compactions        prometheus.Counter
	compactionFailures prometheus.Counter
}

// newGroup returns a new compaction group.
func newGroup(
	logger log.Logger,
	bkt objstore.Bucket,
	lset labels.Labels,
	resolution int64,
	compactions prometheus.Counter,
	compactionFailures prometheus.Counter,
) (*Group, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	g := &Group{
		logger:             logger,
		bkt:                bkt,
		labels:             lset,
		resolution:         resolution,
		blocks:             map[ulid.ULID]*block.Meta{},
		compactions:        compactions,
		compactionFailures: compactionFailures,
	}
	return g, nil
}

// Key returns an identifier for the group.
func (cg *Group) Key() string {
	return fmt.Sprintf("%d@%s", cg.resolution, cg.labels)
}

// Add the block with the given meta to the group.
func (cg *Group) Add(meta *block.Meta) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	if !cg.labels.Equals(labels.FromMap(meta.Thanos.Labels)) {
		return errors.New("block and group labels do not match")
	}
	if cg.resolution != meta.Thanos.Downsample.Resolution {
		return errors.New("block and group resolution do not match")
	}
	cg.blocks[meta.ULID] = meta
	return nil
}

// IDs returns all sorted IDs of blocks in the group.
func (cg *Group) IDs() (ids []ulid.ULID) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	for id := range cg.blocks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})
	return ids
}

// Labels returns the labels that all blocks in the group share.
func (cg *Group) Labels() labels.Labels {
	return cg.labels
}

// Resolution returns the common downsampling resolution of blocks in the group.
func (cg *Group) Resolution() int64 {
	return cg.resolution
}

// Compact plans and runs a single compaction against the group. The compacted result
// is uploaded into the bucket the blocks were retrieved from.
func (cg *Group) Compact(ctx context.Context, dir string, comp tsdb.Compactor) (ulid.ULID, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return ulid.ULID{}, errors.Wrap(err, "create compaction dir")
	}
	id, err := cg.compact(ctx, dir, comp)
	if err != nil {
		cg.compactionFailures.Inc()
	}
	cg.compactions.Inc()

	return id, err
}

// HaltError is a type wrapper for errors that should halt any further progress on compactions.
type HaltError struct {
	err error
}

func halt(err error) HaltError {
	return HaltError{err: err}
}

func (e HaltError) Error() string {
	return e.err.Error()
}

// IsHaltError returns true if the base error is a HaltError.
func IsHaltError(err error) bool {
	_, ok1 := errors.Cause(err).(HaltError)
	_, ok2 := errors.Cause(err).(*HaltError)
	return ok1 || ok2
}

func (cg *Group) compact(ctx context.Context, dir string, comp tsdb.Compactor) (id ulid.ULID, err error) {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	// Planning a compaction works purely based on the meta.json files in our future group's dir.
	// So we first dump all our memory block metas into the directory.
	for _, meta := range cg.blocks {
		bdir := filepath.Join(dir, meta.ULID.String())
		if err := os.MkdirAll(bdir, 0777); err != nil {
			return id, errors.Wrap(err, "create planning block dir")
		}
		if err := block.WriteMetaFile(bdir, meta); err != nil {
			return id, errors.Wrap(err, "write planning meta file")
		}
	}

	// Plan against the written meta.json files.
	plan, err := comp.Plan(dir)
	if err != nil {
		return id, errors.Wrap(err, "plan compaction")
	}
	if len(plan) == 0 {
		// Nothing to do.
		return id, nil
	}

	// Due to #183 we verify that none of the blocks in the plan have overlapping sources.
	// This is one potential source of how we could end up with duplicated chunks.
	uniqueSources := map[ulid.ULID]struct{}{}

	for _, pdir := range plan {
		meta, err := block.ReadMetaFile(pdir)
		if err != nil {
			return id, errors.Wrapf(err, "read meta from %s", pdir)
		}
		for _, s := range meta.Compaction.Sources {
			if _, ok := uniqueSources[s]; ok {
				return id, halt(errors.Errorf("overlapping sources detected for plan %v", plan))
			}
			uniqueSources[s] = struct{}{}
		}
	}

	// Once we have a plan we need to download the actual data.
	begin := time.Now()

	for _, b := range plan {
		idStr := filepath.Base(b)

		if err := objstore.DownloadDir(ctx, cg.bkt, idStr, b); err != nil {
			return id, errors.Wrapf(err, "download block %s", idStr)
		}

		// Ensure all input blocks are valid.
		if err := block.VerifyIndex(filepath.Join(b, "index")); err != nil {
			return id, errors.Wrapf(halt(err), "invalid plan block %s", b)
		}
	}
	level.Debug(cg.logger).Log("msg", "downloaded blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	begin = time.Now()

	id, err = comp.Compact(dir, plan...)
	if err != nil {
		return id, errors.Wrapf(err, "compact blocks %v", plan)
	}
	level.Debug(cg.logger).Log("msg", "compacted blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	bdir := filepath.Join(dir, id.String())

	os.Remove(filepath.Join(bdir, "tombstones"))

	newMeta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read new meta")
	}
	newMeta.Thanos.Labels = cg.labels.Map()

	if err := block.WriteMetaFile(bdir, newMeta); err != nil {
		return id, errors.Wrap(err, "write new meta")
	}

	// Ensure the output block is valid.
	if err := block.VerifyIndex(filepath.Join(bdir, "index")); err != nil {
		return id, errors.Wrapf(halt(err), "invalid result block %s", bdir)
	}

	begin = time.Now()

	if err := objstore.UploadDir(ctx, cg.bkt, bdir, id.String()); err != nil {
		return id, errors.Wrap(err, "upload block")
	}
	level.Debug(cg.logger).Log("msg", "uploaded block", "block", id, "duration", time.Since(begin))

	// Delete the blocks we just compacted from the group so they do not get included
	// into the next planning cycle.
	// Eventually the block we just uploaded should get synced into the group again.
	for _, p := range plan {
		if err := os.RemoveAll(p); err != nil {
			level.Error(cg.logger).Log("msg", "remove compacted block dir", "err", err)
		}
	}
	return id, nil
}

// iterBlocks calls f for each meta.json of block directories in dir.
func iterBlocks(dir string, f func(dir string, id ulid.ULID) error) error {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, n := range names {
		id, err := ulid.Parse(n)
		if err != nil {
			continue
		}
		if err := f(filepath.Join(dir, n), id); err != nil {
			return err
		}
	}
	return nil
}

func renameFile(from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fsync(pdir); err != nil {
		pdir.Close()
		return err
	}
	return pdir.Close()
}
