package compact

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/improbable-eng/thanos/pkg/block"
	"github.com/improbable-eng/thanos/pkg/objstore"
	"github.com/prometheus/tsdb/fileutil"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

// Syncer syncronizes block metas from a bucket into a local directory.
// It sorts them into compaction groups based on equal label sets.
type Syncer struct {
	logger    log.Logger
	reg       prometheus.Registerer
	dir       string
	bkt       objstore.Bucket
	syncDelay time.Duration
	mtx       sync.Mutex
	groups    map[string]*Group
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
	if reg != nil {
		reg.MustRegister(
			m.syncMetas,
			m.syncMetaFailures,
			m.syncMetaDuration,
			m.garbageCollectedBlocks,
			m.garbageCollections,
			m.garbageCollectionFailures,
			m.garbageCollectionDuration,
		)
	}
	return &m
}

// NewSyncer returns a new Syncer for the given Bucket and directory.
// Blocks must be at least as old as the sync delay for being considered.
func NewSyncer(logger log.Logger, reg prometheus.Registerer, dir string, bkt objstore.Bucket, syncDelay time.Duration) (*Syncer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}

	c := &Syncer{
		logger:    logger,
		reg:       reg,
		dir:       dir,
		syncDelay: syncDelay,
		groups:    map[string]*Group{},
		bkt:       bkt,
		metrics:   newSyncerMetrics(reg),
	}
	return c, errors.Wrap(c.reloadGroups(), "reload groups")
}

func (c *Syncer) reloadGroups() error {
	groupDir := filepath.Join(c.dir, "groups")

	if err := os.RemoveAll(groupDir); err != nil {
		return errors.Wrap(err, "remove old groups")
	}
	if err := os.MkdirAll(groupDir, 0777); err != nil {
		return errors.Wrap(err, "create group dir")
	}

	return iterBlocks(c.dir, func(dir string, _ ulid.ULID) error {
		if err := c.add(dir); err != nil {
			return errors.Wrap(err, "add block to groups")
		}
		return nil
	})
}

// SyncMetas synchronizes all meta files from blocks in the bucket into
// the given directory.
func (c *Syncer) SyncMetas(ctx context.Context) error {
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
	var (
		local  = map[ulid.ULID]*block.Meta{}
		remote = map[ulid.ULID]struct{}{}
	)
	err := iterBlocks(c.dir, func(dir string, id ulid.ULID) error {
		meta, err := block.ReadMetaFile(dir)
		if err != nil {
			return err
		}
		local[id] = meta
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "read local blocks")
	}

	err = c.bkt.Iter(ctx, "", func(name string) error {
		if !strings.HasSuffix(name, "/") {
			return nil
		}
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}
		remote[id] = struct{}{}

		dir := filepath.Join(c.dir, name)

		// Check if we already have this block cached locally.
		if _, err = os.Stat(dir); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		// ULIDs contain a millisecond timestamp. We do not consider blocks
		// that have been created too recently to avoid races when a block
		// is only partially uploaded.
		if ulid.Now()-id.Time() < uint64(c.syncDelay/time.Millisecond) {
			return nil
		}
		level.Debug(c.logger).Log("msg", "download meta", "block", id)

		tmpdir := dir + ".tmp"

		if err := os.MkdirAll(tmpdir, 0777); err != nil {
			return err
		}
		defer os.RemoveAll(tmpdir)

		src := path.Join(name, "meta.json")

		if err := objstore.DownloadFile(ctx, c.bkt, src, tmpdir); err != nil {
			level.Warn(c.logger).Log("msg", "downloading meta.json failed", "block", id, "err", err)
			return nil
		}
		if err := renameFile(tmpdir, dir); err != nil {
			level.Warn(c.logger).Log("msg", "rename temp dir", "block", id, "err", err)
			return nil
		}
		// The first time we see a new block, we sort it into a group. The compactor
		// may delete a block from a group after having compacted it.
		// This prevents re-compacting the same data. The block remain in the top-level
		// directory until it has actually been deleted from the bucket.
		if err := c.add(dir); err != nil {
			level.Warn(c.logger).Log("msg", "add new block to group", "err", err)
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "retrieve bucket block metas")
	}

	// Delete all local block dirs that no longer exist in the bucket.
	for id, meta := range local {
		if _, ok := remote[id]; ok {
			continue
		}

		if err := os.RemoveAll(filepath.Join(c.dir, id.String())); err != nil {
			level.Warn(c.logger).Log("msg", "delete outdated block", "block", id, "err", err)
		}
		if err := os.RemoveAll(filepath.Join(c.dir, "groups", c.groupKey(meta), id.String())); err != nil {
			level.Warn(c.logger).Log("msg", "delete outdated block from group", "block", id, "err", err)
		}
	}

	return nil
}

func (c *Syncer) groupKey(meta *block.Meta) string {
	return fmt.Sprintf("%x", labels.FromMap(meta.Thanos.Labels).Hash())
}

// add adds the block in the given directory to its respective compaction group.
func (c *Syncer) add(bdir string) error {
	meta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return errors.Wrap(err, "read meta file")
	}
	h := c.groupKey(meta)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	g, ok := c.groups[h]
	if !ok {
		g, err = newGroup(
			log.With(c.logger, "compactionGroup", h),
			c.reg,
			c.bkt,
			filepath.Join(c.dir, "groups", h),
			meta.Thanos.Labels,
		)
		if err != nil {
			return errors.Wrap(err, "create compaction group")
		}
		c.groups[h] = g
	}
	return g.Add(meta)
}

// Groups returns the compaction groups created by the Syncer.
func (c *Syncer) Groups() (res []*Group) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, g := range c.groups {
		res = append(res, g)
	}
	return res
}

// GarbageCollect deletes blocks from the bucket if their data is available as part of a
// block with a higher compaction level.
func (c *Syncer) GarbageCollect(ctx context.Context) error {
	begin := time.Now()

	err := c.garbageCollect(ctx)
	if err != nil {
		c.metrics.garbageCollectionFailures.Inc()
	}
	c.metrics.garbageCollections.Inc()
	c.metrics.garbageCollectionDuration.Observe(time.Since(begin).Seconds())

	return err
}

func (c *Syncer) garbageCollect(ctx context.Context) error {
	// Map each block to its highest priority parent. Initial blocks have themselves
	// in their source section, i.e. are their own parent.
	var (
		all     = map[ulid.ULID]*block.Meta{}
		parents = map[ulid.ULID]ulid.ULID{}
	)
	err := iterBlocks(c.dir, func(dir string, _ ulid.ULID) error {
		meta, err := block.ReadMetaFile(dir)
		if err != nil {
			return errors.Wrap(err, "read meta")
		}
		all[meta.ULID] = meta
		return nil
	})
	if err != nil {
		return err
	}

	for id, meta := range all {
		// For each source block we contain, check whether we are the highest priority parent block.
		for _, sid := range meta.Compaction.Sources {
			pid, ok := parents[sid]
			// No parents for the source block so far.
			if !ok {
				parents[sid] = id
				continue
			}
			pmeta, ok := all[pid]
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

	for id := range all {
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
		c.metrics.garbageCollectedBlocks.Inc()
	}
	return nil
}

type groupMetrics struct {
	cachedBlocksCount float64

	compactions        prometheus.Counter
	compactionFailures prometheus.Counter
	compactionDuration prometheus.Histogram
}

func mapToString(m map[string]string) string {
	var s string

	var i int
	for k, v := range m {
		if i > 0 {
			s += ";"
		}
		i++
		s += fmt.Sprintf("%s=%s", k, v)
	}
	return s
}

func newGroupMetrics(reg prometheus.Registerer, labels map[string]string, getBlocksFunc func() (ids []ulid.ULID, err error)) *groupMetrics {
	var m groupMetrics

	// We cannot just append labels as metric labels, because it will conflict with the things generated by given Thanos
	// source represent by this group.
	groupLabels := map[string]string{
		"group_labels": mapToString(labels),
	}

	groupBlocksCount := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "thanos_compact_group_blocks_count",
		Help:        "Number of currently loaded blocks for group.",
		ConstLabels: groupLabels,
	}, func() float64 {
		b, err := getBlocksFunc()
		if err != nil {
			return float64(m.cachedBlocksCount)
		}
		m.cachedBlocksCount = float64(len(b))
		return float64(len(b))
	})

	m.compactions = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "thanos_compact_group_compactions_total",
		Help:        "Total number of group compactions attempts.",
		ConstLabels: groupLabels,
	})
	m.compactionFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "thanos_compact_group_compactions_failures_total",
		Help:        "Total number of failed group compactions.",
		ConstLabels: groupLabels,
	})
	m.compactionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "thanos_compact_group_compactions_duration_seconds",
		Help:        "Time it took to compact whole group.",
		ConstLabels: groupLabels,
		Buckets: []float64{
			0.25, 0.6, 1, 2, 3.5, 5, 7.5, 10, 15, 30, 60, 100, 200, 500,
		},
	})

	if reg != nil {
		reg.MustRegister(
			groupBlocksCount,
			m.compactions,
			m.compactionFailures,
			m.compactionDuration,
		)
	}
	return &m
}

// Group captures a set of blocks that have the same origin labels.
// Those blocks generally contain the same series and can thus efficiently be compacted.
type Group struct {
	logger  log.Logger
	mtx     sync.Mutex
	dir     string
	bkt     objstore.Bucket
	labels  map[string]string
	metrics *groupMetrics
}

// newGroup returns a new compaction group.
func newGroup(logger log.Logger, reg prometheus.Registerer, bkt objstore.Bucket, dir string, labels map[string]string) (*Group, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	g := &Group{
		logger: logger,
		dir:    dir,
		bkt:    bkt,
		labels: labels,
	}
	g.metrics = newGroupMetrics(reg, labels, g.IDs)
	return g, nil
}

// Add the block with the given meta to the group.
func (cg *Group) Add(meta *block.Meta) error {
	cg.mtx.Lock()
	defer cg.mtx.Unlock()

	if !reflect.DeepEqual(cg.labels, meta.Thanos.Labels) {
		return errors.New("block and group labels do not match")
	}

	bdir := filepath.Join(cg.dir, meta.ULID.String())

	if err := os.MkdirAll(bdir, 0777); err != nil {
		return errors.Wrap(err, "create block dir")
	}
	return block.WriteMetaFile(bdir, meta)
}

// Dir returns the groups directory.
func (cg *Group) Dir() string {
	return cg.dir
}

// IDs returns all sorted IDs of blocks in the group.
func (cg *Group) IDs() (ids []ulid.ULID, err error) {
	err = iterBlocks(cg.dir, func(_ string, id ulid.ULID) error {
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

// Labels returns the labels that all blocks in the group share.
func (cg *Group) Labels() map[string]string {
	return cg.labels
}

// Compact plans and runs a single compaction against the group. The compacted result
// is uploaded into the bucket the blocks were retrieved from.
func (cg *Group) Compact(ctx context.Context, comp tsdb.Compactor) (ulid.ULID, error) {
	begin := time.Now()

	id, err := cg.compact(ctx, comp)
	if err != nil {
		cg.metrics.compactionFailures.Inc()
	}
	cg.metrics.compactions.Inc()
	cg.metrics.compactionDuration.Observe(time.Since(begin).Seconds())

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

func (cg *Group) compact(ctx context.Context, comp tsdb.Compactor) (id ulid.ULID, err error) {
	// Planning a compaction works purely based on the meta.json files in our group's dir.
	cg.mtx.Lock()
	plan, err := comp.Plan(cg.dir)
	cg.mtx.Unlock()

	if err != nil {
		return id, errors.Wrap(err, "plan compaction")
	}
	if len(plan) == 0 {
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
		// Ensure all input blocks are valid.
		if err := block.VerifyIndex(filepath.Join(pdir, "index")); err != nil {
			return id, errors.Wrapf(halt(err), "invalid plan block %s", pdir)
		}
	}

	// Once we have a plan we need to download the actual data. We don't touch
	// the main directory but use an intermediate one instead.
	wdir := filepath.Join(cg.dir, "tmp")

	if err := os.MkdirAll(wdir, 0777); err != nil {
		return id, errors.Wrap(err, "create working dir")
	}
	defer os.RemoveAll(wdir)

	var compDirs []string
	begin := time.Now()

	for _, b := range plan {
		ids := filepath.Base(b)
		dst := filepath.Join(wdir, ids)

		if err := objstore.DownloadDir(ctx, cg.bkt, ids, dst); err != nil {
			return id, errors.Wrapf(err, "download block %s", ids)
		}
		compDirs = append(compDirs, dst)
	}
	level.Debug(cg.logger).Log("msg", "downloaded blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	begin = time.Now()

	id, err = comp.Compact(wdir, compDirs...)
	if err != nil {
		return id, errors.Wrapf(err, "compact blocks %v", plan)
	}
	level.Debug(cg.logger).Log("msg", "compacted blocks",
		"blocks", fmt.Sprintf("%v", plan), "duration", time.Since(begin))

	bdir := filepath.Join(wdir, id.String())

	os.Remove(filepath.Join(bdir, "tombstones"))

	newMeta, err := block.ReadMetaFile(bdir)
	if err != nil {
		return id, errors.Wrap(err, "read new meta")
	}
	newMeta.Thanos.Labels = cg.labels

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
